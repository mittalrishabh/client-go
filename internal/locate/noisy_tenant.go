// Copyright 2026 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package locate

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// noisyTenantTTL is how long a resource group stays marked noisy on a store
// after TiKV last blamed it. The window has to outlast TiKV's own shedding
// decision, which is refreshed on a 30s tick in the resource_control crate, so
// that the mark does not lapse between two ticks of a group that is still over
// its baseline.
const noisyTenantTTL = 60 * time.Second

// noisyTenantSweepInterval is how often expired entries are dropped. Expiry is
// enforced on read, so this only bounds memory for groups that stop sending.
const noisyTenantSweepInterval = 30 * time.Second

// noisyTenantKey scopes a mark to one resource group on one store.
//
// The scope is per store rather than per cluster because TiKV sheds per node:
// a group over its RU baseline on one overloaded store is usually still within
// baseline everywhere else, and a cluster-wide mark would pin that group's
// reads to the leader on stores that are serving it fine.
type noisyTenantKey struct {
	group   string
	storeID uint64
}

// noisyTenants tracks which resource groups TiKV has recently blamed for its
// own overload, so that the decision survives the request that discovered it.
//
// Without this, each request has to learn the hard way: it fans out to a
// follower, the follower asks the same overloaded leader for a ReadIndex, and
// only the resulting ServerIsBusy teaches that one request to stay on the
// leader. Remembering the verdict for a window lets every later request from
// the same group skip that round trip.
type noisyTenants struct {
	// group+store -> *atomic.Int64 holding the expiry as unix nanos.
	// A sync.Map keyed this way keeps the read path, which runs once per
	// request, off a shared mutex; entries are never removed while live so a
	// loaded pointer stays valid for the caller's Store/Load.
	m sync.Map
}

// mark starts or extends a group's noisy window on a store. Called when TiKV
// explicitly attributes a ServerIsBusy to this group.
func (n *noisyTenants) mark(group string, storeID uint64, now time.Time) {
	if group == "" || storeID == 0 {
		return
	}
	expiry := now.Add(noisyTenantTTL).UnixNano()
	key := noisyTenantKey{group: group, storeID: storeID}
	if v, ok := n.m.Load(key); ok {
		storeMaxExpiry(v.(*atomic.Int64), expiry)
		return
	}
	slot := new(atomic.Int64)
	slot.Store(expiry)
	if v, loaded := n.m.LoadOrStore(key, slot); loaded {
		storeMaxExpiry(v.(*atomic.Int64), expiry)
	}
}

// renew extends an existing noisy window but never opens one.
//
// The signals that feed it -- deadline exceeded, an untagged ServerIsBusy, a
// request timeout -- are all ambiguous on their own: any tenant sharing an
// overloaded store sees them, so treating one as proof of noisiness would mark
// the victims along with the cause. They are only evidence that a group TiKV
// has *already* named is still in trouble, which is enough to hold the mark
// open but not to create it.
func (n *noisyTenants) renew(group string, storeID uint64, now time.Time) bool {
	if group == "" || storeID == 0 {
		return false
	}
	v, ok := n.m.Load(noisyTenantKey{group: group, storeID: storeID})
	if !ok {
		return false
	}
	slot := v.(*atomic.Int64)
	if slot.Load() <= now.UnixNano() {
		// Already lapsed. Let it stay lapsed: reviving it here would let a
		// group that has been quiet for a full window be re-pinned by one
		// ambiguous error.
		return false
	}
	storeMaxExpiry(slot, now.Add(noisyTenantTTL).UnixNano())
	return true
}

// isNoisy reports whether the group currently has a live mark on the store.
func (n *noisyTenants) isNoisy(group string, storeID uint64, now time.Time) bool {
	if group == "" || storeID == 0 {
		return false
	}
	v, ok := n.m.Load(noisyTenantKey{group: group, storeID: storeID})
	if !ok {
		return false
	}
	return v.(*atomic.Int64).Load() > now.UnixNano()
}

// sweep drops lapsed entries. Safe to run concurrently with mark and renew:
// an entry is only deleted after a compare-and-swap fixes its expiry in the
// past, so a mark that lands mid-sweep either wins the swap and survives, or
// is re-created by the next mark.
func (n *noisyTenants) sweep(_ context.Context, now time.Time) bool {
	cutoff := now.UnixNano()
	n.m.Range(func(key, value any) bool {
		slot := value.(*atomic.Int64)
		if expiry := slot.Load(); expiry <= cutoff && slot.CompareAndSwap(expiry, expiry) {
			n.m.Delete(key)
		}
		return true
	})
	return false
}

func storeMaxExpiry(slot *atomic.Int64, expiry int64) {
	for {
		cur := slot.Load()
		if cur >= expiry || slot.CompareAndSwap(cur, expiry) {
			return
		}
	}
}
