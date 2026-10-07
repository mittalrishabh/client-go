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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNoisyTenantWindowLifecycle(t *testing.T) {
	var n noisyTenants
	t0 := time.Now()

	require.False(t, n.isNoisy("uds_006", 1, t0))

	n.mark("uds_006", 1, t0)
	require.True(t, n.isNoisy("uds_006", 1, t0))
	require.True(t, n.isNoisy("uds_006", 1, t0.Add(noisyTenantTTL-time.Nanosecond)))
	require.False(t, n.isNoisy("uds_006", 1, t0.Add(noisyTenantTTL)))
}

func TestNoisyTenantWindowIsScopedPerGroupAndStore(t *testing.T) {
	var n noisyTenants
	t0 := time.Now()
	n.mark("uds_006", 1, t0)

	// A neighbour on the same store is not implicated, and the same group on a
	// store that has not blamed it keeps its follower reads.
	require.False(t, n.isNoisy("uds_007", 1, t0))
	require.False(t, n.isNoisy("uds_006", 2, t0))
}

func TestNoisyTenantEmptyGroupOrStoreIsIgnored(t *testing.T) {
	var n noisyTenants
	t0 := time.Now()

	// Requests without resource control set must not all collapse onto one
	// shared window.
	n.mark("", 1, t0)
	n.mark("uds_006", 0, t0)
	require.False(t, n.isNoisy("", 1, t0))
	require.False(t, n.isNoisy("uds_006", 0, t0))
}

func TestNoisyTenantRenewExtendsButNeverOpens(t *testing.T) {
	var n noisyTenants
	t0 := time.Now()

	// An ambiguous signal for a group nobody has blamed must not mark it:
	// every tenant on an overloaded store sees these.
	require.False(t, n.renew("uds_006", 1, t0))
	require.False(t, n.isNoisy("uds_006", 1, t0))

	n.mark("uds_006", 1, t0)
	half := t0.Add(noisyTenantTTL / 2)
	require.True(t, n.renew("uds_006", 1, half))
	// Renewal restarts the window from the signal, not from the original mark.
	require.True(t, n.isNoisy("uds_006", 1, t0.Add(noisyTenantTTL)))
	require.False(t, n.isNoisy("uds_006", 1, half.Add(noisyTenantTTL)))
}

func TestNoisyTenantRenewDoesNotRevive(t *testing.T) {
	var n noisyTenants
	t0 := time.Now()
	n.mark("uds_006", 1, t0)

	lapsed := t0.Add(noisyTenantTTL)
	require.False(t, n.renew("uds_006", 1, lapsed))
	require.False(t, n.isNoisy("uds_006", 1, lapsed))
}

func TestNoisyTenantMarkKeepsFurthestExpiry(t *testing.T) {
	var n noisyTenants
	t0 := time.Now()
	n.mark("uds_006", 1, t0.Add(time.Second))
	// A late-arriving mark from an earlier signal must not shorten the window.
	n.mark("uds_006", 1, t0)
	require.True(t, n.isNoisy("uds_006", 1, t0.Add(noisyTenantTTL)))
}

func TestNoisyTenantSweepDropsOnlyLapsed(t *testing.T) {
	var n noisyTenants
	t0 := time.Now()
	n.mark("uds_006", 1, t0)
	n.mark("uds_007", 2, t0.Add(noisyTenantTTL))

	n.sweep(context.Background(), t0.Add(noisyTenantTTL))

	_, live := n.m.Load(noisyTenantKey{group: "uds_006", storeID: 1})
	require.False(t, live)
	_, live = n.m.Load(noisyTenantKey{group: "uds_007", storeID: 2})
	require.True(t, live)
}
