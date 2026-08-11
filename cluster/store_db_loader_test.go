//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDBLoaderDeferredDeletesKeepFrozen pins that the one drop pass a deferred
// delete gets is told about frozen tenants if any incarnation of the class had
// them. A class can be deleted and re-added several times during a load; if a
// later hot delete overwrites an earlier frozen one, DropClass is never told to
// clean up cloud storage and the offloaded data is orphaned.
func TestDBLoaderDeferredDeletesKeepFrozen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		deletes []bool
		want    bool
	}{
		{name: "hot only", deletes: []bool{false, false}, want: false},
		{name: "frozen only", deletes: []bool{true, true}, want: true},
		{name: "frozen then hot", deletes: []bool{true, false}, want: true},
		{name: "hot then frozen", deletes: []bool{false, true}, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var l dbLoader
			require.True(t, l.begin())

			for _, hasFrozen := range test.deletes {
				require.True(t, l.deferWrite("C", hasFrozen),
					"a command landing mid-load must defer its DB write")
			}

			deletes, done := l.finish()
			require.False(t, done, "a deferred write owes the loader another pass")
			require.Equal(t, map[string]bool{"C": test.want}, deletes)
		})
	}
}

// TestDBLoaderIdleWritesGoStraightToTheDB pins that nothing is deferred, or
// recorded, once the load is done. The record only drains on a pass, so a class
// recorded after the last one sits there for good.
func TestDBLoaderIdleWritesGoStraightToTheDB(t *testing.T) {
	t.Parallel()

	var l dbLoader
	require.False(t, l.deferWrite("C", true), "no load in flight: nothing to defer")

	require.True(t, l.begin())
	_, done := l.finish()
	require.True(t, done)

	require.False(t, l.deferWrite("C", true), "the load is over: nothing to defer")
	require.Nil(t, l.deletes, "a write that was not deferred must not be recorded")
	require.False(t, l.begin(), "the load is one-shot")
}
