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

package hfresh

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAuditHFreshVersionWrapRetainsStalePosting covers #12960: after the
// seven-bit generation counter wraps 127→1, GarbageCollect must treat 127 as
// the immediately preceding generation and drop the stale posting.
func TestAuditHFreshVersionWrapRetainsStalePosting(t *testing.T) {
	ctx := t.Context()
	versionMap := makeVersionMap(t)

	const vectorID = uint64(900)
	version := v1
	var err error
	for range 126 {
		version, err = versionMap.Increment(ctx, vectorID, version)
		require.NoError(t, err)
	}
	require.EqualValues(t, 127, version.Version())

	stale := NewVector(vectorID, version, []byte{0x01})

	version, err = versionMap.Increment(ctx, vectorID, version)
	require.NoError(t, err)
	require.EqualValues(t, 1, version.Version())

	retained, err := Posting{stale}.GarbageCollect(versionMap)
	require.NoError(t, err)

	ids := make([]uint64, len(retained))
	for i, v := range retained {
		ids[i] = v.ID()
	}
	require.Empty(t, ids, "expected generation-127 posting removed after wrap to 1; retained=%v", ids)
}

// TestAuditHFreshVersionNoWrapRemovesOlderPosting is the non-wrapped control:
// advancing 1→2 must still drop the older posting.
func TestAuditHFreshVersionNoWrapRemovesOlderPosting(t *testing.T) {
	ctx := t.Context()
	versionMap := makeVersionMap(t)

	const vectorID = uint64(901)
	old := v1
	stale := NewVector(vectorID, old, []byte{0x01})

	next, err := versionMap.Increment(ctx, vectorID, old)
	require.NoError(t, err)
	require.EqualValues(t, 2, next.Version())

	retained, err := Posting{stale}.GarbageCollect(versionMap)
	require.NoError(t, err)
	require.Empty(t, retained, "expected generation-1 posting removed after advance to 2")
}
