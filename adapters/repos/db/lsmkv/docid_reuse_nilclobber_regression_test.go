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

package lsmkv

import (
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestDocIDReuse_MemtableSecondaryNilClobber pins the memtable-level sibling of
// finding C1: updateSecondaryToPrimary (memtable.go) unconditionally does
//
//	m.secondaryToPrimary[i][prevSec] = nil   // then
//	m.secondaryToPrimary[i][newSec]  = key
//
// for a tombstone's previousKeys, without checking whether prevSec currently
// resolves to THIS primary key. Under docID reuse a UUID can transiently be
// carried by two primary rows at once (the deleted-but-not-yet-compacted old
// docID and the re-created object's new docID). Tombstoning the stale row then
// clobbers the live row's secondary->primary mapping and re-points it at the
// dead row, so GetBySecondary(UUID) resolves to a tombstoned object and the
// live re-creation becomes invisible.
//
// This is the sibling the 10-14/15 audit flagged as having no repro
// (combined-suite gap, see docid-reuse-jero-audit memory). NEW pinning test.
//
// Red at tests/docid-reuse-regression head (dec791e549): the final
// getBySecondary returns the tombstoned primary (err / wrong key) instead of
// the live re-created primary.
func TestDocIDReuse_MemtableSecondaryNilClobber(t *testing.T) {
	t.Skip("pins memtable secondary nil-clobber on UUID re-creation; no fix yet — see docid-reuse-jero-audit (combined-suite gap, memtable updateSecondaryToPrimary). Verified RED at dec791e549: getBySecondary(UUID) returns 'deleted'.")

	logger, _ := test.NewNullLogger()
	path := filepath.Join(t.TempDir(), "segment")

	cl, err := newCommitLogger(path, StrategyReplace, 0)
	require.NoError(t, err)

	m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
		path:             path,
		strategy:         StrategyReplace,
		secondaryIndices: 1,
	})
	require.NoError(t, err)

	uuid := []byte("11111111-1111-1111-1111-111111111111")
	oldDocID := mustDocIDKey(1)
	newDocID := mustDocIDKey(2)

	// old object: UUID -> docID 1
	require.NoError(t, m.put(oldDocID, []byte("old"), WithSecondaryKey(0, uuid)))

	// re-create the same UUID at a reused/new docID 2 while the old row is
	// still present (the transient two-rows-share-UUID state reachable under
	// docID reuse). UUID now resolves to the live re-creation.
	require.NoError(t, m.put(newDocID, []byte("new"), WithSecondaryKey(0, uuid)))

	primary, _, err := m.getBySecondary(0, uuid)
	require.NoError(t, err)
	require.Equal(t, newDocID, primary, "precondition: UUID resolves to the live re-created row")

	// delete the stale old row. It carries the same UUID, so
	// updateSecondaryToPrimary nils-then-reassigns the UUID mapping to the
	// tombstoned primary.
	require.NoError(t, m.setTombstone(oldDocID, WithSecondaryKey(0, uuid)))

	// the live re-creation must still be reachable by its UUID.
	primary, _, err = m.getBySecondary(0, uuid)
	require.NoError(t, err, "live re-created object became invisible by secondary key after the stale row was tombstoned")
	require.Equal(t, newDocID, primary,
		"UUID must resolve to the live re-created docID, not the tombstoned stale docID")
}

// mustDocIDKey renders a docID as the 8-byte big-endian primary key the objects
// bucket uses, so the test keys match production primary-key shape.
func mustDocIDKey(id uint64) []byte {
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[7-i] = byte(id >> (8 * i))
	}
	return b
}
