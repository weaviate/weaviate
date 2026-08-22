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

package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestPutPublishesTheRecordByReplacingTheFile pins how Put writes, not just
// what it writes. Every durability argument on this branch rests on a record
// being published whole — the promotion rename, the flip decision and the
// horizon are all vouched for by a record that is either the old one or the
// new one and never half of either. An in-place write satisfies every other
// test in the package.
//
// A hard link is the witness: it holds the inode the old record occupied, so
// it reports what a replacing publisher left behind and what an in-place one
// overwrote. The second row is the control — without a publisher that fails
// these assertions, they could pass for the wrong reason.
func TestPutPublishesTheRecordByReplacingTheFile(t *testing.T) {
	tests := []struct {
		name string
		// publish writes the second version of the record.
		publish func(t *testing.T, store *MigrationRecordStore, target string, rec MigrationRecord)
		// wantReplaced: the publish left a different file behind, so a reader
		// holding the old one still reads the old record whole.
		wantReplaced bool
	}{
		{
			name: "the store's own publish replaces the file",
			publish: func(t *testing.T, store *MigrationRecordStore, _ string, rec MigrationRecord) {
				require.NoError(t, store.Put(rec))
			},
			wantReplaced: true,
		},
		{
			name: "a plain in-place write does not",
			publish: func(t *testing.T, _ *MigrationRecordStore, target string, rec MigrationRecord) {
				data, err := encodeMigrationRecord(rec)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(target, data, 0o600))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsm := t.TempDir()
			logger, _ := test.NewNullLogger()
			store := NewMigrationRecordStore(lsm, logger)

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			require.NoError(t, store.Put(NewMigrationRecordMerged(subject)))

			target := filepath.Join(store.Dir(), subject.Key.fileName())
			before, err := os.ReadFile(target)
			require.NoError(t, err)
			beforeInfo, err := os.Stat(target)
			require.NoError(t, err)

			// Outside the records directory: a stray file inside it is what
			// the store's own temp-file sweep exists to remove.
			witness := filepath.Join(lsm, "witness")
			require.NoError(t, os.Link(target, witness))

			tc.publish(t, store, target, NewMigrationRecordSwapped(subject,
				[]string{"title"}, map[string]string{"title": "property_title"}))

			afterTarget, err := os.ReadFile(target)
			require.NoError(t, err)
			require.NotEqual(t, before, afterTarget, "the fixture must actually publish something new")

			witnessed, err := os.ReadFile(witness)
			require.NoError(t, err)
			afterInfo, err := os.Stat(target)
			require.NoError(t, err)

			if tc.wantReplaced {
				require.Equal(t, before, witnessed,
					"the file the old record occupied must still hold it whole")
				require.False(t, os.SameFile(beforeInfo, afterInfo),
					"publishing must leave a different file at the name")
				return
			}
			require.Equal(t, afterTarget, witnessed,
				"an in-place write is visible through the old file")
			require.True(t, os.SameFile(beforeInfo, afterInfo),
				"an in-place write keeps the same file at the name")
		})
	}
}

// TestPutLeavesTheOldRecordIntactWhenItCannotPublish is the other half: a
// publish that cannot finish must leave the previous record readable rather
// than a truncated one. A records directory it may not write into is the
// portable way to stop it partway, and it stops it before the target is
// touched at all — which is the property.
func TestPutLeavesTheOldRecordIntactWhenItCannotPublish(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes into a directory whatever its mode says")
	}
	lsm := t.TempDir()
	logger, _ := test.NewNullLogger()
	store := NewMigrationRecordStore(lsm, logger)

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	require.NoError(t, store.Put(NewMigrationRecordMerged(subject)))
	target := filepath.Join(store.Dir(), subject.Key.fileName())
	before, err := os.ReadFile(target)
	require.NoError(t, err)

	require.NoError(t, os.Chmod(store.Dir(), 0o500))
	defer func() { require.NoError(t, os.Chmod(store.Dir(), 0o755)) }()

	require.Error(t, store.Put(NewMigrationRecordSwapped(subject,
		[]string{"title"}, map[string]string{"title": "property_title"})),
		"a publish that cannot create its temp file must report it")

	after, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, before, after, "the previous record must survive a publish that failed")

	rec, outcome, err := loadMigrationRecordFile(target)
	require.NoError(t, err)
	require.Equal(t, MigrationRecordLoaded, outcome)
	require.Equal(t, MigrationStateMerged, rec.State())
}
