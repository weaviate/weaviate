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

// A reader must see the old record or the new one, never half of either.
func TestPutPublishesTheRecordByReplacingTheFile(t *testing.T) {
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

	// A second name for the file the old record occupies: an in-place write
	// shows through it, a replacement cannot.
	witness := filepath.Join(lsm, "witness")
	require.NoError(t, os.Link(target, witness))

	require.NoError(t, store.Put(NewMigrationRecordSwapped(subject,
		[]string{"title"}, map[string]string{"title": "property_title_searchable"})))

	after, err := os.ReadFile(target)
	require.NoError(t, err)
	require.NotEqual(t, before, after, "the fixture must actually publish something new")

	witnessed, err := os.ReadFile(witness)
	require.NoError(t, err)
	require.Equal(t, before, witnessed, "the file the old record occupied must still hold it whole")

	afterInfo, err := os.Stat(target)
	require.NoError(t, err)
	require.False(t, os.SameFile(beforeInfo, afterInfo),
		"publishing must leave a different file at the name")
}

func TestPutAllRefusesWithoutWritingAnything(t *testing.T) {
	logger, _ := test.NewNullLogger()
	first := NewMigrationRecordMerged(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title"))
	second := NewMigrationRecordMerged(testMigrationSubject(42, StrategyCodeFilterableRetokenize, "body"))
	keys := []MigrationRecordKey{first.Subject().Key, second.Subject().Key}

	t.Run("every record lands", func(t *testing.T) {
		store := NewMigrationRecordStore(t.TempDir(), logger)
		require.NoError(t, store.PutAll([]MigrationRecord{first, second}))
		reloaded := NewMigrationRecordStore(filepath.Dir(filepath.Dir(store.Dir())), logger)
		require.NoError(t, reloaded.Load())
		for _, s := range []*MigrationRecordStore{store, reloaded} {
			for _, key := range keys {
				_, ok := s.Get(key)
				require.True(t, ok, key.String())
			}
		}
	})

	tests := []struct {
		name    string
		setup   func(t *testing.T, store *MigrationRecordStore)
		recs    []MigrationRecord
		wantErr string
	}{
		{
			name: "a record here cannot be read",
			setup: func(t *testing.T, store *MigrationRecordStore) {
				require.NoError(t, os.MkdirAll(store.Dir(), 0o777))
				garbled := testMigrationSubject(7, StrategyCodeSearchableRetokenize, "summary").Key
				require.NoError(t, os.WriteFile(filepath.Join(store.Dir(), garbled.fileName()), []byte("{"), 0o600))
				require.NoError(t, store.Load())
			},
			recs:    []MigrationRecord{first, second},
			wantErr: "refusing to write",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMigrationRecordStore(t.TempDir(), logger)
			tc.setup(t, store)

			require.ErrorContains(t, store.PutAll(tc.recs), tc.wantErr)
			for _, key := range keys {
				require.NoFileExists(t, filepath.Join(store.Dir(), key.fileName()))
				_, ok := store.Get(key)
				require.False(t, ok, key.String())
			}
		})
	}
}

// Whether the syncs run shows only after a crash; these rows check what real faults show.
func TestRecordWritesKeepTheirOrderingOnRealFaults(t *testing.T) {
	logger, _ := test.NewNullLogger()
	rec := NewMigrationRecordMerged(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title"))

	t.Run("the content is whole before the name exists", func(t *testing.T) {
		dir := t.TempDir()
		tmp, err := writeSyncedTemp(dir, "record.mig", []byte("payload"))
		require.NoError(t, err)
		got, err := os.ReadFile(tmp)
		require.NoError(t, err)
		require.Equal(t, "payload", string(got))
		require.NoFileExists(t, filepath.Join(dir, "record.mig"))
	})

	t.Run("a failed rename keeps only the records published before it", func(t *testing.T) {
		store := NewMigrationRecordStore(t.TempDir(), logger)
		second := NewMigrationRecordMerged(testMigrationSubject(42, StrategyCodeFilterableRetokenize, "body"))
		require.NoError(t, os.MkdirAll(filepath.Join(store.Dir(), second.Subject().Key.fileName(), "blocker"), 0o777))

		require.Error(t, store.PutAll([]MigrationRecord{rec, second}))
		_, ok := store.Get(rec.Subject().Key)
		require.True(t, ok, "the first record was renamed into place, so memory adopts it")
		_, ok = store.Get(second.Subject().Key)
		require.False(t, ok)
	})
}
