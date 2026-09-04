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

// No other test catches a deleted content sync: it only shows after a crash. The
// directory sync past the rename is pinned by diskio's TestRenameAndSync.
func TestWriteFileAtomicSyncsTheContentBeforeItPublishesTheName(t *testing.T) {
	dir := t.TempDir()
	const name = "record.mig"
	target := filepath.Join(dir, name)

	var synced []string
	sync := func(f *os.File) error {
		got, err := os.ReadFile(f.Name())
		require.NoError(t, err)
		require.Equal(t, "payload", string(got), "the bytes have to be written before the sync")
		require.NoFileExists(t, target, "and the name must not be published before it")
		synced = append(synced, f.Name())
		return f.Sync()
	}

	require.NoError(t, writeFileAtomicWithSync(dir, name, []byte("payload"), sync))

	require.Len(t, synced, 1, "the content is synced exactly once, before the rename")
	require.FileExists(t, target, "and the rename then publishes the name")
	got, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, "payload", string(got))
}
