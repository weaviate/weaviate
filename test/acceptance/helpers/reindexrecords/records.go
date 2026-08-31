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

// Package reindexrecords renders migration records for acceptance tests that
// plant on-disk state a crash would have left behind.
package reindexrecords

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db"
)

// Encode returns the file name the record store gives rec and the bytes it
// writes into it, through the production writer on purpose: a hand-built
// fixture would pin the format version it was written against, and a bump
// would silently stop pinning anything once the server can no longer read it.
func Encode(t *testing.T, rec db.MigrationRecord) (name, content string) {
	t.Helper()

	logger := logrus.New()
	logger.SetOutput(io.Discard)
	store := db.NewMigrationRecordStore(t.TempDir(), logger)
	require.NoError(t, store.Put(rec))

	entries, err := os.ReadDir(store.Dir())
	require.NoError(t, err)
	require.Len(t, entries, 1, "one record was written, so one file must be there")

	data, err := os.ReadFile(filepath.Join(store.Dir(), entries[0].Name()))
	require.NoError(t, err)
	return entries[0].Name(), string(data)
}
