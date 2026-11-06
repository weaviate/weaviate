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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// Pins that the recovery hook arms nothing while runtime reindex is disabled,
// though the shard holds a recoverable migration.
func TestReindexRecoveryHook_RuntimeReindexDisabled(t *testing.T) {
	logger, _ := test.NewNullLogger()
	lsm := filepath.Join(t.TempDir(), "lsm")
	require.NoError(t, os.MkdirAll(lsm, 0o777))
	store := NewMigrationRecordStore(lsm, logger)
	require.NoError(t, store.Load())
	require.NoError(t, store.Put(NewMigrationRecordIterated(testMigrationSubject(3, StrategyCodeSearchableRetokenize, "title"))))

	disabled := NewReindexRecoveryHook(false, logger, nil)
	require.NotEmpty(t, disabled.recoverableTasks(store, "shard-1"), "the fixture is recoverable")
	require.NoError(t, disabled.RunAfterLsmInit(context.Background(), &Shard{name: "shard-1", migrationRecords: store}))
	require.Empty(t, disabled.pending)
}
