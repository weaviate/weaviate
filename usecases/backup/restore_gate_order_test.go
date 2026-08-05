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

package backup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

// Pins the order Scheduler.Restore documents: authorization, then the reindex
// gate, then existence. The authorization half is pinned in auth_test.go.
func TestRestoreGateAnswersBeforeExistence(t *testing.T) {
	ctx := context.Background()
	const (
		unknownID   = "no-such-backup"
		backendName = "s3"
		homePath    = "root/123"
	)

	newFixture := func(t *testing.T) *fakeScheduler {
		t.Helper()
		fs := newFakeScheduler(nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(homePath)
		fs.backend.On("GetObject", ctx, unknownID, GlobalBackupFile).
			Return(nil, backup.NewErrNotFound(errors.New("not found")))
		fs.backend.On("GetObject", ctx, unknownID, BackupFile).Return(nil, backup.ErrNotFound{})
		return fs
	}

	t.Run("live reindex outranks an unknown id", func(t *testing.T) {
		fs := newFixture(t)
		fs.selector.reindexInFlightErr = errors.New("runtime-reindex in flight")

		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      unknownID,
		}, false)

		require.Error(t, err)
		assert.IsTypef(t, backup.ErrUnprocessable{}, err,
			"a live reindex must answer 422 before the unknown id answers 404; got %v", err)
		assert.Contains(t, err.Error(), "restore blocked")
	})

	t.Run("without a live reindex the unknown id still answers 404", func(t *testing.T) {
		fs := newFixture(t)

		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      unknownID,
		}, false)

		require.Error(t, err)
		assert.IsType(t, backup.ErrNotFound{}, err,
			"the gate must not turn a genuinely unknown id into anything else")
	})
}

// A restore that names no classes takes its own path through Restore: the class
// list only exists once the meta is read, so the gate sits after the meta read
// instead of before it. It still has to refuse, and refuse before any of the
// backup's schema is pulled.
func TestRestoreWithoutExplicitIncludeIsGated(t *testing.T) {
	ctx := context.Background()
	const (
		backupID    = "1"
		backendName = "s3"
		homePath    = "bucket/backups/1"
		node        = "Node-A"
		class       = "Movies"
	)

	newFixture := func() *fakeScheduler {
		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		meta := backup.DistributedBackupDescriptor{
			ID:            backupID,
			StartedAt:     time.Now().UTC(),
			Version:       Version,
			ServerVersion: "1.23",
			Status:        backup.Success,
			Nodes:         map[string]*backup.NodeDescriptor{node: {Classes: []string{class}}},
		}
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(homePath)
		fs.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).
			Return(marshalCoordinatorMeta(meta), nil)
		fs.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).
			Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, backupID+"/"+node, BackupFile).
			Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, backupID, BackupFile).
			Return(nil, backup.ErrNotFound{})
		return fs
	}

	t.Run("a live reindex refuses the restore", func(t *testing.T) {
		fs := newFixture()
		fs.selector.reindexInFlightErr = errors.New("runtime-reindex in flight")

		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      backupID,
		}, false)

		require.Error(t, err)
		assert.IsTypef(t, backup.ErrUnprocessable{}, err,
			"a blocked restore is retryable, so 422; got %v", err)
		assert.Contains(t, err.Error(), "restore blocked")
		fs.backend.AssertNotCalled(t, "GetObject", ctx, backupID+"/"+node, BackupFile)
	})

	t.Run("without a live reindex the restore proceeds past the gate", func(t *testing.T) {
		fs := newFixture()

		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      backupID,
		}, false)
		// It fails further along on the missing per-node meta; what matters is
		// that the gate is not what stopped it, and that the schema was read.
		if err != nil {
			assert.NotContains(t, err.Error(), "restore blocked",
				"an idle cluster must not be refused by the reindex gate")
		}
		fs.backend.AssertCalled(t, "GetObject", ctx, backupID+"/"+node, BackupFile)
	})
}
