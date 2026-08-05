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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
)

// A restore that cannot run right now must say so before it says the id is
// unknown, the way authorization answers before either. Sending a caller to fix
// an id that was never the problem costs them a debugging round trip.
//
// Ordering under test, in full: authorization, then the reindex gate, then
// existence. The authorization half is pinned in auth_test.go.
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
