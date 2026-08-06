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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// recordingAuthorizer refuses every check and remembers what it was asked, so a
// test can tell an unauthorized answer from a merely-absent one.
type recordingAuthorizer struct {
	err    error
	verbs  []string
	assets [][]string
}

func (a *recordingAuthorizer) Authorize(_ context.Context, _ *models.Principal, verb string, resources ...string) error {
	a.verbs = append(a.verbs, verb)
	a.assets = append(a.assets, resources)
	return a.err
}

func (a *recordingAuthorizer) AuthorizeSilent(ctx context.Context, pr *models.Principal, verb string, resources ...string) error {
	return a.Authorize(ctx, pr, verb, resources...)
}

func (a *recordingAuthorizer) FilterAuthorizedResources(_ context.Context, _ *models.Principal, _ string, resources ...string) ([]string, error) {
	if a.err != nil {
		return nil, a.err
	}
	return resources, nil
}

// Pins the order Scheduler.Restore documents: authorization, then the reindex
// gate, then existence. The authorization half of the meta-found path is pinned
// in auth_test.go; the meta-not-found arm has no classes to authorize against
// and stands on its own broad grant, which is pinned below.
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

	// The gate's answer is cluster-wide state. On this arm the request names no
	// classes and the meta does not exist, so there is nothing class-scoped to
	// authorize against and the broad grant is the only thing standing between a
	// principal with no backup permission at all and that answer.
	t.Run("an unauthorized caller is refused before the gate answers", func(t *testing.T) {
		fs := newFixture(t)
		fs.selector.reindexInFlightErr = errors.New("runtime-reindex in flight")
		authz := &recordingAuthorizer{err: errors.New("forbidden")}
		fs.auth = authz

		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      unknownID,
		}, false)

		require.EqualError(t, err, "forbidden",
			"a caller with no backup permission must be refused, not told whether a reindex is running")
		require.Equal(t, [][]string{authorization.Backups()}, authz.assets,
			"the only check on this arm is the broad backup grant")
		require.Equal(t, []string{authorization.CREATE}, authz.verbs)
	})

	// The same arm with the grant held: the gate is what answers, and it
	// answers before existence.
	t.Run("an authorized caller reaches the gate", func(t *testing.T) {
		fs := newFixture(t)
		fs.selector.reindexInFlightErr = errors.New("runtime-reindex in flight")
		authz := &recordingAuthorizer{}
		fs.auth = authz

		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      unknownID,
		}, false)

		require.Error(t, err)
		assert.IsTypef(t, backup.ErrUnprocessable{}, err,
			"with the grant held the gate must answer 422; got %v", err)
		require.Equal(t, [][]string{authorization.Backups()}, authz.assets)
	})

	// The arm has no meta, so it cannot know which collections the backup would
	// have touched. Re-asking about the caller's own Include only repeats the
	// question already answered above and lets a migration anywhere else in the
	// cluster through.
	t.Run("the unknown-id arm asks the gate cluster-wide", func(t *testing.T) {
		fs := newFixture(t)
		fs.selector.reindexInFlightFor = func(collections []string) error {
			if len(collections) == 0 {
				return errors.New("runtime-reindex in flight")
			}
			return nil
		}

		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      unknownID,
			Include: []string{"Movies"},
		}, false)

		require.Error(t, err)
		assert.IsTypef(t, backup.ErrUnprocessable{}, err,
			"a migration outside the caller's classes must still block this restore; got %v", err)
		require.Len(t, fs.selector.reindexCollections, 2,
			"the caller's classes are asked about first, the cluster second")
		require.Nil(t, fs.selector.reindexCollections[1],
			"the second question must be cluster-wide; scoping it repeats the first")
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

		// Same shape as the arms in TestRestoreGateIsScopedPerArm; kept here
		// because this is where the meta-backed fixture already lives.
		require.Len(t, fs.selector.reindexCollections, 1, "the gate must run exactly once on this arm")
		require.Equal(t, []string{class}, fs.selector.reindexCollections[0],
			"once the meta is read this arm knows its classes and must scope the gate to them")
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

// Each restore arm has a different class list in hand, and the gate's node-local
// half is scoped by it. An arm that passes the wrong list — or nil where it has
// classes — silently widens or narrows the check, and nothing else notices.
func TestRestoreGateIsScopedPerArm(t *testing.T) {
	ctx := context.Background()
	const backendName = "s3"

	t.Run("explicit include passes the requested classes", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.selector.reindexInFlightErr = errors.New("runtime-reindex in flight")

		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      "some-backup",
			Include: []string{"Movies", "Actors"},
		}, false)

		require.Error(t, err)
		require.Len(t, fs.selector.reindexCollections, 1, "the gate must run exactly once on this arm")
		require.Equal(t, []string{"Movies", "Actors"}, fs.selector.reindexCollections[0],
			"the explicit-include arm knows its classes and must scope the gate to them")
	})

	t.Run("meta-not-found falls back to the blind check", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("root/123")
		fs.backend.On("GetObject", ctx, "no-such-backup", GlobalBackupFile).
			Return(nil, backup.NewErrNotFound(errors.New("not found")))
		fs.backend.On("GetObject", ctx, "no-such-backup", BackupFile).Return(nil, backup.ErrNotFound{})
		fs.selector.reindexInFlightErr = errors.New("runtime-reindex in flight")

		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      "no-such-backup",
		}, false)

		require.Error(t, err)
		require.Len(t, fs.selector.reindexCollections, 1)
		require.Empty(t, fs.selector.reindexCollections[0],
			"this arm answers before the meta is read, so it has no classes to scope by")
	})
}
