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
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
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
	// have touched — not even for a caller that named its own. The gate is asked
	// cluster-wide, so a migration anywhere blocks it.
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
		require.Len(t, fs.selector.reindexCollections, 1,
			"the gate must run exactly once on this arm")
		require.Nil(t, fs.selector.reindexCollections[0],
			"the question must be cluster-wide: this arm has no resolved class list to scope by")
	})

	// A caller that names its classes has already been authorized against
	// exactly those, above. Re-asking against the wildcard here answers 403 for
	// a mistyped id to every principal holding anything narrower than a grant on
	// all collections, and the handler maps Forbidden ahead of NotFound.
	t.Run("a per-class grant still gets the 404, not a 403", func(t *testing.T) {
		fs := newFixture(t)
		authz := &scopedAuthorizer{granted: authorization.Backups("Movies")}
		fs.auth = authz

		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      unknownID,
			Include: []string{"Movies"},
		}, false)

		require.Error(t, err)
		assert.IsTypef(t, backup.ErrNotFound{}, err,
			"a principal holding the grant it named must be told the id is unknown; got %v", err)
		require.NotContains(t, authz.asked, authorization.Backups(),
			"the wildcard grant is not what this caller needs")
	})
}

// scopedAuthorizer allows only the resources granted, the way a per-collection
// RBAC grant does, and records what it was asked.
type scopedAuthorizer struct {
	granted []string
	asked   [][]string
}

func (a *scopedAuthorizer) Authorize(_ context.Context, pr *models.Principal, verb string, resources ...string) error {
	a.asked = append(a.asked, resources)
	for _, r := range resources {
		if !slices.Contains(a.granted, r) {
			return authzerrors.NewForbidden(pr, verb, resources...)
		}
	}
	return nil
}

func (a *scopedAuthorizer) AuthorizeSilent(ctx context.Context, pr *models.Principal, verb string, resources ...string) error {
	return a.Authorize(ctx, pr, verb, resources...)
}

func (a *scopedAuthorizer) FilterAuthorizedResources(_ context.Context, _ *models.Principal, _ string, resources ...string) ([]string, error) {
	allowed := make([]string, 0, len(resources))
	for _, r := range resources {
		if slices.Contains(a.granted, r) {
			allowed = append(allowed, r)
		}
	}
	return allowed, nil
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

// Each restore arm has a different class list in hand, and the gate is scoped by
// it. An arm that passes the wrong list — or nil where it has classes — silently
// widens or narrows the check, and nothing else notices.
//
// The wildcard rows are the ones that need the meta: `include` accepts patterns
// and they only become class names inside validateRestoreRequest, so a gate
// asked before that is asked about a string no collection can equal.
func TestRestoreGateIsScopedPerArm(t *testing.T) {
	ctx := context.Background()
	const (
		backendName = "s3"
		backupID    = "1"
	)

	tests := []struct {
		name    string
		include []string
		want    []string
	}{
		{
			name:    "explicit include passes the requested classes",
			include: []string{"Movies", "Actors"},
			want:    []string{"Movies", "Actors"},
		},
		{
			name:    "a wildcard include is expanded before the gate sees it",
			include: []string{"Mov*"},
			want:    []string{"Movies"},
		},
		{
			name:    "a wildcard matching several classes carries them all",
			include: []string{"*s"},
			want:    []string{"Movies", "Actors"},
		},
		{
			name:    "a single-character wildcard is expanded too",
			include: []string{"Actor?"},
			want:    []string{"Actors"},
		},
		{
			name:    "no include gates on every class in the backup",
			include: nil,
			want:    []string{"Movies", "Actors"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := restoreMetaFixture(ctx, backupID, "Movies", "Actors")
			fs.selector.reindexInFlightErr = errors.New("runtime-reindex in flight")

			_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
				Backend: backendName,
				ID:      backupID,
				Include: tt.include,
			}, false)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "restore blocked")
			require.Len(t, fs.selector.reindexCollections, 1, "the gate must run exactly once on this arm")
			require.ElementsMatch(t, tt.want, fs.selector.reindexCollections[0],
				"the gate must be scoped to the classes this restore resolves to")
		})
	}

	// The one live migration the wildcard rows above are about: it must refuse a
	// restore whose pattern resolves onto it, and admit one whose pattern does not.
	t.Run("a wildcard restore is refused by a migration it resolves onto", func(t *testing.T) {
		for _, tc := range []struct {
			include []string
			refused bool
		}{
			{include: []string{"Mov*"}, refused: true},
			{include: []string{"Act*"}, refused: false},
		} {
			fs := restoreMetaFixture(ctx, backupID, "Movies", "Actors")
			fs.selector.reindexInFlightFor = func(collections []string) error {
				if slices.Contains(collections, "Movies") {
					return errors.New("runtime-reindex in flight")
				}
				return nil
			}

			_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
				Backend: backendName,
				ID:      backupID,
				Include: tc.include,
			}, false)

			if tc.refused {
				require.Error(t, err)
				assert.Containsf(t, err.Error(), "restore blocked",
					"a migration on Movies must refuse a restore of %v", tc.include)
				continue
			}
			if err != nil {
				assert.NotContainsf(t, err.Error(), "restore blocked",
					"a migration on Movies must not refuse a restore of %v", tc.include)
			}
		}
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

// restoreMetaFixture builds a scheduler whose backend serves a successful backup
// meta holding classes, so a restore reaches the gate with a real class list.
func restoreMetaFixture(ctx context.Context, backupID string, classes ...string) *fakeScheduler {
	const node = "Node-A"

	fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
	meta := backup.DistributedBackupDescriptor{
		ID:            backupID,
		StartedAt:     time.Now().UTC(),
		Version:       Version,
		ServerVersion: "1.23",
		Status:        backup.Success,
		Nodes:         map[string]*backup.NodeDescriptor{node: {Classes: classes}},
	}
	fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/backups/" + backupID)
	fs.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
	fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(marshalCoordinatorMeta(meta), nil)
	fs.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
	fs.backend.On("GetObject", ctx, backupID+"/"+node, BackupFile).Return(nil, backup.ErrNotFound{})
	fs.backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, backup.ErrNotFound{})
	return fs
}
