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

// recordingAuthorizer remembers what it was asked, so a test can tell an
// unauthorized answer from a merely-absent one. err refuses every check
// outright; otherwise only the granted resources pass, the way a per-collection
// RBAC grant does.
type recordingAuthorizer struct {
	err     error
	granted []string
	verbs   []string
	asked   [][]string
}

func (a *recordingAuthorizer) Authorize(_ context.Context, pr *models.Principal, verb string, resources ...string) error {
	a.verbs = append(a.verbs, verb)
	a.asked = append(a.asked, resources)
	if a.err != nil {
		return a.err
	}
	for _, r := range resources {
		if !slices.Contains(a.granted, r) {
			return authzerrors.NewForbidden(pr, verb, resources...)
		}
	}
	return nil
}

func (a *recordingAuthorizer) AuthorizeSilent(ctx context.Context, pr *models.Principal, verb string, resources ...string) error {
	return a.Authorize(ctx, pr, verb, resources...)
}

func (a *recordingAuthorizer) FilterAuthorizedResources(_ context.Context, _ *models.Principal, _ string, resources ...string) ([]string, error) {
	if a.err != nil {
		return nil, a.err
	}
	allowed := make([]string, 0, len(resources))
	for _, r := range resources {
		if slices.Contains(a.granted, r) {
			allowed = append(allowed, r)
		}
	}
	return allowed, nil
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
	)

	// The gate's answer is cluster-wide state. On this arm the request names no
	// classes and the meta does not exist, so there is nothing class-scoped to
	// authorize against and the broad grant is the only thing standing between a
	// principal with no backup permission at all and that answer.
	t.Run("an unauthorized caller is refused before the gate answers", func(t *testing.T) {
		fs := unknownIDFixture(ctx, unknownID)
		fs.selector.reindexInFlightErr = errors.New("runtime-reindex in flight")
		authz := &recordingAuthorizer{err: errors.New("forbidden")}
		fs.auth = authz

		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      unknownID,
		}, false)

		require.EqualError(t, err, "forbidden",
			"a caller with no backup permission must be refused, not told whether a reindex is running")
		require.Equal(t, [][]string{authorization.Backups()}, authz.asked,
			"the only check on this arm is the broad backup grant")
		require.Equal(t, []string{authorization.CREATE}, authz.verbs)
	})

	// The gate's answer is cluster-wide state, so the question has to be as
	// narrow as what the caller was authorized for. Without an include there is
	// nothing to narrow by and the broad grant above pays for the broad
	// question; with one, asking cluster-wide would tell a principal holding
	// only "Movies" that a migration is running on a collection they cannot see.
	t.Run("the unknown-id arm asks only about what the caller named", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			include     []string
			wantAsked   []string
			wantBlocked bool
		}{
			{
				name:        "no include asks cluster-wide",
				wantBlocked: true,
			},
			{
				name:      "an explicit include asks about those collections only",
				include:   []string{"Movies"},
				wantAsked: []string{"Movies"},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				fs := unknownIDFixture(ctx, unknownID)
				// A migration nobody asked about: it blocks the cluster-wide
				// question and nothing else.
				fs.selector.reindexInFlightFor = func(collections []string) error {
					if len(collections) == 0 {
						return errors.New("runtime-reindex in flight")
					}
					return nil
				}

				_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
					Backend: backendName,
					ID:      unknownID,
					Include: tc.include,
				}, false)

				require.Error(t, err)
				require.Len(t, fs.selector.reindexCollections, 1,
					"the gate must run exactly once on this arm")
				require.ElementsMatch(t, tc.wantAsked, fs.selector.reindexCollections[0])
				if tc.wantBlocked {
					assert.IsTypef(t, backup.ErrUnprocessable{}, err,
						"a migration must block a restore nobody scoped; got %v", err)
					return
				}
				assert.IsTypef(t, backup.ErrNotFound{}, err,
					"a migration on a collection this caller did not name must not be disclosed; got %v", err)
			})
		}
	})

	// A caller that names its classes has already been authorized against
	// exactly those, above. Re-asking against the wildcard here answers 403 for
	// a mistyped id to every principal holding anything narrower than a grant on
	// all collections, and the handler maps Forbidden ahead of NotFound.
	t.Run("a per-class grant still gets the 404, not a 403", func(t *testing.T) {
		fs := unknownIDFixture(ctx, unknownID)
		authz := &recordingAuthorizer{granted: authorization.Backups("Movies")}
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

// A restore that names no classes takes its own path through Restore: the class
// list only exists once the meta is read, so the gate sits after the meta read
// instead of before it. It still has to refuse, and refuse before any of the
// backup's schema is pulled.
func TestRestoreWithoutExplicitIncludeIsGated(t *testing.T) {
	ctx := context.Background()
	const (
		backupID    = "1"
		backendName = "s3"
		node        = "Node-A"
		class       = "Movies"
	)

	fs := restoreMetaFixture(ctx, backupID, class)
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

	require.Len(t, fs.selector.reindexCollections, 1, "the gate must run exactly once on this arm")
	require.Equal(t, []string{class}, fs.selector.reindexCollections[0],
		"once the meta is read this arm knows its classes and must scope the gate to them")
}

// The include a restore names is a pattern, not a class list: it only becomes
// class names inside validateRestoreRequest. A gate asked before that is asked
// about a string no collection can equal, and a gate asked about the backup's
// whole class list refuses restores the caller scoped away from the migration.
// Both are silent — the refusal still reads the same either way — so the rows
// below pin what the gate was asked, not only what it answered.
func TestRestoreGateIsScopedPerArm(t *testing.T) {
	ctx := context.Background()
	const (
		backendName = "s3"
		backupID    = "1"
	)

	tests := []struct {
		name    string
		include []string
		// wantAsked is what the gate must be handed once the pattern resolves.
		wantAsked []string
		refused   bool
	}{
		{
			name:      "a wildcard resolving onto the migration is refused",
			include:   []string{"Mov*"},
			wantAsked: []string{"Movies"},
			refused:   true,
		},
		{
			name:      "a wildcard resolving away from it is not",
			include:   []string{"Act*"},
			wantAsked: []string{"Actors"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := restoreMetaFixture(ctx, backupID, "Movies", "Actors")
			// The one live migration these rows are about.
			fs.selector.reindexInFlightFor = func(collections []string) error {
				if slices.Contains(collections, "Movies") {
					return errors.New("runtime-reindex in flight")
				}
				return nil
			}

			_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
				Backend: backendName,
				ID:      backupID,
				Include: tt.include,
			}, false)

			// Without this the admitting row is green even when Restore
			// returns before ever consulting the gate.
			require.Len(t, fs.selector.reindexCollections, 1, "the gate must run exactly once on this arm")
			require.ElementsMatch(t, tt.wantAsked, fs.selector.reindexCollections[0],
				"the gate must be scoped to the classes this restore resolves to")

			if tt.refused {
				require.Error(t, err)
				assert.Containsf(t, err.Error(), "restore blocked",
					"a migration on Movies must refuse a restore of %v", tt.include)
				return
			}
			if err != nil {
				assert.NotContainsf(t, err.Error(), "restore blocked",
					"a migration on Movies must not refuse a restore of %v", tt.include)
			}
		})
	}
}

// unknownIDFixture builds a scheduler whose backend holds no meta at all for id,
// so a restore of it runs into the not-found answer.
func unknownIDFixture(ctx context.Context, id string) *fakeScheduler {
	fs := newFakeScheduler(nil)
	fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("root/123")
	fs.backend.On("GetObject", ctx, id, GlobalBackupFile).
		Return(nil, backup.NewErrNotFound(errors.New("not found")))
	fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})
	return fs
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
