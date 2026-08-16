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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func reindexRefusal(collection string) error {
	return fmt.Errorf("restore blocked: %w: collection %q has an active runtime-reindex task",
		backup.ErrReindexInFlight, collection)
}

func reindexUndetermined() error {
	return fmt.Errorf("%w: the cluster task list could not be read",
		backup.ErrReindexActivityUndetermined)
}

func expectUnknownID(ctx context.Context, fs *fakeScheduler, id string) {
	fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + id)
	fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
	fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})
}

func expectKnownID(ctx context.Context, fs *fakeScheduler, id, cls string) {
	meta := backup.DistributedBackupDescriptor{
		ID: id, StartedAt: time.Now().UTC(), Version: Version,
		ServerVersion: "1.23", Status: backup.Success,
		Nodes: map[string]*backup.NodeDescriptor{nodeName: {Classes: []string{cls}}},
	}
	fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + id)
	fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(marshalCoordinatorMeta(meta), nil)
}

func restoreScheduler(ctx context.Context, id string, meta backup.DistributedBackupDescriptor,
	nodeMeta backup.BackupDescriptor, nodes ...string,
) *fakeScheduler {
	fs := newFakeScheduler(newFakeNodeResolver(nodes))
	fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + id)
	fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(marshalCoordinatorMeta(meta), nil)
	for _, node := range nodes {
		fs.backend.On("GetObject", ctx, id+"/"+node, BackupFile).Return(marshalMeta(nodeMeta), nil).Maybe()
	}
	fs.backend.On("GetObject", mock.Anything, mock.Anything, mock.Anything).Return(nil, backup.ErrNotFound{}).Maybe()
	return fs
}

func TestQuoteClassList(t *testing.T) {
	tests := []struct {
		name    string
		classes []string
		want    string
	}{
		{name: "none", want: "the collections being restored"},
		{name: "one", classes: []string{"Movies"}, want: `"Movies"`},
		{name: "at the cap", classes: []string{"A", "B", "C", "D", "E"}, want: `"A", "B", "C", "D", "E"`},
		{
			name:    "over the cap",
			classes: []string{"A", "B", "C", "D", "E", "F", "G"},
			want:    `"A", "B", "C", "D", "E" and 2 more`,
		},
		{
			name:    "arriving in map order",
			classes: []string{"G", "C", "A", "F", "B", "E", "D"},
			want:    `"A", "B", "C", "D", "E" and 2 more`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := append([]string(nil), tt.classes...)
			require.Equal(t, tt.want, quoteClassList(tt.classes))
			require.Equal(t, original, tt.classes, "the caller's slice is not reordered")
		})
	}
}

func TestRestoreRefusedByParticipant(t *testing.T) {
	err := restoreRefusedByParticipant([]string{"Movies", "Shows"})
	require.ErrorIs(t, err, backup.ErrReindexInFlight)
	assert.Contains(t, err.Error(), `"Movies"`)
	assert.Contains(t, err.Error(), `"Shows"`)
	assert.Contains(t, err.Error(), "retry after it finishes")
	assert.Contains(t, err.Error(), "GET /v1/tasks", "a rebuilt refusal still owes a route to check")

	assert.Equal(t, 1, strings.Count(err.Error(), backup.ErrReindexInFlight.Error()))
}

func TestCanCommitRefusalKeepsUnrelatedFailures(t *testing.T) {
	ctx := context.Background()
	refusal := func(class string) error {
		return fmt.Errorf("%w: collection %q has an active runtime-reindex task in DTM",
			backup.ErrBackupBlockedByInFlightReindex, class)
	}
	unchecked := fmt.Errorf("%w: the cluster task list could not be read",
		backup.ErrBackupReindexActivityUndetermined)
	tests := []struct {
		name            string
		classes         []string
		backupErr       error
		wantKind        CanCommitErrorKind
		wantContains    []string
		wantNotContains []string
	}{
		{
			name:      "one class, refused",
			classes:   []string{"Movies"},
			backupErr: refusal("Movies"),
			wantKind:  CanCommitErrInFlightReindex,
			// The rebuilt body names the collection, not the participant's shards.
			wantContains:    []string{`on "Movies"`},
			wantNotContains: []string{"in DTM", "at least one of"},
		},
		{
			name:      "several classes, all refused",
			classes:   []string{"Movies", "Shows"},
			backupErr: errors.Join(refusal("Movies"), refusal("Shows")),
			wantKind:  CanCommitErrInFlightReindex,
			// Which of the two migrates does not survive the hop.
			wantContains:    []string{`at least one of "Movies", "Shows"`},
			wantNotContains: []string{"in DTM"},
		},
		{
			name:      "a refusal next to a class that does not exist",
			classes:   []string{"Ghost", "Movies"},
			backupErr: errors.Join(errors.New("class Ghost doesn't exist"), refusal("Movies")),
			wantKind:  CanCommitErrCannotCommit,
			wantContains: []string{
				"class Ghost doesn't exist",
				`collection "Movies" has an active runtime-reindex task`,
			},
			wantNotContains: []string{`in progress on "Ghost"`},
		},
		{
			name:            "the node could not read the task list",
			classes:         []string{"Movies"},
			backupErr:       unchecked,
			wantKind:        CanCommitErrCreateReindexUndetermined,
			wantContains:    []string{"could not be determined", "retry once the cluster is reachable"},
			wantNotContains: []string{"in progress on", "/cancel"},
		},
		{
			name:         "one class refused, another one it could not check",
			classes:      []string{"Movies", "Shows"},
			backupErr:    errors.Join(refusal("Movies"), unchecked),
			wantKind:     CanCommitErrInFlightReindex,
			wantContains: []string{`at least one of "Movies", "Shows"`},
		},
		{
			name:            "neither class could be checked",
			classes:         []string{"Movies", "Shows"},
			backupErr:       errors.Join(unchecked, unchecked),
			wantKind:        CanCommitErrCreateReindexUndetermined,
			wantContains:    []string{"could not be determined"},
			wantNotContains: []string{"in progress on"},
		},
		{
			// Forwarded, not rebuilt: the cause is a setting on the refusing node.
			name:    "a configuration refusal no rebuild could state",
			classes: []string{"Movies", "Shows"},
			backupErr: backup.ReindexOverlapCheckError{
				Msg: backup.ErrReindexOverlapCheckUnanswerable.Error() +
					": DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS is 0; raise it above the time a " +
					"backup takes",
			},
			wantKind:        CanCommitErrOverlapCheckUnanswerable,
			wantContains:    []string{"DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS"},
			wantNotContains: []string{"in flight", "retry after it finishes", "Movies", "Shows"},
		},
		{
			// A quoted cancel would relabel this FAILED backup CANCELLED.
			name:            "a configuration refusal whose text quotes a cancelled context",
			classes:         []string{"Movies"},
			backupErr:       backup.ReindexOverlapCheckError{Msg: "the check cannot answer: " + context.Canceled.Error()},
			wantKind:        CanCommitErrOverlapCheckUnanswerable,
			wantContains:    []string{"a canceled context"},
			wantNotContains: []string{context.Canceled.Error()},
		},
		{
			// A node one release behind sets the kind and leaves the text empty.
			name:            "a configuration refusal that arrived without its text",
			classes:         []string{"Movies", "Shows"},
			backupErr:       backup.ReindexOverlapCheckError{},
			wantKind:        CanCommitErrOverlapCheckUnanswerable,
			wantContains:    []string{backup.ErrReindexOverlapCheckUnanswerable.Error()},
			wantNotContains: []string{"Movies", "Shows"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := newFakeBackend()
			backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/1")
			sourcer := &fakeSourcer{}
			sourcer.On("Backupable", ctx, mock.Anything).Return(tt.backupErr)
			m := createManager(sourcer, nil, backend, nil)
			resp := m.OnCanCommit(ctx, &Request{
				Method: OpCreate, ID: "1", Backend: "s3", Classes: tt.classes,
			})
			require.Equal(t, tt.wantKind, resp.ErrKind)

			published := canCommitErrFromResponse(resp, tt.classes)
			require.Error(t, published)
			for _, want := range tt.wantContains {
				assert.Contains(t, published.Error(), want)
			}
			for _, unwanted := range tt.wantNotContains {
				assert.NotContains(t, published.Error(), unwanted)
			}
		})
	}
}

func TestIsReindexRefusal(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil},
		{name: "unrelated", err: errors.New("raft: leader unreachable")},
		{name: "restore sentinel", err: reindexRefusal("Movies"), want: true},
		{name: "backup sentinel", err: fmt.Errorf("canCommit: %w", backup.ErrBackupBlockedByInFlightReindex), want: true},
		{
			name: "restore sentinel behind a join",
			err:  errors.Join(errors.New("other"), reindexRefusal("Movies")),
			want: true,
		},
		{name: "backup undetermined sentinel", err: backupUndeterminedByParticipant(), want: true},
		{name: "the generic canCommit failure", err: errCannotCommit},
		{
			name: "the unanswerable overlap check",
			err:  fmt.Errorf("canCommit: %w", backup.ErrReindexOverlapCheckUnanswerable),
			want: true,
		},
		{name: "a commit-time overlap", err: fmt.Errorf("commit: %w", backup.ErrReindexOverlappedBackup)},
		{name: "a commit-time undetermined overlap", err: fmt.Errorf("commit: %w", backup.ErrReindexOverlapUndetermined)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isReindexRefusal(tt.err))
		})
	}
	t.Run("an observed migration outranks a node that could not check", func(t *testing.T) {
		assert.Less(t, refusalRank(backupUndeterminedByParticipant()),
			refusalRank(backupRefusedByParticipant([]string{"Movies"})))
	})
}

// errors.Join drops nil members, so an empty join needs its own shape.
type emptyJoin struct{}

func (emptyJoin) Error() string   { return "nothing went wrong" }
func (emptyJoin) Unwrap() []error { return nil }

func TestAllReindexRefusals(t *testing.T) {
	refusal := reindexRefusal("Movies")
	failure := errors.New("connection refused")
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil"},
		{name: "an unrelated failure", err: failure},
		{name: "a lone refusal", err: refusal, want: true},
		{name: "a lone undetermined answer", err: reindexUndetermined(), want: true},
		{name: "the blocked-error leaf", err: backup.ReindexBlockedError{Msg: "blocked"}, want: true},
		{name: "two refusals", err: errors.Join(refusal, reindexUndetermined()), want: true},
		{name: "joined refusals nested one deeper", err: errors.Join(errors.Join(refusal, refusal), refusal), want: true},
		{name: "a refusal beside a permanent failure", err: errors.Join(refusal, failure)},
		{
			name: "the same pair behind a wrapper",
			err:  fmt.Errorf("canCommit: %w", errors.Join(refusal, failure)),
		},
		{name: "a join of nothing", err: emptyJoin{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, allReindexRefusals(tt.err))
		})
	}
}

// The configuration refusal wins: the others end when the migration does, and it
// does not end until an operator changes a setting.
func TestRefusalRank(t *testing.T) {
	rank := func(sentinel error) int { return refusalRank(fmt.Errorf("canCommit: %w", sentinel)) }

	undetermined := rank(backup.ErrReindexActivityUndetermined)
	inFlight := rank(backup.ErrBackupBlockedByInFlightReindex)
	unanswerable := rank(backup.ErrReindexOverlapCheckUnanswerable)

	require.Equal(t, inFlight, rank(backup.ErrReindexInFlight),
		"both in-flight refusals name a migration to wait for")
	require.Greater(t, inFlight, undetermined)
	require.Greater(t, unanswerable, inFlight)
}

func TestRestoreGateOrdering(t *testing.T) {
	const (
		cls         = "MyClass"
		backendName = "s3"
		id          = "1234"
	)
	ctx := context.Background()
	t.Run("a mistyped id is refused before it is reported missing", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.selector.setReindexGate(reindexRefusal(cls))
		expectUnknownID(ctx, fs, id)
		_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
			Backend: backendName, ID: id, Include: []string{cls},
		}, false)
		require.Error(t, err)
		require.ErrorAs(t, err, &backup.ErrUnprocessable{},
			"the gate's 422 must win over the 404")
		require.NotErrorAs(t, err, &backup.ErrNotFound{})
		assert.Contains(t, err.Error(), backup.ErrReindexInFlight.Error())
		require.Equal(t, [][]string{{cls}}, fs.selector.gateCalls(),
			"the gate is asked once, about the caller's own literal include")
	})
	t.Run("a mistyped id still 404s when nothing is migrating", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		expectUnknownID(ctx, fs, id)
		_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
			Backend: backendName, ID: id, Include: []string{cls},
		}, false)
		require.ErrorAs(t, err, &backup.ErrNotFound{})
	})
	t.Run("naming no class at all is asked cluster-wide", func(t *testing.T) {
		auth := authorization.NewMockAuthorizer(t)
		auth.EXPECT().Authorize(mock.Anything, mock.Anything, authorization.CREATE, "backups/collections/*").
			Return(nil).Once()
		auth.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).Maybe()
		fs := newFakeScheduler(nil)
		fs.auth = auth
		expectUnknownID(ctx, fs, id)
		_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
			Backend: backendName, ID: id,
		}, false)
		require.ErrorAs(t, err, &backup.ErrNotFound{})
		require.Equal(t, [][]string{nil}, fs.selector.gateCalls(),
			"a restore naming nothing covers everything, so the gate is asked about everything")
	})
	t.Run("a literal include is never checked for cluster-wide permission", func(t *testing.T) {
		auth := authorization.NewMockAuthorizer(t)
		auth.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).Maybe()
		fs := newFakeScheduler(nil)
		fs.auth = auth
		fs.selector.setReindexGate(reindexRefusal(cls))
		expectUnknownID(ctx, fs, id)
		_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
			Backend: backendName, ID: id, Include: []string{cls},
		}, false)
		require.Error(t, err)
		auth.AssertNotCalled(t, "Authorize",
			mock.Anything, mock.Anything, authorization.CREATE, "backups/collections/*")
	})
	for _, include := range [][]string{{"MyCl*"}, {"MyCl*", "Other"}, {"Other", "MyCl*"}} {
		t.Run("a wildcard in "+strings.Join(include, ",")+" widens the question", func(t *testing.T) {
			fs := newFakeScheduler(nil)
			expectUnknownID(ctx, fs, id)
			_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
				Backend: backendName, ID: id, Include: include,
			}, false)
			require.Error(t, err)
			require.Equal(t, [][]string{nil}, fs.selector.gateCalls(),
				"an unresolvable pattern must be asked cluster-wide, never as a literal name")
		})
	}
	t.Run("a known id is gated on the resolved class list", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.selector.setReindexGate(reindexRefusal(cls))
		expectKnownID(ctx, fs, id, cls)
		_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
			Backend: backendName, ID: id, Include: []string{cls},
		}, false)
		require.Error(t, err)
		require.ErrorAs(t, err, &backup.ErrUnprocessable{})
		assert.Contains(t, err.Error(), backup.ErrReindexInFlight.Error())
		require.Equal(t, [][]string{{cls}}, fs.selector.gateCalls(),
			"asked once, about the resolved class names")
		fs.backend.AssertNotCalled(t, "GetObject", ctx, id, BackupFile)
	})
	t.Run("a wildcard include on a known id is gated on what it resolved to", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.selector.setReindexGate(reindexRefusal(cls))
		expectKnownID(ctx, fs, id, cls)
		_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
			Backend: backendName, ID: id, Include: []string{"MyCl*"},
		}, false)
		require.Error(t, err)
		require.Equal(t, [][]string{{cls}}, fs.selector.gateCalls(),
			"the descriptor resolved the pattern, so the gate gets the class name")
	})
}

func TestRestoreGateAuthorizationPrecedesDisclosure(t *testing.T) {
	const (
		backendName = "s3"
		id          = "1234"
	)
	ctx := context.Background()
	auth := mocks.NewMockAuthorizer()
	auth.SetErr(errors.New("forbidden"))
	fs := newFakeScheduler(nil)
	fs.auth = auth
	fs.selector.setReindexGate(reindexRefusal("SomeoneElsesClass"))
	expectUnknownID(ctx, fs, id)
	_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
		Backend: backendName, ID: id,
	}, false)
	require.ErrorAs(t, err, &backup.ErrNotFound{})
	assert.NotContains(t, err.Error(), backup.ErrReindexInFlight.Error())
	assert.Empty(t, fs.selector.gateCalls(),
		"an unauthorized caller must not reach the gate at all")
}

func TestParticipantRestoreGate(t *testing.T) {
	ctx := context.Background()
	req := &Request{Method: OpRestore, ID: "1", Backend: "s3", Classes: []string{"MyClass"}}
	t.Run("refuses before it reads the backup", func(t *testing.T) {
		sourcer := &fakeSourcer{}
		sourcer.setReindexGate(reindexRefusal("MyClass"))
		backend := newFakeBackend()
		m := createManager(sourcer, nil, backend, nil)
		resp := m.OnCanCommit(ctx, req)
		assert.Equal(t, CanCommitErrRestoreBlockedByReindex, resp.ErrKind)
		assert.Contains(t, resp.Err, "restore blocked")
		assert.NotContains(t, resp.Err, nodeName)
		assert.NotContains(t, resp.Err, `shard "`)
		assert.Zero(t, resp.Timeout, "a refused participant promises nothing")
		require.Equal(t, [][]string{{"MyClass"}}, sourcer.gateCalls())
		backend.AssertNotCalled(t, "GetObject", mock.Anything, mock.Anything, mock.Anything)
	})
	t.Run("a node narrowed to no collection is gated on all of them", func(t *testing.T) {
		// An empty list is not "nothing to stage": the node restores its whole descriptor.
		sourcer := &fakeSourcer{}
		sourcer.setReindexGate(reindexRefusal("SomeoneElsesClass"))
		m := createManager(sourcer, nil, newFakeBackend(), nil)
		resp := m.OnCanCommit(ctx, &Request{Method: OpRestore, ID: "1", Backend: "s3"})
		assert.Equal(t, [][]string{nil}, sourcer.gateCalls())
		assert.Equal(t, CanCommitErrRestoreBlockedByReindex, resp.ErrKind)
		assert.Contains(t, resp.Err, backup.ErrReindexInFlight.Error())
	})
	t.Run("a gate that could not check sends its own kind", func(t *testing.T) {
		sourcer := &fakeSourcer{}
		sourcer.setReindexGate(reindexUndetermined())
		m := createManager(sourcer, nil, newFakeBackend(), nil)
		resp := m.OnCanCommit(ctx, req)
		assert.Equal(t, CanCommitErrRestoreReindexUndetermined, resp.ErrKind)
		assert.Zero(t, resp.Timeout)
	})
}

func TestRestoreUndeterminedReaches422(t *testing.T) {
	const (
		backendName = "s3"
		id          = "1234"
		cls         = "Movies"
		node1       = "node1"
		node2       = "node2"
	)
	ctx := context.Background()
	meta := backup.DistributedBackupDescriptor{
		ID: id, StartedAt: time.Now().UTC(), Version: Version,
		ServerVersion: "1.23", Status: backup.Success, Leader: node1,
		Nodes: map[string]*backup.NodeDescriptor{
			node1: {Classes: []string{cls}},
			node2: {Classes: []string{cls}},
		},
	}
	nodeMeta := backup.BackupDescriptor{
		ID: id, Status: backup.Success,
		Classes: []backup.ClassDescriptor{{Name: cls}},
	}
	fs := restoreScheduler(ctx, id, meta, nodeMeta, node1, node2)
	fs.client.On("CanCommit", mock.Anything, mock.Anything, mock.Anything).Return(&CanCommitResponse{
		Method:  OpRestore,
		ID:      id,
		Err:     reindexUndetermined().Error(),
		ErrKind: CanCommitErrRestoreReindexUndetermined,
	}, nil)
	fs.client.On("Abort", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
		Backend: backendName, ID: id,
	}, false)
	require.Error(t, err)
	require.ErrorAs(t, err, &backup.ErrUnprocessable{},
		"a refusal the caller can retry is 422, not 500")
	assert.Contains(t, err.Error(), backup.ErrReindexActivityUndetermined.Error())
	assert.NotContains(t, err.Error(), backup.ErrReindexInFlight.Error())
	assert.NotContains(t, err.Error(), "has an active runtime-reindex task")
	assert.NotContains(t, err.Error(), "runtime-reindex work is in progress")
	assert.NotContains(t, err.Error(), node2, "a cluster fact names no node")
}

func TestBackupRefusalReaches422(t *testing.T) {
	const id, cls, node = "1234", "Movies", "node1"
	ctx, any := context.Background(), mock.Anything
	fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
	fs.selector.On("ListClasses", ctx).Return([]string{cls})
	fs.selector.On("Backupable", ctx, []string{cls}).Return(nil)
	fs.selector.On("Shards", ctx, cls).Return([]string{node}, nil)
	expectUnknownID(ctx, fs, id)
	fs.backend.On("Initialize", ctx, any).Return(nil)
	fs.client.On("CanCommit", any, node, any).Return(&CanCommitResponse{
		Method: OpCreate, ID: id, ErrKind: CanCommitErrInFlightReindex,
	}, nil)
	_, err := fs.scheduler().Backup(ctx, &models.Principal{}, &BackupRequest{
		Backend: "s3", ID: id, Include: []string{cls},
	})
	require.ErrorAs(t, err, &backup.ErrUnprocessable{},
		"a refusal the caller can retry is 422, not 500")
	assert.Contains(t, err.Error(), backup.ErrBackupBlockedByInFlightReindex.Error())
	assert.Contains(t, err.Error(), "GET /v1/tasks", "and the only answer the operator gets owes a route")
}

// The create route ranks the same way the restore route does: a peer that failed for its
// own reason is permanent, so the pair is not something retrying the backup can fix.
func TestBackupRefusalBesideAPeerFailureIsNot422(t *testing.T) {
	const id, cls, node1, node2 = "1234", "Movies", "node1", "node2"
	ctx, any := context.Background(), mock.Anything
	fs := newFakeScheduler(newFakeNodeResolver([]string{node1, node2}))
	fs.selector.On("ListClasses", ctx).Return([]string{cls})
	fs.selector.On("Backupable", ctx, []string{cls}).Return(nil)
	fs.selector.On("Shards", ctx, cls).Return([]string{node1, node2}, nil)
	expectUnknownID(ctx, fs, id)
	fs.backend.On("Initialize", ctx, any).Return(nil)
	// node2 answers only once the refusal is in, so the refusal is what the error group returns.
	refused := make(chan struct{})
	fs.client.On("CanCommit", any, node1, any).
		Run(func(mock.Arguments) { close(refused) }).
		Return(&CanCommitResponse{Method: OpCreate, ID: id, ErrKind: CanCommitErrInFlightReindex}, nil)
	fs.client.On("CanCommit", any, node2, any).
		Run(func(mock.Arguments) { <-refused }).
		Return(nil, errors.New("connection refused"))
	fs.client.On("Abort", any, any, any).Return(nil).Maybe()

	_, err := fs.scheduler().Backup(ctx, &models.Principal{}, &BackupRequest{
		Backend: "s3", ID: id, Include: []string{cls},
	})
	require.Error(t, err)
	require.NotErrorAs(t, err, &backup.ErrUnprocessable{},
		"waiting out the migration cannot fix a peer that failed permanently")
	assert.Contains(t, err.Error(), backup.ErrBackupBlockedByInFlightReindex.Error())
	assert.Contains(t, err.Error(), "connection refused")
}

func TestPublishAsCancelled(t *testing.T) {
	tests := []struct {
		name        string
		err, ctxErr error
		want        bool
	}{
		{
			name: "a refusal whose cause was cancelled",
			err:  fmt.Errorf("%w: %w", backup.ErrBackupBlockedByInFlightReindex, context.Canceled),
		},
		{name: "a restore refusal whose cause was cancelled", err: fmt.Errorf("%w: %w", backup.ErrReindexInFlight, context.Canceled)},
		{
			name: "an observed overlap whose cause was cancelled",
			err:  fmt.Errorf("%w: %w", backup.ErrReindexOverlappedBackup, context.Canceled),
		},
		{
			name: "an unanswerable overlap check whose cause was cancelled",
			err:  fmt.Errorf("%w: %w", backup.ErrReindexOverlapUndetermined, context.Canceled),
		},
		{
			// A simultaneous operator cancel does not make the refusal an abort.
			name:   "an observed overlap on a cancelled operation",
			err:    fmt.Errorf("%w: x", backup.ErrReindexOverlappedBackup),
			ctxErr: context.Canceled,
		},
		{
			name:   "an unanswerable overlap check on a cancelled operation",
			err:    fmt.Errorf("%w: x", backup.ErrReindexOverlapUndetermined),
			ctxErr: context.Canceled,
		},
		{name: "a plain cancellation", err: context.Canceled, want: true},
		{name: "a cancelled operation context", ctxErr: context.Canceled, want: true},
		{name: "a refusal racing an operator abort", err: fmt.Errorf("%w: x", backup.ErrReindexInFlight), ctxErr: context.Canceled, want: true},
		{name: "an unrelated failure", err: errors.New("no space left on device")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, publishAsCancelled(tt.err, tt.ctxErr))
		})
	}
}

func TestCanCommitRefusalOutranksPeerFailure(t *testing.T) {
	const (
		backendName = "s3"
		id          = "1234"
		clsA        = "Movies"
		clsB        = "Shows"
		node1       = "node1"
		node2       = "node2"
	)
	ctx := context.Background()
	any := mock.Anything
	meta := backup.DistributedBackupDescriptor{
		ID: id, StartedAt: time.Now().UTC(), Version: Version,
		ServerVersion: "1.23", Status: backup.Success, Leader: node1,
		Nodes: map[string]*backup.NodeDescriptor{
			node1: {Classes: []string{clsA}},
			node2: {Classes: []string{clsB}},
		},
	}
	nodeMeta := backup.BackupDescriptor{
		ID: id, Status: backup.Success,
		Classes: []backup.ClassDescriptor{{Name: clsA}, {Name: clsB}},
	}
	type answer struct {
		resp *CanCommitResponse
		err  error
	}
	refusalNaming := func(cls string) *CanCommitResponse {
		return &CanCommitResponse{
			Method: OpRestore, ID: id,
			Err:     reindexRefusal(cls).Error(),
			ErrKind: CanCommitErrRestoreBlockedByReindex,
		}
	}

	// The node not answering first unblocks only once the error group recorded the other answer.
	restore := func(t *testing.T, first string, answers map[string]answer) error {
		t.Helper()
		var inFlight sync.WaitGroup
		inFlight.Add(len(answers))
		sequenced := func(node string) func(mock.Arguments) {
			return func(args mock.Arguments) {
				inFlight.Done()
				inFlight.Wait()
				if node == first {
					return
				}
				<-args.Get(0).(context.Context).Done()
			}
		}
		fs := restoreScheduler(ctx, id, meta, nodeMeta, node1, node2)
		fs.client.On("Abort", any, any, any).Return(nil).Maybe()
		for node, a := range answers {
			call := fs.client.On("CanCommit", any, node, any)
			if a.err != nil {
				call.Return(nil, a.err)
			} else {
				call.Return(a.resp, nil)
			}
			call.Run(sequenced(node))
		}
		_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
			Backend: backendName, ID: id,
		}, false)
		require.Error(t, err)
		return err
	}

	t.Run("a refusal outranks a peer's own failure", func(t *testing.T) {
		for _, first := range []string{node1, node2} {
			t.Run("answering first: "+first, func(t *testing.T) {
				err := restore(t, first, map[string]answer{
					node1: {err: errors.New("connection refused")},
					node2: {resp: refusalNaming(clsB)},
				})
				require.NotErrorAs(t, err, &backup.ErrUnprocessable{},
					"waiting out the migration cannot fix a peer that failed permanently")
				assert.Contains(t, err.Error(), backup.ErrReindexInFlight.Error())
				assert.Contains(t, err.Error(), "connection refused",
					"a peer that failed for its own reason must travel with the refusal")
				assert.NotContains(t, err.Error(), node2, "the refusing node is still not named")
			})
		}
	})

	t.Run("a peer the refusal itself cut short is not a second failure", func(t *testing.T) {
		err := restore(t, node2, map[string]answer{
			node1: {err: context.Canceled},
			node2: {resp: refusalNaming(clsB)},
		})
		require.ErrorAs(t, err, &backup.ErrUnprocessable{},
			"the refusal cancels its own siblings; that cancellation must not make it permanent")
		assert.NotContains(t, err.Error(), context.Canceled.Error())
	})

	t.Run("an observed migration outranks a node that could not check", func(t *testing.T) {
		undetermined := &CanCommitResponse{
			Method: OpRestore, ID: id,
			Err:     reindexUndetermined().Error(),
			ErrKind: CanCommitErrRestoreReindexUndetermined,
		}
		for _, first := range []string{node1, node2} {
			t.Run("answering first: "+first, func(t *testing.T) {
				err := restore(t, first, map[string]answer{
					node1: {resp: undetermined},
					node2: {resp: refusalNaming(clsB)},
				})
				assert.Contains(t, err.Error(), backup.ErrReindexInFlight.Error(),
					"the node that saw the migration says what to wait for")
				assert.NotContains(t, err.Error(), backup.ErrReindexActivityUndetermined.Error())
			})
		}
	})

	t.Run("two refusing nodes describe the same restore", func(t *testing.T) {
		bodies := make([]string, 0, 2)
		for _, first := range []string{node1, node2} {
			bodies = append(bodies, restore(t, first, map[string]answer{
				node1: {resp: refusalNaming(clsA)},
				node2: {resp: refusalNaming(clsB)},
			}).Error())
		}
		for _, body := range bodies {
			assert.Contains(t, body, strconv.Quote(clsA))
			assert.Contains(t, body, strconv.Quote(clsB),
				"the refusal covers the restore, not the classes co-resident with the node that answered")
		}
		require.Equal(t, bodies[0], bodies[1],
			"the same restore must be refused in the same words whichever node answered first")
	})
}

func TestCanCommitKeepsAnUnresolvableHost(t *testing.T) {
	const (
		id           = "1234"
		cls          = "Movies"
		node1, node2 = "N1", "N2"
	)
	any := mock.Anything
	refused := make(chan struct{})
	var asked atomic.Int32
	resolver := newFakeNodeResolver([]string{node1, node2})
	// A slow second lookup: the refusal wins the first-error race, which is where a host
	// that cannot be resolved used to vanish.
	resolver.resolve = func(node string) (string, bool) {
		if asked.Add(1) == 1 {
			return node, true
		}
		<-refused
		time.Sleep(100 * time.Millisecond)
		return "", false
	}
	fc := newFakeCoordinator(resolver)
	fc.client.On("CanCommit", any, any, any).
		Run(func(mock.Arguments) { close(refused) }).
		Return(&CanCommitResponse{
			Method: OpCreate, ID: id, ErrKind: CanCommitErrInFlightReindex,
		}, nil)
	fc.client.On("Abort", any, any, any).Return(nil).Maybe()

	c := fc.coordinator()
	c.descriptor = &backup.DistributedBackupDescriptor{ID: id, Nodes: map[string]*backup.NodeDescriptor{
		node1: {Classes: []string{cls}}, node2: {Classes: []string{cls}},
	}}
	_, err := c.canCommit(context.Background(), &Request{Method: OpCreate, ID: id, Classes: []string{cls}})

	assert.ErrorIs(t, err, backup.ErrBackupBlockedByInFlightReindex)
	assert.Contains(t, err.Error(), "cannot resolve hostname",
		"a host that could not be resolved must travel with the refusal, not be swallowed by it")
}

// The fan-out's own producer answers the operation context too. A cancellation is not a
// peer's failure and must not make the refusal permanent; a deadline is one and must not
// go missing behind it.
func TestCanCommitRanksTheProducersContextError(t *testing.T) {
	const id, cls = "1234", "Movies"
	any := mock.Anything
	for _, tt := range []struct {
		name      string
		cancel    bool
		timeout   time.Duration
		retryable bool
	}{
		{name: "cancelled mid-fan-out", cancel: true, timeout: time.Minute, retryable: true},
		{name: "the shared canCommit deadline expires", timeout: 40 * time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var asked atomic.Int32
			resolver := newFakeNodeResolver([]string{"N1", "N2", "N3"})
			// The producer's next loop top answers; the slow peers below make that answer
			// the first error the group keeps.
			resolver.resolve = func(node string) (string, bool) {
				if asked.Add(1) == 2 {
					if tt.cancel {
						cancel()
					} else {
						time.Sleep(80 * time.Millisecond)
					}
				}
				return node, true
			}
			fc := newFakeCoordinator(resolver)
			fc.client.On("CanCommit", any, any, any).
				Run(func(mock.Arguments) { time.Sleep(300 * time.Millisecond) }).
				Return(&CanCommitResponse{Method: OpCreate, ID: id, ErrKind: CanCommitErrInFlightReindex}, nil)
			fc.client.On("Abort", any, any, any).Return(nil).Maybe()

			c := fc.coordinator()
			c.timeoutCanCommit = tt.timeout
			c.descriptor = &backup.DistributedBackupDescriptor{ID: id, Nodes: map[string]*backup.NodeDescriptor{
				"N1": {Classes: []string{cls}}, "N2": {Classes: []string{cls}}, "N3": {Classes: []string{cls}},
			}}
			_, err := c.canCommit(ctx, &Request{Method: OpCreate, ID: id, Classes: []string{cls}})

			require.ErrorIs(t, err, backup.ErrBackupBlockedByInFlightReindex)
			assert.Equal(t, tt.retryable, allReindexRefusals(err), "%v", err)
		})
	}
}

func TestRestoreKeepsNodesNarrowedToNothing(t *testing.T) {
	const (
		backendName = "s3"
		id          = "1234"
		allowedCls  = "Movies"
		deniedCls   = "Shows"
		node1       = "node1"
		node2       = "node2"
	)
	ctx := context.Background()
	any := mock.Anything
	auth := authorization.NewMockAuthorizer(t)
	auth.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything,
		"backups/collections/"+deniedCls).
		Return(authzerrors.NewForbidden(&models.Principal{}, "create")).Maybe()
	auth.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()
	meta := backup.DistributedBackupDescriptor{
		ID: id, StartedAt: time.Now().UTC(), Version: Version,
		ServerVersion: "1.23", Status: backup.Success, Leader: node1,
		Nodes: map[string]*backup.NodeDescriptor{
			node1: {Classes: []string{allowedCls}},
			node2: {Classes: []string{deniedCls}},
		},
	}
	nodeMeta := backup.BackupDescriptor{
		ID: id, Status: backup.Success,
		Classes: []backup.ClassDescriptor{{Name: allowedCls}},
	}
	fs := restoreScheduler(ctx, id, meta, nodeMeta, node1, node2)
	fs.auth = auth
	// PutObject must fail: it ends the restore before the commit phase races the assertions.
	fs.client.On("CanCommit", any, any, any).
		Return(&CanCommitResponse{Method: OpRestore, ID: id, Timeout: time.Minute}, nil)
	fs.backend.On("PutObject", any, any, GlobalRestoreFile, any).
		Return(errors.New("bucket is read only"))
	fs.client.On("Abort", any, any, any).Return(nil).Maybe()

	_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
		Backend: backendName, ID: id,
	}, false)
	require.ErrorContains(t, err, "put initial metadata",
		"the restore must have got past the fan-out")

	asked := map[string]*Request{}
	for _, call := range fs.client.Calls {
		if call.Method == "CanCommit" {
			req := call.Arguments.Get(2).(*Request)
			asked[req.NodeName] = req
		}
	}
	require.Contains(t, asked, node2,
		"the node the authorization filter narrowed to nothing must still be asked "+
			"to commit, or the blobs only it holds are never restored")
	assert.Empty(t, asked[node2].Classes, "and asked with the empty class list it was left with")
	assert.Equal(t, []string{allowedCls}, asked[node1].Classes)

	// validate() reads that empty list as the node's whole descriptor, so it restores
	// every collection it holds and its request must reach the gate. Replayed through a
	// real handler, so no future mark on the request can make the participant skip.
	sourcer := &fakeSourcer{}
	sourcer.setReindexGate(reindexRefusal(deniedCls))
	backend := newFakeBackend()
	backend.On("HomeDir", any, any, any).Return("bucket/" + id)
	backend.On("GetObject", any, any, any).Return(nil, backup.ErrNotFound{})
	resp := createManager(sourcer, nil, backend, nil).OnCanCommit(ctx, asked[node2])
	require.Len(t, sourcer.gateCalls(), 1, "the emptied node must still reach the gate")
	assert.Empty(t, sourcer.gateCalls()[0], "and be asked as every collection")
	assert.Equal(t, CanCommitErrRestoreBlockedByReindex, resp.ErrKind)
}
