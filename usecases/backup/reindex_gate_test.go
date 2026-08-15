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

// reindexRefusal is what the gate hands back, shaped like the storage
// layer's: the sentinel plus a collection, no node and no shard.
func reindexRefusal(collection string) error {
	return fmt.Errorf("restore blocked: %w: collection %q has an active runtime-reindex task",
		backup.ErrReindexInFlight, collection)
}

func reindexUndetermined() error {
	return fmt.Errorf("%w: the cluster task list could not be read",
		backup.ErrReindexActivityUndetermined)
}

// TestQuoteClassList pins the cap. A restore can cover every collection
// in the cluster, and the refusal is the same one whichever is blocked.
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

// An older participant words its refusal in terms of its own shards and
// node name, neither of which the caller asked about or can act on.
func TestRestoreRefusedByParticipant(t *testing.T) {
	err := restoreRefusedByParticipant([]string{"Movies", "Shows"})
	require.ErrorIs(t, err, backup.ErrReindexInFlight)
	assert.Contains(t, err.Error(), `"Movies"`)
	assert.Contains(t, err.Error(), `"Shows"`)
	assert.Contains(t, err.Error(), "retry after the migration finishes")

	// The sentinel is stated once even though a participant's own message
	// already opens with it.
	assert.Equal(t, 1, strings.Count(err.Error(), backup.ErrReindexInFlight.Error()))
}

// One call site sees both chains: canCommit carries a backup refusal and a
// restore refusal through the same fan-out.
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
		{name: "the generic canCommit failure", err: errCannotCommit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isReindexRefusal(tt.err))
		})
	}
}

// TestRestoreGateOrdering pins the gate's contract on both arms of
// Scheduler.Restore: what it is asked about, and what it is asked before.
func TestRestoreGateOrdering(t *testing.T) {
	const (
		cls         = "MyClass"
		backendName = "s3"
		id          = "1234"
	)
	ctx := context.Background()
	t.Run("a mistyped id is refused before it is reported missing", func(t *testing.T) {
		// Deliberate: a caller who cannot restore right now should be
		// told that, not sent to fix an id that was never the problem.
		fs := newFakeScheduler(nil)
		fs.selector.setReindexGate(reindexRefusal(cls))
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + id)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})
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
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + id)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})
		_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
			Backend: backendName, ID: id, Include: []string{cls},
		}, false)
		require.ErrorAs(t, err, &backup.ErrNotFound{})
	})
	t.Run("naming no class at all is asked cluster-wide", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + id)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})
		_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
			Backend: backendName, ID: id,
		}, false)
		require.ErrorAs(t, err, &backup.ErrNotFound{})
		require.Equal(t, [][]string{nil}, fs.selector.gateCalls(),
			"a restore naming nothing covers everything, so the gate is asked about everything")
	})
	t.Run("a literal include is never checked for cluster-wide permission", func(t *testing.T) {
		// The caller was already authorized on exactly these names one
		// step up. Asking again, silently, would let a caller without
		// cluster-wide permission fall through to the 404.
		auth := authorization.NewMockAuthorizer(t)
		auth.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).Maybe()
		fs := newFakeScheduler(nil)
		fs.auth = auth
		fs.selector.setReindexGate(reindexRefusal(cls))
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + id)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})
		_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
			Backend: backendName, ID: id, Include: []string{cls},
		}, false)
		require.Error(t, err)
		require.Equal(t, [][]string{{cls}}, fs.selector.gateCalls())
		auth.AssertNotCalled(t, "AuthorizeSilent",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})
	// One wildcard anywhere in the list settles it, including behind a
	// literal that follows it.
	for _, include := range [][]string{{"MyCl*"}, {"MyCl*", "Other"}, {"Other", "MyCl*"}} {
		t.Run("a wildcard in "+strings.Join(include, ",")+" widens the question", func(t *testing.T) {
			fs := newFakeScheduler(nil)
			fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + id)
			fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
			fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})
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
		meta := backup.DistributedBackupDescriptor{
			ID: id, StartedAt: time.Now().UTC(), Version: Version,
			ServerVersion: "1.23", Status: backup.Success,
			Nodes: map[string]*backup.NodeDescriptor{nodeName: {Classes: []string{cls}}},
		}
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + id)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(marshalCoordinatorMeta(meta), nil)
		_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
			Backend: backendName, ID: id, Include: []string{cls},
		}, false)
		require.Error(t, err)
		require.ErrorAs(t, err, &backup.ErrUnprocessable{})
		assert.Contains(t, err.Error(), backup.ErrReindexInFlight.Error())
		require.Equal(t, [][]string{{cls}}, fs.selector.gateCalls(),
			"asked once, about the resolved class names")
		// Refused before the backup's own schema is read: the gate's
		// answer does not depend on it.
		fs.backend.AssertNotCalled(t, "GetObject", ctx, id, BackupFile)
	})
	t.Run("a wildcard include on a known id is gated on what it resolved to", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.selector.setReindexGate(reindexRefusal(cls))
		meta := backup.DistributedBackupDescriptor{
			ID: id, StartedAt: time.Now().UTC(), Version: Version,
			ServerVersion: "1.23", Status: backup.Success,
			Nodes: map[string]*backup.NodeDescriptor{nodeName: {Classes: []string{cls}}},
		}
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + id)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(marshalCoordinatorMeta(meta), nil)
		_, err := fs.scheduler().Restore(ctx, &models.Principal{}, &BackupRequest{
			Backend: backendName, ID: id, Include: []string{"MyCl*"},
		}, false)
		require.Error(t, err)
		require.Equal(t, [][]string{{cls}}, fs.selector.gateCalls(),
			"the descriptor resolved the pattern, so the gate gets the class name")
	})
}

// TestRestoreGateAuthorizationPrecedesDisclosure pins that a caller without
// cluster-wide backup permission cannot learn from a mistyped id that a
// migration is running somewhere it cannot see.
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
	fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + id)
	fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
	fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})
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
		assert.Zero(t, resp.Timeout, "a refused participant promises nothing")
		require.Equal(t, [][]string{{"MyClass"}}, sourcer.gateCalls())
		backend.AssertNotCalled(t, "GetObject", mock.Anything, mock.Anything, mock.Anything)
	})
	t.Run("the refusal names no node and no shard", func(t *testing.T) {
		sourcer := &fakeSourcer{}
		sourcer.setReindexGate(reindexRefusal("MyClass"))
		m := createManager(sourcer, nil, newFakeBackend(), nil)
		resp := m.OnCanCommit(ctx, req)
		assert.NotContains(t, resp.Err, nodeName)
		assert.NotContains(t, resp.Err, `shard "`)
	})
	t.Run("a node narrowed to no collection is not gated", func(t *testing.T) {
		// It stays in the fan-out for the blobs only its own descriptor
		// holds. The gate reads an empty list as every collection, so asking
		// it here refuses the whole restore over a migration on a collection
		// this node was not asked to restore.
		sourcer := &fakeSourcer{}
		sourcer.setReindexGate(reindexRefusal("SomeoneElsesClass"))
		backend := newFakeBackend()
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/1")
		backend.On("GetObject", ctx, mock.Anything, BackupFile).Return(nil, backup.ErrNotFound{})
		m := createManager(sourcer, nil, backend, nil)
		resp := m.OnCanCommit(ctx, &Request{Method: OpRestore, ID: "1", Backend: "s3"})
		assert.Empty(t, sourcer.gateCalls(), "a node restoring nothing has nothing to gate")
		assert.NotEqual(t, CanCommitErrRestoreBlockedByReindex, resp.ErrKind)
		assert.NotContains(t, resp.Err, backup.ErrReindexInFlight.Error())
	})
	t.Run("a gate that could not check sends its own kind", func(t *testing.T) {
		// The kind is the only thing that survives the hop, so a gate that
		// observed nothing has to carry a kind of its own or the
		// coordinator rebuilds it as a migration it never saw.
		sourcer := &fakeSourcer{}
		sourcer.setReindexGate(reindexUndetermined())
		m := createManager(sourcer, nil, newFakeBackend(), nil)
		resp := m.OnCanCommit(ctx, req)
		assert.Equal(t, CanCommitErrRestoreReindexUndetermined, resp.ErrKind)
		assert.Zero(t, resp.Timeout)
	})
}

// TestRestoreUndeterminedReaches422 pins the whole hop end to end: a
// participant that could not check answers a kind of its own, and what
// Scheduler.Restore publishes says so.
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
	fs := newFakeScheduler(newFakeNodeResolver([]string{node1, node2}))
	fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + id)
	fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(marshalCoordinatorMeta(meta), nil)
	fs.backend.On("GetObject", ctx, id+"/"+node1, BackupFile).Return(marshalMeta(nodeMeta), nil).Maybe()
	fs.backend.On("GetObject", ctx, id+"/"+node2, BackupFile).Return(marshalMeta(nodeMeta), nil).Maybe()
	fs.backend.On("GetObject", mock.Anything, mock.Anything, mock.Anything).Return(nil, backup.ErrNotFound{}).Maybe()
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
	// ErrUnprocessable carries no Unwrap, so the published body is what a
	// caller reads, and it is what these assert on.
	assert.Contains(t, err.Error(), backup.ErrReindexActivityUndetermined.Error())
	assert.NotContains(t, err.Error(), backup.ErrReindexInFlight.Error())
	assert.NotContains(t, err.Error(), "has an active runtime-reindex task")
	assert.NotContains(t, err.Error(), "retry after the migration finishes")
	assert.NotContains(t, err.Error(), node2, "a cluster fact names no node")
}

// A cancellation from the gate's RAFT client is not somebody stopping the
// backup; CANCELLED would hide a refused capture behind a deliberate status.
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
		{name: "a plain cancellation", err: context.Canceled, want: true},
		{name: "a cancelled operation context", ctxErr: context.Canceled, want: true},
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

	// restore sequences the fan-out: neither participant returns until both
	// are in flight, and then the one not named first waits for the context
	// to be cancelled, which an error group does only after it has already
	// recorded the other answer.
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
		fs := newFakeScheduler(newFakeNodeResolver([]string{node1, node2}))
		fs.backend.On("HomeDir", any, any, any).Return("bucket/" + id)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(marshalCoordinatorMeta(meta), nil)
		fs.backend.On("GetObject", ctx, id+"/"+node1, BackupFile).Return(marshalMeta(nodeMeta), nil).Maybe()
		fs.backend.On("GetObject", ctx, id+"/"+node2, BackupFile).Return(marshalMeta(nodeMeta), nil).Maybe()
		fs.backend.On("GetObject", any, any, any).Return(nil, backup.ErrNotFound{}).Maybe()
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
				require.ErrorAs(t, err, &backup.ErrUnprocessable{},
					"a refusal is 422 whichever answer the fan-out recorded first")
				assert.Contains(t, err.Error(), backup.ErrReindexInFlight.Error())
				assert.NotContains(t, err.Error(), "connection refused")
				assert.NotContains(t, err.Error(), node1)
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

// TestRestoreKeepsNodesNarrowedToNothing pins that a node the authorization
// filter narrowed to nothing is still asked to commit: its dynamic-user and
// RBAC blobs come from its own descriptor, so dropping the node drops those.
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
	fs := newFakeScheduler(newFakeNodeResolver([]string{node1, node2}))
	fs.auth = auth
	fs.backend.On("HomeDir", any, any, any).Return("bucket/" + id)
	fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(marshalCoordinatorMeta(meta), nil)
	fs.backend.On("GetObject", ctx, id+"/"+node1, BackupFile).Return(marshalMeta(nodeMeta), nil).Maybe()
	fs.backend.On("GetObject", ctx, id+"/"+node2, BackupFile).Return(marshalMeta(nodeMeta), nil).Maybe()
	fs.backend.On("GetObject", any, any, any).Return(nil, backup.ErrNotFound{}).Maybe()
	// Every node accepts, so the fan-out runs whole and what each one was
	// asked stays observable. Failing the write that follows it ends the
	// restore on this goroutine, before the commit phase starts one of its
	// own that would still be calling the client during the assertions.
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

	asked := map[string][]string{}
	for _, call := range fs.client.Calls {
		if call.Method != "CanCommit" {
			continue
		}
		req := call.Arguments.Get(2).(*Request)
		asked[req.NodeName] = req.Classes
	}
	require.Contains(t, asked, node2,
		"the node the authorization filter narrowed to nothing must still be asked "+
			"to commit, or the blobs only it holds are never restored")
	assert.Empty(t, asked[node2], "and asked with the empty class list it was left with")
	assert.Equal(t, []string{allowedCls}, asked[node1])
}
