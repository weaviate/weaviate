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
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// indexForCheckpointTest assembles a minimal *Index with just the wiring
// needed by the per-shard receiver methods and resolveShardNames:
//   - shardCreateLocks for GetShard's per-key locking
//   - closingCtx so ForEachShard's "is the index closing?" check passes
//   - a logger so the structured log lines don't panic on nil
//   - a class name so the log fields render meaningfully
//
// closeLock is a value-typed sync.RWMutex so its zero value is safe.
// Callers populate idx.shards with MockShardLike instances to control
// per-shard behaviour.
func indexForCheckpointTest(t *testing.T) *Index {
	t.Helper()
	logger, _ := test.NewNullLogger()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Index{
		Config:           IndexConfig{ClassName: schema.ClassName("MyClass")},
		shardCreateLocks: esync.NewKeyRWLocker(),
		logger:           logrus.NewEntry(logger),
		closingCtx:       ctx,
	}
}

// expectPreventShutdown wires the default expectation every test needs:
// GetShard(ensureInit=false) calls preventShutdown on the loaded shard to
// obtain the release function. We return a no-op release to keep the
// receiver methods focused on their own behaviour.
func expectPreventShutdown(t *testing.T, m *MockShardLike) {
	t.Helper()
	m.On("preventShutdown").Return(func() {}, nil).Maybe()
}

// --- resolveShardNames ------------------------------------------------------

func TestIndex_ResolveShardNames_ExplicitSubsetWins(t *testing.T) {
	// When the caller passes a non-empty subset, resolveShardNames must
	// return it unchanged — never expand to "all shards", even if the
	// shard map happens to contain different names.
	idx := indexForCheckpointTest(t)
	idx.shards.Store("s1", NewMockShardLike(t))
	idx.shards.Store("s2", NewMockShardLike(t))

	got := idx.resolveShardNames([]string{"s2", "explicit-only"})

	assert.Equal(t, []string{"s2", "explicit-only"}, got)
}

func TestIndex_ResolveShardNames_EmptyReturnsAllLoaded(t *testing.T) {
	// An empty/nil shards argument means "all the shards this index
	// currently knows about". The bash script uses this contract implicitly
	// when the operator omits --shards.
	idx := indexForCheckpointTest(t)
	idx.shards.Store("s1", NewMockShardLike(t))
	idx.shards.Store("s2", NewMockShardLike(t))

	got := idx.resolveShardNames(nil)
	sort.Strings(got)
	assert.Equal(t, []string{"s1", "s2"}, got)

	gotEmpty := idx.resolveShardNames([]string{})
	sort.Strings(gotEmpty)
	assert.Equal(t, []string{"s1", "s2"}, gotEmpty)
}

func TestIndex_ResolveShardNames_NoShardsReturnsNil(t *testing.T) {
	idx := indexForCheckpointTest(t)
	assert.Nil(t, idx.resolveShardNames(nil))
}

func TestIndex_ResolveShardNames_UsesSchemaReaderWhenAvailable(t *testing.T) {
	// The schemaReader is the authoritative source for "which shards does
	// this node own". The in-memory i.shards map can lag during a
	// schema-change race; resolveShardNames must prefer the schema reader
	// so newly-created shards aren't silently skipped from a fan-out.
	idx := indexForCheckpointTest(t)
	// Populate the in-memory map with a STALE entry so we can detect
	// which source resolveShardNames actually consulted: if it returns
	// the schema list (s2/s3), it consulted schemaReader. If it returns
	// the in-memory entry (stale-s1), it incorrectly fell back.
	idx.shards.Store("stale-s1", NewMockShardLike(t))

	sr := schemaUC.NewMockSchemaReader(t)
	sr.EXPECT().LocalShards("MyClass").Return([]string{"s2", "s3"}, nil).Once()
	idx.schemaReader = sr

	got := idx.resolveShardNames(nil)
	sort.Strings(got)
	assert.Equal(t, []string{"s2", "s3"}, got)
}

func TestIndex_ResolveShardNames_FallsBackOnSchemaReaderError(t *testing.T) {
	// If the schema read fails (transient cluster issue, mid-shutdown,
	// etc.), the fallback to the in-memory shard map keeps fan-out
	// best-effort rather than producing an empty list.
	idx := indexForCheckpointTest(t)
	idx.shards.Store("fallback-s1", NewMockShardLike(t))

	sr := schemaUC.NewMockSchemaReader(t)
	sr.EXPECT().LocalShards("MyClass").Return(nil, errors.New("schema unavailable")).Once()
	idx.schemaReader = sr

	got := idx.resolveShardNames(nil)
	assert.Equal(t, []string{"fallback-s1"}, got)
}

func TestIndex_ResolveShardNames_ExplicitSubsetBypassesSchemaReader(t *testing.T) {
	// Explicit shards never consult the schema reader — the caller has
	// already decided which shards to target. The mock will fail the test
	// if LocalShards is unexpectedly called.
	idx := indexForCheckpointTest(t)
	sr := schemaUC.NewMockSchemaReader(t) // no expectations: any call fails
	idx.schemaReader = sr

	got := idx.resolveShardNames([]string{"explicit-1", "explicit-2"})
	assert.Equal(t, []string{"explicit-1", "explicit-2"}, got)
}

// --- per-shard receiver: CreateAsyncCheckpoint ------------------------------

func TestIndex_CreateAsyncCheckpoint_DelegatesToShard(t *testing.T) {
	idx := indexForCheckpointTest(t)
	now := time.Now().UTC()

	shard := NewMockShardLike(t)
	expectPreventShutdown(t, shard)
	shard.On("CreateAsyncCheckpoint", mock.Anything, int64(123), now).Return(nil).Once()
	idx.shards.Store("s1", shard)

	require.NoError(t, idx.createAsyncCheckpoint(context.Background(), "s1", 123, now))
}

func TestIndex_CreateAsyncCheckpoint_ShardNotLoadedReturnsNil(t *testing.T) {
	// GetShard returns (nil, _, nil) when the shard isn't in the map. A
	// fan-out create posts the same shard list to every node, so a shard
	// not hosted here is a benign no-op — not a failure. This matches
	// Index.deleteAsyncCheckpoint.
	idx := indexForCheckpointTest(t)
	require.NoError(t, idx.createAsyncCheckpoint(context.Background(), "missing", 123, time.Now()))
}

func TestIndex_CreateAsyncCheckpoint_ShardErrorPropagates(t *testing.T) {
	idx := indexForCheckpointTest(t)
	shard := NewMockShardLike(t)
	expectPreventShutdown(t, shard)
	staleErr := errors.New("checkpoint createdAt is not newer than the active one")
	shard.On("CreateAsyncCheckpoint", mock.Anything, mock.Anything, mock.Anything).Return(staleErr).Once()
	idx.shards.Store("s1", shard)

	err := idx.createAsyncCheckpoint(context.Background(), "s1", 123, time.Now())
	require.Error(t, err)
	assert.Equal(t, staleErr, err, "shard-level error must surface untransformed so the REST status mapper can classify it")
}

// --- per-shard receiver: DeleteAsyncCheckpoint ------------------------------

func TestIndex_DeleteAsyncCheckpoint_DelegatesToShard(t *testing.T) {
	idx := indexForCheckpointTest(t)
	shard := NewMockShardLike(t)
	expectPreventShutdown(t, shard)
	shard.On("DeleteAsyncCheckpoint", mock.Anything).Return(nil).Once()
	idx.shards.Store("s1", shard)

	require.NoError(t, idx.deleteAsyncCheckpoint(context.Background(), "s1"))
}

func TestIndex_DeleteAsyncCheckpoint_ShardNotLoadedReturnsNil(t *testing.T) {
	// Unlike Create, Delete is idempotent across replicas — a missing shard
	// here is a no-op so a fan-out delete to a node that doesn't host the
	// shard is harmless. This matches the design note in
	// Index.deleteAsyncCheckpoint.
	idx := indexForCheckpointTest(t)
	require.NoError(t, idx.deleteAsyncCheckpoint(context.Background(), "missing"))
}

// --- GetAsyncCheckpointShardStatus -----------------------------------------

func TestIndex_GetAsyncCheckpointShardStatus_OmitsUnloadedShards(t *testing.T) {
	// The aggregator at GetAsyncCheckpointStatus relies on the absence of
	// an entry to mean "this node doesn't host that shard". Loaded shards
	// must produce an entry (active OR inactive); unloaded shards must
	// not.
	idx := indexForCheckpointTest(t)

	active := NewMockShardLike(t)
	expectPreventShutdown(t, active)
	activeRoot := hashtree.Digest{1, 2}
	activeCreatedAt := time.UnixMilli(1_000_000).UTC()
	active.On("AsyncCheckpointRoot", mock.Anything).Return(activeRoot, int64(500), activeCreatedAt, true).Once()
	idx.shards.Store("active", active)

	inactive := NewMockShardLike(t)
	expectPreventShutdown(t, inactive)
	inactive.On("AsyncCheckpointRoot", mock.Anything).Return(hashtree.Digest{}, int64(0), time.Time{}, false).Once()
	idx.shards.Store("inactive", inactive)

	got, err := idx.getAsyncCheckpointShardStatus(context.Background(),
		[]string{"active", "inactive", "missing"})
	require.NoError(t, err)

	// Both loaded shards present, "missing" omitted.
	require.Len(t, got, 2)
	require.Contains(t, got, "active")
	require.Contains(t, got, "inactive")
	assert.NotContains(t, got, "missing")

	assert.Equal(t, int64(500), got["active"].CutoffMs)
	assert.Equal(t, activeCreatedAt, got["active"].CreatedAt)
	assert.Equal(t, activeRoot, got["active"].Root)

	// Inactive entry is zero-valued but present — the caller distinguishes
	// "no active checkpoint" from "not loaded here" by entry presence.
	assert.Equal(t, int64(0), got["inactive"].CutoffMs)
	assert.True(t, got["inactive"].CreatedAt.IsZero())
}

func TestIndex_GetAsyncCheckpointShardStatus_EmptyRequestReturnsEmptyMap(t *testing.T) {
	idx := indexForCheckpointTest(t)
	got, err := idx.getAsyncCheckpointShardStatus(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestIndex_GetAsyncCheckpointShardStatus_CancelledContextReturnsError(t *testing.T) {
	idx := indexForCheckpointTest(t)
	idx.shards.Store("s1", NewMockShardLike(t))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := idx.getAsyncCheckpointShardStatus(ctx, []string{"s1"})
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, got)
}

// --- receiver-side multi-shard loops ----------------------------------------

func TestIndex_CreateAsyncCheckpointShards_SkipsNotHostedShards(t *testing.T) {
	// A fan-out create posts the same shard list to every node. Shards not
	// hosted here must be skipped silently — not fail the whole request.
	idx := indexForCheckpointTest(t)
	now := time.Now().UTC()

	hosted := NewMockShardLike(t)
	expectPreventShutdown(t, hosted)
	hosted.On("CreateAsyncCheckpoint", mock.Anything, int64(123), now).Return(nil).Once()
	idx.shards.Store("hosted", hosted)

	// "not-hosted" is absent from idx.shards.
	err := idx.createAsyncCheckpointShards(context.Background(),
		[]string{"hosted", "not-hosted"}, 123, now)
	require.NoError(t, err)
}

func TestIndex_CreateAsyncCheckpointShards_BestEffortAggregatesErrors(t *testing.T) {
	// A per-shard failure must not short-circuit the loop: every hostable
	// shard is still attempted, and the stale sentinel survives errors.Join
	// so the REST/gRPC mappers can still classify the aggregated result.
	idx := indexForCheckpointTest(t)
	now := time.Now().UTC()

	stale := NewMockShardLike(t)
	expectPreventShutdown(t, stale)
	stale.On("CreateAsyncCheckpoint", mock.Anything, mock.Anything, mock.Anything).
		Return(replica.ErrAsyncCheckpointStale).Once()
	idx.shards.Store("stale", stale)

	ok := NewMockShardLike(t)
	expectPreventShutdown(t, ok)
	ok.On("CreateAsyncCheckpoint", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	idx.shards.Store("ok", ok)

	err := idx.createAsyncCheckpointShards(context.Background(),
		[]string{"stale", "ok"}, 123, now)
	require.Error(t, err)
	assert.ErrorIs(t, err, replica.ErrAsyncCheckpointStale)
	// Both .Once() expectations are verified by NewMockShardLike(t)'s
	// cleanup — proving "ok" was attempted despite "stale" failing first.
}

func TestIndex_CreateAsyncCheckpointShards_CancelledContextReturnsError(t *testing.T) {
	idx := indexForCheckpointTest(t)
	idx.shards.Store("s1", NewMockShardLike(t))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := idx.createAsyncCheckpointShards(ctx, []string{"s1"}, 123, time.Now())
	require.ErrorIs(t, err, context.Canceled)
}

func TestIndex_DeleteAsyncCheckpointShards_SkipsNotHostedShards(t *testing.T) {
	idx := indexForCheckpointTest(t)

	hosted := NewMockShardLike(t)
	expectPreventShutdown(t, hosted)
	hosted.On("DeleteAsyncCheckpoint", mock.Anything).Return(nil).Once()
	idx.shards.Store("hosted", hosted)

	err := idx.deleteAsyncCheckpointShards(context.Background(),
		[]string{"hosted", "not-hosted"})
	require.NoError(t, err)
}

// --- initiator-side fan-out (plural) ----------------------------------------

// stubBroadcaster records every call so tests can assert that the same
// createdAt was propagated to every shard AND to the broadcast helper, that
// per-shard local failures don't abort the loop, and that the function
// returns nil even when local+remote failures occurred (the best-effort
// design contract). It's a fakery rather than a Mock so the tests stay
// behavioural — we care about "the broadcaster saw these args" more than
// "expect-then-verify".
type stubBroadcaster struct {
	localNode string

	gotCreateShards    []string
	gotCreateCutoff    int64
	gotCreateCreatedAt time.Time
	gotDeleteShards    []string
	gotStatusShards    []string

	createSuccesses, createFailures int
	deleteSuccesses, deleteFailures int
	statusSuccesses, statusFailures int

	statusResult map[string][]replica.AsyncCheckpointNodeStatus
}

func (s *stubBroadcaster) LocalNodeName() string { return s.localNode }

func (s *stubBroadcaster) BroadcastCreateAsyncCheckpoint(_ context.Context, shardNames []string, cutoffMs int64, createdAt time.Time) (int, int) {
	s.gotCreateShards = append([]string(nil), shardNames...)
	s.gotCreateCutoff = cutoffMs
	s.gotCreateCreatedAt = createdAt
	return s.createSuccesses, s.createFailures
}

func (s *stubBroadcaster) BroadcastDeleteAsyncCheckpoint(_ context.Context, shardNames []string) (int, int) {
	s.gotDeleteShards = append([]string(nil), shardNames...)
	return s.deleteSuccesses, s.deleteFailures
}

func (s *stubBroadcaster) BroadcastGetAsyncCheckpointStatus(_ context.Context, shardNames []string) (map[string][]replica.AsyncCheckpointNodeStatus, int, int) {
	s.gotStatusShards = append([]string(nil), shardNames...)
	if s.statusResult == nil {
		return map[string][]replica.AsyncCheckpointNodeStatus{}, s.statusSuccesses, s.statusFailures
	}
	// Return a defensive copy so the test's assertion source-of-truth
	// doesn't get mutated by the production code under test.
	out := make(map[string][]replica.AsyncCheckpointNodeStatus, len(s.statusResult))
	for k, v := range s.statusResult {
		out[k] = append([]replica.AsyncCheckpointNodeStatus(nil), v...)
	}
	return out, s.statusSuccesses, s.statusFailures
}

func TestIndex_CreateAsyncCheckpoints_PropagatesCreatedAtAndFansOut(t *testing.T) {
	// Two local shards both succeed; the broadcast helper is called with
	// the same target list AND the exact same createdAt that the per-shard
	// calls received. This is the convergence-determining invariant: every
	// replica (local and remote) sees the same createdAt for this call.
	idx := indexForCheckpointTest(t)
	now := time.Now().UTC()

	// Capture the createdAt that the local shards observe so we can prove
	// it equals the value passed to BroadcastCreateAsyncCheckpoint.
	var (
		mu              sync.Mutex
		observedCreated []time.Time
	)
	capture := func(_, raw mock.Arguments) {
		mu.Lock()
		defer mu.Unlock()
		observedCreated = append(observedCreated, raw.Get(2).(time.Time))
	}
	_ = capture // keep linter quiet; the .Run closure does the recording

	s1 := NewMockShardLike(t)
	expectPreventShutdown(t, s1)
	s1.On("CreateAsyncCheckpoint", mock.Anything, int64(123), mock.AnythingOfType("time.Time")).
		Run(func(args mock.Arguments) {
			mu.Lock()
			observedCreated = append(observedCreated, args.Get(2).(time.Time))
			mu.Unlock()
		}).Return(nil).Once()
	idx.shards.Store("s1", s1)

	s2 := NewMockShardLike(t)
	expectPreventShutdown(t, s2)
	s2.On("CreateAsyncCheckpoint", mock.Anything, int64(123), mock.AnythingOfType("time.Time")).
		Run(func(args mock.Arguments) {
			mu.Lock()
			observedCreated = append(observedCreated, args.Get(2).(time.Time))
			mu.Unlock()
		}).Return(nil).Once()
	idx.shards.Store("s2", s2)

	br := &stubBroadcaster{localNode: "node-A"}
	require.NoError(t, idx.createAsyncCheckpoints(context.Background(), 123, []string{"s1", "s2"}, br))

	// 1. Every per-shard call saw the same createdAt.
	require.Len(t, observedCreated, 2)
	assert.Equal(t, observedCreated[0], observedCreated[1],
		"both local shards must see the same createdAt (convergence tie-breaker)")
	// 2. That createdAt is also what flowed into the broadcast helper.
	assert.Equal(t, observedCreated[0], br.gotCreateCreatedAt,
		"broadcast helper must receive the same createdAt as the local shards")
	// 3. The createdAt must be close to "now" (set inside the call); we
	//    don't pin equality with `now` because the function calls
	//    time.Now() itself.
	assert.WithinDuration(t, now, br.gotCreateCreatedAt, 5*time.Second)
	// 4. The broadcast helper got the same shard set + cutoff.
	assert.Equal(t, []string{"s1", "s2"}, br.gotCreateShards)
	assert.Equal(t, int64(123), br.gotCreateCutoff)
}

func TestIndex_CreateAsyncCheckpoints_LocalFailureDoesNotAbortLoopOrBroadcast(t *testing.T) {
	// One local shard fails (stale createdAt — the receiver-side
	// tie-breaker) but the other still gets called AND the broadcast
	// still fires. The function returns nil because the design treats
	// fan-out as best-effort: a single failure doesn't sink the call.
	idx := indexForCheckpointTest(t)

	s1 := NewMockShardLike(t)
	expectPreventShutdown(t, s1)
	s1.On("CreateAsyncCheckpoint", mock.Anything, mock.Anything, mock.Anything).
		Return(replica.ErrAsyncCheckpointStale).Once()
	idx.shards.Store("s1", s1)

	s2 := NewMockShardLike(t)
	expectPreventShutdown(t, s2)
	s2Called := false
	s2.On("CreateAsyncCheckpoint", mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ mock.Arguments) { s2Called = true }).
		Return(nil).Once()
	idx.shards.Store("s2", s2)

	br := &stubBroadcaster{localNode: "node-A"}
	err := idx.createAsyncCheckpoints(context.Background(), 123, []string{"s1", "s2"}, br)

	require.NoError(t, err, "local failure must not propagate; fan-out is best-effort")
	assert.True(t, s2Called, "the loop must not short-circuit on the first local failure")
	assert.Equal(t, []string{"s1", "s2"}, br.gotCreateShards,
		"broadcast must still fire even when local shards failed")
}

func TestIndex_CreateAsyncCheckpoints_ReturnsNilDespiteRemoteFailures(t *testing.T) {
	// Remote failures from the broadcast helper are counted but never
	// propagated as a return error. Operators rely on the Info log line
	// + per-node Warn lines for visibility.
	idx := indexForCheckpointTest(t)
	s1 := NewMockShardLike(t)
	expectPreventShutdown(t, s1)
	s1.On("CreateAsyncCheckpoint", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	idx.shards.Store("s1", s1)

	br := &stubBroadcaster{localNode: "node-A", createSuccesses: 0, createFailures: 3}
	err := idx.createAsyncCheckpoints(context.Background(), 123, []string{"s1"}, br)
	require.NoError(t, err)
}

func TestIndex_CreateAsyncCheckpoints_RejectsNonPositiveCutoff(t *testing.T) {
	idx := indexForCheckpointTest(t)
	br := &stubBroadcaster{localNode: "node-A"}
	err := idx.createAsyncCheckpoints(context.Background(), 0, []string{"s1"}, br)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be > 0")
	// The broadcaster must NOT have been called.
	assert.Empty(t, br.gotCreateShards, "guard must fire before any fan-out")
}

func TestIndex_DeleteAsyncCheckpoints_FansOutAfterLocalLoop(t *testing.T) {
	idx := indexForCheckpointTest(t)

	s1 := NewMockShardLike(t)
	expectPreventShutdown(t, s1)
	s1.On("DeleteAsyncCheckpoint", mock.Anything).Return(nil).Once()
	idx.shards.Store("s1", s1)

	br := &stubBroadcaster{localNode: "node-A"}
	require.NoError(t, idx.deleteAsyncCheckpoints(context.Background(), []string{"s1"}, br))
	assert.Equal(t, []string{"s1"}, br.gotDeleteShards)
}

func TestIndex_GetAsyncCheckpointStatus_AggregatesLocalAndRemote(t *testing.T) {
	// The aggregator merges the local node's per-shard status (via
	// GetAsyncCheckpointShardStatus) with the broadcast's per-node
	// statuses. Both sets share one shard ("s1") and the function
	// must return an entry per (shard, node) pair without duplicating
	// or dropping anything.
	idx := indexForCheckpointTest(t)

	localCreatedAt := time.UnixMilli(1_000_000).UTC()
	localRoot := hashtree.Digest{0xAAA, 0xBBB}

	local := NewMockShardLike(t)
	expectPreventShutdown(t, local)
	local.On("AsyncCheckpointRoot", mock.Anything).
		Return(localRoot, int64(500), localCreatedAt, true).Once()
	idx.shards.Store("s1", local)

	remoteCreatedAt := time.UnixMilli(2_000_000).UTC()
	remoteRoot := hashtree.Digest{0xCCC, 0xDDD}

	br := &stubBroadcaster{
		localNode: "node-A",
		statusResult: map[string][]replica.AsyncCheckpointNodeStatus{
			"s1": {{Node: "node-B", CutoffMs: 500, CreatedAt: remoteCreatedAt, Root: remoteRoot}},
		},
	}

	got, err := idx.getAsyncCheckpointStatus(context.Background(), []string{"s1"}, br)
	require.NoError(t, err)
	require.Contains(t, got, "s1")
	require.Len(t, got["s1"], 2, "must include both the local and the remote entries for s1")

	// One entry must be tagged with the local node name; the other must
	// match what the broadcaster returned for node-B.
	var sawLocal, sawRemote bool
	for _, entry := range got["s1"] {
		switch entry.Node {
		case "node-A":
			sawLocal = true
			assert.Equal(t, localCreatedAt.UnixMilli(), entry.CreatedAt.UnixMilli())
			assert.Equal(t, localRoot, entry.Root)
		case "node-B":
			sawRemote = true
			assert.Equal(t, remoteCreatedAt.UnixMilli(), entry.CreatedAt.UnixMilli())
			assert.Equal(t, remoteRoot, entry.Root)
		}
	}
	assert.True(t, sawLocal, "local entry tagged with LocalNodeName must be present")
	assert.True(t, sawRemote, "remote entry from the broadcaster must be present")
}
