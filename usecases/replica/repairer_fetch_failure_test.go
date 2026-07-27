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

package replica_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
)

const (
	metricRepairCount   = "weaviate_replication_read_repair_count"
	metricRepairFailure = "weaviate_replication_read_repair_failure"
)

// repairProbe is a Finder whose read repair metrics land in a registry the test
// owns, so an assertion can read the counters rather than infer them.
type repairProbe struct {
	*replica.Finder
	reg *prometheus.Registry
	f   *fakeFactory
}

func newRepairProbe(t *testing.T, f *fakeFactory, thisNode, deletionStrategy string) *repairProbe {
	t.Helper()

	nodeResolver := cluster.NewMockNodeResolver(t)
	for _, n := range f.Nodes {
		nodeResolver.EXPECT().NodeHostname(n).Return(n, true).Maybe()
	}

	reg := prometheus.NewPedanticRegistry()
	metrics, err := replica.NewMetrics(&monitoring.PrometheusMetrics{Registerer: reg})
	require.NoError(t, err)

	return &repairProbe{
		Finder: replica.NewFinder(f.CLS, f.newRouter(thisNode), nodeResolver, thisNode,
			f.RClient, metrics, f.log, func() string { return deletionStrategy }),
		reg: reg,
		f:   f,
	}
}

func (p *repairProbe) counter(t *testing.T, name string) float64 {
	t.Helper()
	families, err := p.reg.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		if fam.GetName() == name {
			require.Len(t, fam.GetMetric(), 1)
			return fam.GetMetric()[0].GetCounter().GetValue()
		}
	}
	return 0
}

// repairBatchLogs returns the messages the batch repair path logged. The log
// write happens before CheckConsistency returns, so no polling is needed.
func (p *repairProbe) repairBatchLogs(t *testing.T) []string {
	t.Helper()
	var msgs []string
	for _, e := range p.f.hook.AllEntries() {
		if op, _ := e.Data["op"].(string); op == "repair_batch" {
			msgs = append(msgs, e.Message)
		}
	}
	return msgs
}

// writeRecorder accepts any OverwriteObjects call and remembers the payloads,
// so a test can assert on what was written, including that nothing was.
type writeRecorder struct {
	mu       sync.Mutex
	byNode   map[string][]*objects.VObject
	numCalls int
}

func recordWrites(f *fakeFactory, nodes ...string) *writeRecorder {
	w := &writeRecorder{byNode: map[string][]*objects.VObject{}}
	for _, node := range nodes {
		f.RClient.EXPECT().OverwriteObjects(anyVal, node, anyVal, anyVal, anyVal).
			Return([]types.RepairResponse{}, nil).
			Maybe().
			RunFn = func(a mock.Arguments) {
			w.mu.Lock()
			defer w.mu.Unlock()
			w.numCalls++
			w.byNode[a[1].(string)] = append(w.byNode[a[1].(string)], a[4].([]*objects.VObject)...)
		}
	}
	return w
}

func (w *writeRecorder) calls() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.numCalls
}

func (w *writeRecorder) written(node string) []*objects.VObject {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.byNode[node]
}

// TestRepairBatchFetchFromWinnerFails pins that unusable winner fetches are
// logged, counted, and leave the object inconsistent.
func TestRepairBatchFetchFromWinnerFails(t *testing.T) {
	var (
		cls    = "C1"
		shard  = "S1"
		nodes  = []string{"A", "B"}
		id     = strfmt.UUID("11111111-1111-1111-1111-111111111111")
		otherI = strfmt.UUID("22222222-2222-2222-2222-222222222222")
	)

	tests := []struct {
		name    string
		fetched []replica.Replica
		err     error
		wantLog string
	}{
		{
			name:    "transport error",
			err:     errAny,
			wantLog: "read 1 object(s) from B",
		},
		{
			name:    "response carries another uuid",
			fetched: []replica.Replica{repl(otherI, 2, false)},
			wantLog: `malformed full read response: object 0 is "22222222-2222-2222-2222-222222222222"`,
		},
		{
			name:    "response is shorter than the request",
			fetched: []replica.Replica{},
			wantLog: "malformed full read response: length expected 1 got 0",
		},
		{
			name:    "winner moved on after voting",
			fetched: []replica.Replica{repl(id, 3, false)},
			wantLog: "no longer holds the agreed version",
		},
		{
			name:    "winner was deleted mid flight",
			fetched: []replica.Replica{{ID: id, LastUpdateTimeUnixMilli: 2}},
			wantLog: "no longer holds the agreed version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				ctx   = context.Background()
				f     = newFakeFactory(t, cls, shard, nodes, false)
				probe = newRepairProbe(t, f, "A", models.ReplicationConfigDeletionStrategyNoAutomatedResolution)
				xs    = []*storobj.Object{objectEx(id, 1, shard, "A")}
			)

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, []strfmt.UUID{id}, anyVal).
				Return([]types.RepairResponse{{ID: id.String(), UpdateTime: 2}}, nil)
			f.RClient.EXPECT().FetchObjects(anyVal, nodes[1], cls, shard, []strfmt.UUID{id}).
				Return(tt.fetched, tt.err).
				Once()
			writes := recordWrites(f, nodes...)

			require.NoError(t, probe.CheckConsistency(ctx, types.ConsistencyLevelAll, xs))

			require.Zero(t, writes.calls(), "content was unusable, nothing may be written")
			require.Equal(t, []*storobj.Object{objectEx(id, 1, shard, "A")}, xs)
			require.False(t, xs[0].IsConsistent)
			require.Equal(t, float64(1), probe.counter(t, metricRepairCount))
			require.Equal(t, float64(1), probe.counter(t, metricRepairFailure))
			require.Contains(t, strings.Join(probe.repairBatchLogs(t), "\n"), tt.wantLog)
		})
	}
}

// fullReadIDs returns n deterministic uuids for driving FullReads.
func fullReadIDs(n int) []strfmt.UUID {
	ids := make([]strfmt.UUID, n)
	for i := range ids {
		ids[i] = strfmt.UUID(uuid.NewSHA1(uuid.Nil, []byte(strconv.Itoa(i))).String())
	}
	return ids
}

// TestFullReadsChunksIDs pins that FullReads splits large id lists into
// bounded chunks instead of one unbounded request.
func TestFullReadsChunksIDs(t *testing.T) {
	const chunkSize = replica.MaxFullReadIDsPerRequest
	tests := []struct {
		total      int
		wantChunks []int
	}{
		{total: 1, wantChunks: []int{1}},
		{total: chunkSize, wantChunks: []int{chunkSize}},
		{total: chunkSize + 1, wantChunks: []int{chunkSize, 1}},
		{total: 3*chunkSize + 8, wantChunks: []int{chunkSize, chunkSize, chunkSize, 8}},
	}

	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.total), func(t *testing.T) {
			ids := fullReadIDs(tt.total)

			rc := replica.NewMockRClient(t)
			var (
				mu  sync.Mutex
				got []int
			)
			rc.EXPECT().FetchObjects(anyVal, "B", "C1", "S1", anyVal).
				RunAndReturn(func(_ context.Context, _, _, _ string, chunk []strfmt.UUID) ([]replica.Replica, error) {
					mu.Lock()
					got = append(got, len(chunk))
					mu.Unlock()
					rs := make([]replica.Replica, len(chunk))
					for i, id := range chunk {
						rs[i] = repl(id, 1, false)
					}
					return rs, nil
				})

			logger, _ := test.NewNullLogger()
			rs, err := replica.NewFinderClient(rc, logger).FullReads(context.Background(), "B", "C1", "S1", ids)
			require.NoError(t, err)
			// chunks are fetched concurrently, so only the multiset is deterministic
			require.ElementsMatch(t, tt.wantChunks, got)

			require.Len(t, rs, tt.total)
			for i := range rs {
				require.Equal(t, ids[i], rs[i].ID, "responses must stay in request order across chunks")
			}
		})
	}
}

// TestFullReadsFetchesChunksConcurrently pins the bounded fan-out: the barrier
// releases only once the full in-flight cap of requests is simultaneously in
// flight, so a serial implementation times out here and an unbounded one
// overshoots the peak.
func TestFullReadsFetchesChunksConcurrently(t *testing.T) {
	const inFlightCap = replica.MaxConcurrentFullReadRequests
	ids := fullReadIDs((inFlightCap + 4) * replica.MaxFullReadIDsPerRequest)

	var (
		inFlight atomic.Int64
		peak     atomic.Int64
		arrivals atomic.Int64
		release  = make(chan struct{})
	)

	rc := replica.NewMockRClient(t)
	rc.EXPECT().FetchObjects(anyVal, "B", "C1", "S1", anyVal).
		RunAndReturn(func(_ context.Context, _, _, _ string, chunk []strfmt.UUID) ([]replica.Replica, error) {
			n := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			if arrivals.Add(1) == inFlightCap {
				close(release)
			}
			select {
			case <-release:
			case <-time.After(10 * time.Second):
				return nil, fmt.Errorf("never saw %d requests in flight: chunks are being fetched serially", inFlightCap)
			}
			rs := make([]replica.Replica, len(chunk))
			for i, id := range chunk {
				rs[i] = repl(id, 1, false)
			}
			return rs, nil
		})

	logger, _ := test.NewNullLogger()
	rs, err := replica.NewFinderClient(rc, logger).FullReads(context.Background(), "B", "C1", "S1", ids)
	require.NoError(t, err)
	require.EqualValues(t, inFlightCap, peak.Load(), "in-flight requests must reach and never exceed the cap")
	require.Len(t, rs, len(ids))
	for i := range rs {
		require.Equal(t, ids[i], rs[i].ID, "responses must stay in request order across concurrent chunks")
	}
}

// TestFullReadsFailedChunkFailsTheRead pins that one failed chunk fails the
// whole read instead of returning a partial result the repairer would trust.
func TestFullReadsFailedChunkFailsTheRead(t *testing.T) {
	ids := fullReadIDs(2 * replica.MaxFullReadIDsPerRequest)

	rc := replica.NewMockRClient(t)
	rc.EXPECT().FetchObjects(anyVal, "B", "C1", "S1", anyVal).
		RunAndReturn(func(_ context.Context, _, _, _ string, chunk []strfmt.UUID) ([]replica.Replica, error) {
			if chunk[0] == ids[replica.MaxFullReadIDsPerRequest] {
				return nil, errAny
			}
			rs := make([]replica.Replica, len(chunk))
			for i, id := range chunk {
				rs[i] = repl(id, 1, false)
			}
			return rs, nil
		})

	logger, _ := test.NewNullLogger()
	rs, err := replica.NewFinderClient(rc, logger).FullReads(context.Background(), "B", "C1", "S1", ids)
	require.ErrorIs(t, err, errAny)
	require.Nil(t, rs)
}

// TestFullReadsRejectsMisalignedLaterChunk pins that the ID alignment check
// reports absolute indices for chunks past the first.
func TestFullReadsRejectsMisalignedLaterChunk(t *testing.T) {
	ids := fullReadIDs(replica.MaxFullReadIDsPerRequest + 1)
	other := strfmt.UUID("99999999-9999-4999-8999-999999999999")

	rc := replica.NewMockRClient(t)
	rc.EXPECT().FetchObjects(anyVal, "B", "C1", "S1", anyVal).
		RunAndReturn(func(_ context.Context, _, _, _ string, chunk []strfmt.UUID) ([]replica.Replica, error) {
			rs := make([]replica.Replica, len(chunk))
			for i, id := range chunk {
				rs[i] = repl(id, 1, false)
			}
			if len(chunk) == 1 { // the second chunk, holding the one trailing id
				rs[0] = repl(other, 1, false)
			}
			return rs, nil
		})

	logger, _ := test.NewNullLogger()
	rs, err := replica.NewFinderClient(rc, logger).FullReads(context.Background(), "B", "C1", "S1", ids)
	require.ErrorContains(t, err,
		fmt.Sprintf("object %d is %q", replica.MaxFullReadIDsPerRequest, other))
	require.Nil(t, rs)
}

// TestRepairBatchThreeDistinctWinners pins that per-replica fetch
// partitioning maps each response back onto the correct object slot.
func TestRepairBatchThreeDistinctWinners(t *testing.T) {
	var (
		ctx   = context.Background()
		cls   = "C1"
		shard = "S1"
		nodes = []string{"A", "B", "C"}
		ids   = []strfmt.UUID{
			"11111111-1111-1111-1111-111111111111",
			"22222222-2222-2222-2222-222222222222",
			"33333333-3333-3333-3333-333333333333",
		}
		f     = newFakeFactory(t, cls, shard, nodes, false)
		probe = newRepairProbe(t, f, "A", models.ReplicationConfigDeletionStrategyNoAutomatedResolution)
	)

	// one winner per replica: A owns ids[0], B owns ids[1], C owns ids[2]
	xs := []*storobj.Object{
		objectEx(ids[0], 3, shard, "A"),
		objectEx(ids[1], 1, shard, "A"),
		objectEx(ids[2], 1, shard, "A"),
	}
	digest := func(times ...int64) []types.RepairResponse {
		rs := make([]types.RepairResponse, len(times))
		for i, ts := range times {
			rs[i] = types.RepairResponse{ID: ids[i].String(), UpdateTime: ts}
		}
		return rs
	}
	f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, ids, anyVal).
		Return(digest(1, 3, 1), nil)
	f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, ids, anyVal).
		Return(digest(1, 1, 3), nil)

	for i, node := range nodes {
		f.RClient.EXPECT().FetchObjects(anyVal, node, cls, shard, []strfmt.UUID{ids[i]}).
			Return([]replica.Replica{repl(ids[i], 3, false)}, nil).
			Once()
	}
	writes := recordWrites(f, nodes...)

	require.NoError(t, probe.CheckConsistency(ctx, types.ConsistencyLevelAll, xs))
	require.Zero(t, probe.counter(t, metricRepairFailure))

	// each replica gets the two objects it lacks, filed under the correct id
	for _, node := range nodes {
		written := writes.written(node)
		require.Len(t, written, 2, "node %s", node)
		for _, up := range written {
			require.NotNil(t, up.LatestObject, "node %s object %s", node, up.ID)
			require.Equal(t, up.ID, up.LatestObject.ID, "content filed under the wrong uuid")
			require.Equal(t, int64(3), up.LastUpdateTimeUnixMilli)
			require.Equal(t, int64(1), up.StaleUpdateTime)
		}
	}
	for i := range xs {
		require.True(t, xs[i].IsConsistent, "object %d", i)
	}
}

// TestRepairBatchCarriesVectorShapes pins that fetched winner content,
// including multi-vectors, reaches the stale replica intact.
func TestRepairBatchCarriesVectorShapes(t *testing.T) {
	var (
		cls   = "C1"
		shard = "S1"
		nodes = []string{"A", "B"}
		id    = strfmt.UUID("11111111-1111-1111-1111-111111111111")
	)

	tests := []struct {
		name             string
		vectors          map[string][]float32
		multiVectors     map[string][][]float32
		wantVectors      map[string][]float32
		wantMultiVectors map[string][][]float32
	}{
		{
			name:             "multi vectors",
			multiVectors:     map[string][][]float32{"colbert": {{1, 2}, {3, 4}}},
			wantMultiVectors: map[string][][]float32{"colbert": {{1, 2}, {3, 4}}},
		},
		{
			name:             "named and multi vectors together",
			vectors:          map[string][]float32{"title": {5, 6}},
			multiVectors:     map[string][][]float32{"colbert": {{1, 2}}},
			wantVectors:      map[string][]float32{"title": {5, 6}},
			wantMultiVectors: map[string][][]float32{"colbert": {{1, 2}}},
		},
		{
			// the copy is guarded on != nil rather than on length, so an empty
			// map survives as an empty map instead of collapsing to nil
			name:             "empty but non nil maps",
			vectors:          map[string][]float32{},
			multiVectors:     map[string][][]float32{},
			wantVectors:      map[string][]float32{},
			wantMultiVectors: map[string][][]float32{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				ctx   = context.Background()
				f     = newFakeFactory(t, cls, shard, nodes, false)
				probe = newRepairProbe(t, f, "A", models.ReplicationConfigDeletionStrategyNoAutomatedResolution)
				xs    = []*storobj.Object{objectEx(id, 1, shard, "A")}
			)

			winner := &storobj.Object{
				Object:       models.Object{ID: id, LastUpdateTimeUnix: 2},
				Vector:       []float32{7, 8},
				Vectors:      tt.vectors,
				MultiVectors: tt.multiVectors,
			}

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, []strfmt.UUID{id}, anyVal).
				Return([]types.RepairResponse{{ID: id.String(), UpdateTime: 2}}, nil)
			f.RClient.EXPECT().FetchObjects(anyVal, nodes[1], cls, shard, []strfmt.UUID{id}).
				Return([]replica.Replica{{ID: id, Object: winner}}, nil).
				Once()
			writes := recordWrites(f, nodes...)

			require.NoError(t, probe.CheckConsistency(ctx, types.ConsistencyLevelAll, xs))
			require.Zero(t, probe.counter(t, metricRepairFailure))

			written := writes.written(nodes[0])
			require.Len(t, written, 1)
			require.Equal(t, []float32{7, 8}, written[0].Vector)
			require.Equal(t, tt.wantVectors, written[0].Vectors)
			require.Equal(t, tt.wantMultiVectors, written[0].MultiVectors)
			require.True(t, xs[0].IsConsistent)
		})
	}
}

// TestRepairBatchSkipsContentTheStrategyDiscards pins that
// NoAutomatedResolution skips the fetch and reports the object inconsistent.
func TestRepairBatchSkipsContentTheStrategyDiscards(t *testing.T) {
	var (
		ctx   = context.Background()
		cls   = "C1"
		shard = "S1"
		nodes = []string{"A", "B"}
		id    = strfmt.UUID("11111111-1111-1111-1111-111111111111")
		f     = newFakeFactory(t, cls, shard, nodes, false)
		probe = newRepairProbe(t, f, "A", models.ReplicationConfigDeletionStrategyNoAutomatedResolution)
		xs    = []*storobj.Object{objectEx(id, 5, shard, "A")}
	)

	// the caller's copy is the newest, but B holds an older tombstone
	f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, []strfmt.UUID{id}, anyVal).
		Return([]types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: true}}, nil)
	var fetches atomic.Int64
	f.RClient.EXPECT().FetchObjects(anyVal, anyVal, anyVal, anyVal, anyVal).
		Return(nil, nil).
		Maybe().
		RunFn = func(mock.Arguments) { fetches.Add(1) }
	writes := recordWrites(f, nodes...)

	require.NoError(t, probe.CheckConsistency(ctx, types.ConsistencyLevelAll, xs))

	require.Zero(t, fetches.Load(), "no write is possible, so no content may be fetched")
	require.Zero(t, writes.calls())
	require.False(t, xs[0].IsConsistent, "the replicas still disagree")
	require.Zero(t, probe.counter(t, metricRepairFailure), "declining to resolve is not a failure")
}
