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
	"fmt"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compact"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/utils"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/verbosity"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestReadReplicationDetails pins that one read without a retry covers every
// shard of a collection, and how a collection the schema does not hold is
// reported.
func TestReadReplicationDetails(t *testing.T) {
	const className = "Repl"

	// only a partitioned state may hold no shard at all, so every case uses one
	stateWith := func(physicals ...sharding.Physical) *sharding.State {
		m := make(map[string]sharding.Physical, len(physicals))
		for _, p := range physicals {
			m[p.Name] = p
		}
		return &sharding.State{Physical: m, ReplicationFactor: 3, PartitioningEnabled: true}
	}

	tests := []struct {
		name                  string
		state                 *sharding.State
		wantReplicas          map[string]int64
		wantReplicationFactor int64
	}{
		{
			name:                  "one shard",
			state:                 stateWith(sharding.Physical{Name: "s1", BelongsToNodes: []string{"node1", "node2"}}),
			wantReplicas:          map[string]int64{"s1": 2},
			wantReplicationFactor: 3,
		},
		{
			name: "many shards",
			state: stateWith(
				sharding.Physical{Name: "s1", BelongsToNodes: []string{"node1"}},
				sharding.Physical{Name: "s2", BelongsToNodes: []string{"node1", "node2"}},
				sharding.Physical{Name: "s3", BelongsToNodes: []string{"node1", "node2", "node3"}},
			),
			wantReplicas:          map[string]int64{"s1": 1, "s2": 2, "s3": 3},
			wantReplicationFactor: 3,
		},
		{
			// a multi-tenant collection without tenants has no shard to report
			name:                  "no shards",
			state:                 stateWith(),
			wantReplicas:          map[string]int64{},
			wantReplicationFactor: 3,
		},
		{
			// a nil state stands for a collection the schema does not hold
			name:                  "collection not in the schema",
			state:                 nil,
			wantReplicas:          nil,
			wantReplicationFactor: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			reader := &retryingSchemaReader{class: &models.Class{Class: className}, state: tt.state}
			idx := newTestIndex(t, logger, className, reader, nil)

			replicationFactor, replicas := idx.readReplicationDetails()

			assert.Equal(t, tt.wantReplicationFactor, replicationFactor, "replication factor")
			assert.Equal(t, tt.wantReplicas, replicas, "number of replicas per shard")
			// a deleted collection does not come back by waiting, and the retry
			// would sleep while a drop waits to take the index apart
			assert.Equal(t, 1, reader.reads, "number of schema reads")
		})
	}
}

// TestLocalNodeShardStats pins that a verbose scan leaves indexLock free while
// holding the scanned index against a drop, that a collection being deleted is
// the only one a scan may leave out without returning an error, and that no
// collection costs more than one schema read however many shards it holds.
func TestLocalNodeShardStats(t *testing.T) {
	const className = "Slow"

	tests := []struct {
		name              string
		class             string
		shards            []string
		shardsNotInSchema []string
		shardOnSchemaRead string
		shard             string
		extraIndices      int
		withNilIndex      bool
		closeIndex        bool
		closedCause       error
		signalCause       error
		cancelCaller      bool
		cancelBeforeScan  bool
		wantShards        int
		wantShardCount    int64
		wantScanned       int32
		wantClassReported bool
		wantOrder         []string
		wantErr           error
	}{
		{
			name: "all classes", class: "",
			wantShards: 1, wantShardCount: 1, wantScanned: 1, wantClassReported: true,
		},
		{
			name: "single class", class: className,
			wantShards: 1, wantShardCount: 1, wantScanned: 1, wantClassReported: true,
		},
		{
			name: "all classes, one index entry missing", class: "",
			withNilIndex: true, wantShards: 1, wantShardCount: 1, wantScanned: 1,
			wantClassReported: true,
		},
		{
			name: "all classes, counts summed across indices", class: "",
			extraIndices: 2, wantShards: 3, wantShardCount: 3, wantScanned: 1,
			wantClassReported: true,
		},
		{
			name: "all shards of an index", class: "", shards: []string{"s1", "s2", "s3"},
			wantShards: 3, wantShardCount: 3, wantScanned: 3, wantClassReported: true,
		},
		{
			// a diff of two scans shows what changed, not a reshuffled list
			name: "collections and shards reported in a fixed order", class: "",
			shards: []string{"s3", "s1", "s6", "s4", "s2", "s5"}, extraIndices: 3,
			wantShards: 9, wantShardCount: 9, wantScanned: 6, wantClassReported: true,
			wantOrder: []string{
				"extra0", "extra2", "s1", "s2", "s3", "s4", "s5", "s6", "extra1",
			},
		},
		{
			// a shard created after the read, or one already gone from the schema
			name: "shard missing from the sharding state", class: "",
			shards: []string{"s1", "s2"}, shardsNotInSchema: []string{"s2"},
			wantShards: 2, wantShardCount: 2, wantScanned: 2, wantClassReported: true,
		},
		{
			// the shard list is fixed before the schema is read, so a shard that
			// appears afterwards is not part of a scan that is already running
			name: "shard created while the scan runs", class: "",
			shardOnSchemaRead: "s2", wantShards: 1, wantShardCount: 1, wantScanned: 1,
			wantClassReported: true,
		},
		{
			name: "shard filter matches one of many", class: "", shard: "s1",
			extraIndices: 2, wantShards: 1, wantShardCount: 1, wantScanned: 1,
			wantClassReported: true,
		},
		{
			name: "shard filter matches nothing", class: "", shard: "nosuchshard",
			wantShards: 0, wantShardCount: 0, wantScanned: 0,
		},
		{
			name: "index already dropped", class: className,
			closeIndex: true, closedCause: errIndexDropped,
			wantShards: 0, wantShardCount: 0, wantScanned: 0,
		},
		{
			name: "index already shut down", class: className,
			closeIndex: true, closedCause: errIndexShutdown, wantShards: 0, wantScanned: 0,
			wantErr: errIndexShutdown,
		},
		{
			// no production teardown leaves an unsignalled close; see errIndexClosed
			name: "index closed with no cause", class: className,
			closeIndex: true, wantShards: 0, wantScanned: 0, wantErr: errIndexClosed,
		},
		{
			// the scan reached every shard, so a drop signalled afterwards must not discard the result
			name: "drop requested while the scan finished", class: "",
			signalCause: errIndexDropped, wantShards: 1, wantShardCount: 1, wantScanned: 1,
			wantClassReported: true,
		},
		{
			name: "shutdown requested while the scan finished", class: "",
			signalCause: errIndexShutdown, wantShards: 1, wantShardCount: 1, wantScanned: 1,
			wantClassReported: true,
		},
		{
			name: "drop requested mid-scan", class: "", shards: []string{"s1", "s2"},
			signalCause: errIndexDropped, wantShards: 0, wantShardCount: 0, wantScanned: 1,
		},
		{
			name: "drop requested mid-scan keeps the other indices", class: "",
			shards: []string{"s1", "s2"}, signalCause: errIndexDropped, extraIndices: 2,
			wantShards: 2, wantShardCount: 2, wantScanned: 1,
		},
		{
			name: "shutdown requested mid-scan", class: "", shards: []string{"s1", "s2"},
			signalCause: errIndexShutdown, wantShards: 0, wantScanned: 1,
			wantErr: errIndexShutdown,
		},
		{
			name: "drop requested mid-scan with a cancelled caller", class: "",
			shards: []string{"s1", "s2"}, signalCause: errIndexDropped, cancelCaller: true,
			wantShards: 0, wantScanned: 1, wantErr: context.Canceled,
		},
		{
			name: "caller gave up before the scan started", class: "",
			cancelBeforeScan: true, wantShards: 0, wantScanned: 0,
			wantErr: context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shardNames := tt.shards
			if shardNames == nil {
				shardNames = []string{"s1"}
			}
			// the scan only blocks once it reaches a shard of the index under test
			blocking := !tt.closeIndex && !tt.cancelBeforeScan &&
				(tt.shard == "" || slices.Contains(shardNames, tt.shard))

			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseScan := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseScan()

			logger, _ := test.NewNullLogger()
			idx, scanned := shardedIndex(t, className, shardNames, entered, release, blocking)
			if len(tt.shardsNotInSchema) > 0 {
				inSchema := slices.DeleteFunc(slices.Clone(shardNames), func(name string) bool {
					return slices.Contains(tt.shardsNotInSchema, name)
				})
				idx.schemaReader = scannableSchemaReader(className, inSchema)
			}
			if tt.shardOnSchemaRead != "" {
				storeShardOnSchemaRead(t, idx, tt.shardOnSchemaRead)
			}
			if tt.closeIndex {
				if tt.closedCause != nil {
					idx.signalCloseRequested(tt.closedCause)
				}
				idx.closed = true
			}
			db := &DB{logger: logger, indices: map[string]*Index{idx.ID(): idx}}
			if tt.withNilIndex {
				db.indices["gone"] = nil
			}
			// the extras sort on both sides of the collection under test, so a scan
			// that stops at it is told apart from one that carries on
			for i := 0; i < tt.extraIndices; i++ {
				extraClass := fmt.Sprintf("Head%d", i)
				if i%2 == 1 {
					extraClass = fmt.Sprintf("Tail%d", i)
				}
				extra, _ := shardedIndex(t, extraClass,
					[]string{fmt.Sprintf("extra%d", i)}, nil, nil, false)
				db.indices[extra.ID()] = extra
			}

			callerCtx, cancelCaller := context.WithCancel(context.Background())
			defer cancelCaller()
			if tt.cancelBeforeScan {
				cancelCaller()
			}

			var shards []*models.NodeShardStatus
			var stats *models.NodeStats
			var err error
			done := make(chan struct{})
			go func() {
				defer close(done)
				stats, err = db.localNodeShardStats(callerCtx, &shards, tt.class, tt.shard)
			}()

			if blocking {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("shard scan never started")
				}

				if db.indexLock.TryLock() {
					db.indexLock.Unlock()
				} else {
					assert.Fail(t, "indexLock must be free while shards are scanned")
				}
				if idx.dropIndex.TryLock() {
					idx.dropIndex.Unlock()
					assert.Fail(t, "the scanned index must be held against a drop")
				}
				if tt.cancelCaller {
					cancelCaller()
				}
				// a requested close must unblock the scan on its own
				if tt.signalCause != nil {
					idx.signalCloseRequested(tt.signalCause)
				} else {
					releaseScan()
				}
			}

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("shard scan never finished")
			}

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, stats, "a scan that reported an error must not report counts")
			} else {
				require.NoError(t, err)
				require.NotNil(t, stats)
				assert.Equal(t, tt.wantShardCount, stats.ShardCount, "shard count")
				assert.Equal(t, tt.wantShardCount, stats.ObjectCount, "object count")
			}
			require.Len(t, shards, tt.wantShards)
			if tt.wantOrder != nil {
				reported := make([]string, len(shards))
				for i, shard := range shards {
					reported[i] = shard.Name
				}
				assert.Equal(t, tt.wantOrder, reported, "order of the reported shards")
			}
			assert.Equal(t, tt.wantScanned, scanned.Load(), "shards scanned")
			classReported := slices.ContainsFunc(shards, func(s *models.NodeShardStatus) bool {
				return s.Class == className
			})
			assert.Equal(t, tt.wantClassReported, classReported, "shards of the collection under test")
			for _, shard := range shards {
				wantReplicas := int64(1)
				if slices.Contains(tt.shardsNotInSchema, shard.Name) {
					// a shard the read did not cover reports the collection's factor
					wantReplicas = 3
				}
				assert.Equal(t, wantReplicas, shard.NumberOfReplicas, "replicas of shard %q", shard.Name)
			}
			for _, index := range db.indices {
				if index == nil {
					continue
				}
				wantReads := 1
				if tt.cancelBeforeScan {
					wantReads = 0
				}
				assert.LessOrEqual(t, schemaReads(t, index), wantReads,
					"schema reads of collection %q", index.Config.ClassName)
			}
		})
	}
}

// TestGetOneNodeStatusLocal pins how a local scan that cannot finish is reported:
// running out of time times out this node alone, a shutdown fails the request.
func TestGetOneNodeStatusLocal(t *testing.T) {
	const className = "Local"

	tests := []struct {
		name        string
		expiredCtx  bool
		closedCause error
		wantStatus  string
		wantShards  int
		wantErr     error
	}{
		{
			name: "healthy node", wantStatus: models.NodeStatusStatusHEALTHY, wantShards: 1,
		},
		{
			name: "scan ran out of time", expiredCtx: true,
			wantStatus: models.NodeStatusStatusTIMEOUT, wantShards: 0,
		},
		{
			name: "node shutting down", closedCause: errIndexShutdown,
			wantErr: errIndexShutdown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			idx, _ := shardedIndex(t, className, []string{"s1"}, nil, nil, false)
			if tt.closedCause != nil {
				idx.signalCloseRequested(tt.closedCause)
				idx.closed = true
			}
			db := &DB{
				logger:       logger,
				indices:      map[string]*Index{idx.ID(): idx},
				schemaGetter: &fakeSchemaGetter{},
			}

			ctx := context.Background()
			if tt.expiredCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
			}

			status, err := db.GetOneNodeStatus(ctx, db.schemaGetter.NodeName(),
				"", "", verbosity.OutputVerbose)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, status)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, status.Status)
			assert.Equal(t, tt.wantStatus, *status.Status)
			assert.Len(t, status.Shards, tt.wantShards)
		})
	}
}

// TestGetNodeStatusRemoteNodeCannotAnswer pins that a remote node which cannot
// answer is reported on its own, leaving the status of every other node intact.
func TestGetNodeStatusRemoteNodeCannotAnswer(t *testing.T) {
	const className = "Remote"

	healthy := models.NodeStatusStatusHEALTHY

	tests := []struct {
		name       string
		remoteErr  error
		wantStatus string
		wantErr    bool
	}{
		{
			name:       "node answers",
			wantStatus: models.NodeStatusStatusHEALTHY,
		},
		{
			name:       "node refuses to answer while shutting down",
			remoteErr:  enterrors.NewErrUnexpectedStatusCode(http.StatusBadRequest, []byte("collection is closed")),
			wantStatus: models.NodeStatusStatusUNAVAILABLE,
		},
		{
			name:       "node cannot be reached",
			remoteErr:  enterrors.NewErrSendHttpRequest(errors.New("connection refused")),
			wantStatus: models.NodeStatusStatusUNAVAILABLE,
		},
		{
			name:       "node has no address to ask",
			remoteErr:  enterrors.NewErrOpenHttpRequest(errors.New("invalid host")),
			wantStatus: models.NodeStatusStatusUNAVAILABLE,
		},
		{
			name:       "node ran out of time",
			remoteErr:  enterrors.NewErrSendHttpRequest(context.DeadlineExceeded),
			wantStatus: models.NodeStatusStatusTIMEOUT,
		},
		{
			name:      "node sends a body that cannot be read",
			remoteErr: enterrors.NewErrUnmarshalBody(errors.New("invalid character")),
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			idx, _ := shardedIndex(t, className, []string{"s1"}, nil, nil, false)
			db := &DB{
				logger:       logger,
				indices:      map[string]*Index{idx.ID(): idx},
				schemaGetter: &nodeListSchemaGetter{nodeLists: [][]string{{"node1", "node2"}}},
				remoteNode: sharding.NewRemoteNode(
					&fakeRouter{hostnames: map[string]string{"node2": "node2:7101"}},
					&FakeRemoteNodeClient{
						Status: &models.NodeStatus{Name: "node2", Status: &healthy},
						Err:    tt.remoteErr,
					}),
			}

			statuses, err := db.GetNodeStatus(context.Background(), "", "", verbosity.OutputVerbose)

			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, statuses)
				return
			}
			require.NoError(t, err)
			require.Len(t, statuses, 2)
			require.NotNil(t, statuses[0].Status)
			assert.Equal(t, models.NodeStatusStatusHEALTHY, *statuses[0].Status, "status of the local node")
			assert.Len(t, statuses[0].Shards, 1, "shards of the local node")
			require.NotNil(t, statuses[1].Status)
			assert.Equal(t, tt.wantStatus, *statuses[1].Status, "status of the remote node")
		})
	}
}

// TestGetNodeStatusMembershipChange pins that a node joining or leaving while the
// request runs neither panics nor leaves a node of the list it works off silent.
func TestGetNodeStatusMembershipChange(t *testing.T) {
	const className = "Membership"

	healthy := models.NodeStatusStatusHEALTHY

	tests := []struct {
		name      string
		nodeLists [][]string
		wantNodes []string
	}{
		{
			name:      "node leaves",
			nodeLists: [][]string{{"node1", "node2"}, {"node1"}},
			wantNodes: []string{"node1", "node2"},
		},
		{
			name:      "node joins",
			nodeLists: [][]string{{"node1"}, {"node1", "node2"}},
			wantNodes: []string{"node1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			idx, _ := shardedIndex(t, className, []string{"s1"}, nil, nil, false)
			db := &DB{
				logger:       logger,
				indices:      map[string]*Index{idx.ID(): idx},
				schemaGetter: &nodeListSchemaGetter{nodeLists: tt.nodeLists},
				remoteNode: sharding.NewRemoteNode(
					&fakeRouter{hostnames: map[string]string{"node2": "node2:7101"}},
					&FakeRemoteNodeClient{Status: &models.NodeStatus{Name: "node2", Status: &healthy}}),
			}

			statuses, err := db.GetNodeStatus(context.Background(), "", "", verbosity.OutputVerbose)

			require.NoError(t, err)
			names := make([]string, len(statuses))
			for i, status := range statuses {
				require.NotNil(t, status, "status of every node of the list")
				names[i] = status.Name
			}
			assert.Equal(t, tt.wantNodes, names)
		})
	}
}

// TestGetNodeStatisticsRemoteNodeCannotAnswer pins that a remote node which
// cannot be reached is reported as unavailable rather than as an error.
func TestGetNodeStatisticsRemoteNodeCannotAnswer(t *testing.T) {
	tests := []struct {
		name       string
		remoteErr  error
		wantStatus string
		wantErr    bool
	}{
		{
			name: "node answers",
		},
		{
			name:       "node cannot be reached",
			remoteErr:  enterrors.NewErrSendHttpRequest(errors.New("connection refused")),
			wantStatus: models.StatisticsStatusUNAVAILABLE,
		},
		{
			name:       "node has no address to ask",
			remoteErr:  enterrors.NewErrOpenHttpRequest(errors.New("invalid host")),
			wantStatus: models.StatisticsStatusUNAVAILABLE,
		},
		{
			name:       "node ran out of time",
			remoteErr:  enterrors.NewErrSendHttpRequest(context.DeadlineExceeded),
			wantStatus: models.StatisticsStatusTIMEOUT,
		},
		{
			name:      "node sends a body that cannot be read",
			remoteErr: enterrors.NewErrUnmarshalBody(errors.New("invalid character")),
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			db := &DB{
				logger:       logger,
				schemaGetter: &fakeSchemaGetter{},
				remoteNode: sharding.NewRemoteNode(
					&fakeRouter{hostnames: map[string]string{"node2": "node2:7101"}},
					&FakeRemoteNodeClient{Err: tt.remoteErr}),
			}

			statistics, err := db.getNodeStatistics(context.Background(), "node2")

			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, statistics)
				return
			}
			require.NoError(t, err)
			if tt.wantStatus == "" {
				assert.Equal(t, &models.Statistics{}, statistics, "the answer of the node")
				return
			}
			require.NotNil(t, statistics.Status)
			assert.Equal(t, tt.wantStatus, *statistics.Status)
		})
	}
}

// nodeListSchemaGetter reports one node list per read, so a test can add or
// remove a node between the reads of a request.
type nodeListSchemaGetter struct {
	fakeSchemaGetter
	nodeLists [][]string
	reads     int
}

// Nodes reports the next list, and the last one once the lists run out.
func (g *nodeListSchemaGetter) Nodes() []string {
	nodes := g.nodeLists[min(g.reads, len(g.nodeLists)-1)]
	g.reads++
	return nodes
}

// scannableSchemaReader reports each shard name as a single-replica shard of the
// given class, which is all a node status scan reads. The collection asks for more
// replicas than any shard holds, so a shard read from the state is told apart from
// one falling back to the factor.
func scannableSchemaReader(className string, shardNames []string) *retryingSchemaReader {
	physical := make(map[string]sharding.Physical, len(shardNames))
	virtual := make([]sharding.Virtual, 0, len(shardNames))
	for _, name := range shardNames {
		physical[name] = sharding.Physical{Name: name, BelongsToNodes: []string{"node1"}}
		// a state without virtual shards is one the schema reader rejects
		virtual = append(virtual, sharding.Virtual{Name: name, AssignedToPhysical: name})
	}
	return &retryingSchemaReader{
		class: &models.Class{Class: className},
		state: &sharding.State{Physical: physical, Virtual: virtual, ReplicationFactor: 3},
	}
}

// schemaReads reports how often an index read the schema.
func schemaReads(t *testing.T, idx *Index) int {
	t.Helper()

	reader, ok := idx.schemaReader.(*retryingSchemaReader)
	require.True(t, ok, "index was not built with a counting schema reader")
	return reader.reads
}

// scannableShards builds one mock shard per name, each reporting a single object.
// When blocking, the first shard a scan reaches closes entered, then waits for
// release or a cancelled scan. The counter records how many shards were scanned.
func scannableShards(t *testing.T, shardNames []string,
	entered, release chan struct{}, blocking bool,
) (map[string]ShardLike, *atomic.Int32) {
	t.Helper()

	var scanned atomic.Int32
	shards := make(map[string]ShardLike, len(shardNames))
	for _, name := range shardNames {
		shard := NewMockShardLike(t)
		shard.EXPECT().Name().Return(name).Maybe()
		shard.EXPECT().GetStatus().Return(storagestate.StatusReady).Maybe()
		shard.EXPECT().ForEachVectorQueue(mock.Anything).Return(nil).Maybe()
		shard.EXPECT().ForEachGeoQueue(mock.Anything).Return(nil).Maybe()
		shard.EXPECT().ForEachVectorIndex(mock.Anything).Return(nil).Maybe()
		shard.EXPECT().getAsyncReplicationStats(mock.Anything).Return(nil).Maybe()
		shard.EXPECT().Shutdown(mock.Anything).Return(nil).Maybe()
		shard.EXPECT().ObjectCountAsync(mock.Anything).RunAndReturn(func(ctx context.Context) (int64, error) {
			first := scanned.Add(1) == 1
			if blocking && first {
				close(entered)
				// an aborted scan releases the shard through its context
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
			return 1, nil
		}).Maybe()
		shards[name] = shard
	}
	return shards, &scanned
}

// shardedIndex builds an index holding one shard per name, ready to be scanned.
func shardedIndex(t *testing.T, className string, shardNames []string,
	entered, release chan struct{}, blocking bool,
) (*Index, *atomic.Int32) {
	t.Helper()

	logger, _ := test.NewNullLogger()
	shards, scanned := scannableShards(t, shardNames, entered, release, blocking)
	return newTestIndex(t, logger, className, scannableSchemaReader(className, shardNames), shards), scanned
}

// retryingSchemaReader reproduces how the real schema reader resolves a class and
// retries every non-permanent error. reads counts how often the read ran, and
// onRead runs at the moment the state is handed out.
type retryingSchemaReader struct {
	schemaUC.SchemaReader
	class  *models.Class
	state  *sharding.State
	reads  int
	onRead func()
}

func (r *retryingSchemaReader) Read(_ string, retryIfClassNotFound bool,
	read func(*models.Class, *sharding.State) error,
) error {
	return backoff.Retry(func() error {
		r.reads++
		if r.state == nil {
			if retryIfClassNotFound {
				return clusterSchema.ErrClassNotFound
			}
			return backoff.Permanent(clusterSchema.ErrClassNotFound)
		}
		if r.onRead != nil {
			r.onRead()
		}
		return read(r.class, r.state)
	}, utils.NewBackoff())
}

// storeShardOnSchemaRead gives the index a scannable shard at the moment it reads
// the schema, standing in for a shard created while a scan is already running.
func storeShardOnSchemaRead(t *testing.T, idx *Index, shardName string) {
	t.Helper()

	reader, ok := idx.schemaReader.(*retryingSchemaReader)
	require.True(t, ok, "index was not built with a counting schema reader")
	shards, _ := scannableShards(t, []string{shardName}, nil, nil, false)
	reader.onRead = func() {
		for name, shard := range shards {
			idx.shards.Store(name, shard)
		}
	}
}

// TestIndexShutdownAbortsInFlightNodeStatusScan pins that Shutdown does not wait
// for a verbose scan holding closeLock, and that the aborted scan reports the
// shutdown instead of an empty collection.
func TestIndexShutdownAbortsInFlightNodeStatusScan(t *testing.T) {
	const className = "Closing"

	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	idx := newShutdownTestIndex(t, nil)
	idx.Config.ClassName = schema.ClassName(className)
	idx.schemaReader = scannableSchemaReader(className, []string{"s1", "s2"})
	shards, _ := scannableShards(t, []string{"s1", "s2"}, entered, release, true)
	for name, shard := range shards {
		idx.shards.Store(name, shard)
	}

	var status []*models.NodeShardStatus
	var scanErr error
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		_, _, scanErr = scanIndexShards(context.Background(), idx, &status, "")
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("shard scan never started")
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- idx.Shutdown(context.Background()) }()

	select {
	case err := <-shutdownDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown is blocked behind the in-flight node status scan")
	}

	<-scanDone
	assert.Empty(t, status, "a closing index must not report shards")
	require.ErrorIs(t, scanErr, errIndexShutdown,
		"a scan aborted by shutdown must say so, not report an empty collection")
}

// stubCommitLogStatsIndex adds the optional CommitlogStats method on top of a
// plain VectorIndex, the way hnsw (and dynamic wrapping hnsw) provide it.
type stubCommitLogStatsIndex struct {
	VectorIndex
	stats *compact.Stats
}

func (s stubCommitLogStatsIndex) CommitlogStats() *compact.Stats { return s.stats }

func TestShardVectorCommitLogStats(t *testing.T) {
	shard := NewMockShardLike(t)
	shard.EXPECT().ForEachVectorIndex(mock.Anything).RunAndReturn(
		func(f func(string, VectorIndex) error) error {
			// map iteration order is random; feed names unsorted on purpose
			require.NoError(t, f("second", stubCommitLogStatsIndex{stats: &compact.Stats{
				SortedFiles: 2, SnapshotTimestamp: 1234, TotalSizeBytes: 50, Cycles: 7,
			}}))
			require.NoError(t, f("flat", NewMockVectorIndex(t)))
			require.NoError(t, f("first", stubCommitLogStatsIndex{stats: &compact.Stats{
				RawFiles: 1, CondensedFiles: 3, Cycles: 1,
			}}))
			require.NoError(t, f("pending", stubCommitLogStatsIndex{stats: nil}))
			return nil
		})

	got := shardVectorCommitLogStats(shard)

	// "flat" has no CommitlogStats method, "pending" has not completed a
	// cycle yet: both are left out rather than reported as zeros.
	require.Len(t, got, 2)
	assert.Equal(t, &models.VectorCommitLogStats{
		Name: "first", RawFiles: 1, CondensedFiles: 3, CompactionCycles: 1,
	}, got[0])
	assert.Equal(t, &models.VectorCommitLogStats{
		Name: "second", SortedFiles: 2, SnapshotTimestamp: 1234,
		TotalSizeBytes: 50, CompactionCycles: 7,
	}, got[1])
}
