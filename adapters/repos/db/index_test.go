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
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

func TestIndex_aggregateCount(t *testing.T) {
	logger, _ := test.NewNullLogger()
	metrics, err := NewMetrics(logger, monitoring.GetMetrics(), "Abc", "n/a")
	require.NoError(t, err, "create index metrics")

	replicaMetrics, err := replica.NewMetrics(monitoring.GetMetrics())
	require.NoError(t, err, "create replica metrics")

	startServer := func(t *testing.T, count int) *httptest.Server {
		t.Helper()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, strconv.Itoa(count))
		}))
		t.Cleanup(srv.Close)
		return srv
	}

	// Each test case will spin up N test HTTP servers, where N=len(counts).
	// Each server mocks a replica and reports its counts[i].
	// To simplify the setup, we assume the test collection "Abc" is
	// fully contained within a single shard "abc".
	// The aggregation requests satisfies [aggregate.IsCountStar], so we expect
	// each of the replicas to be queried and their results "averaged".
	for _, tt := range []struct {
		name   string // Test case name
		counts []int  // Counts reported by each replica.
		want   int    // Expected aggregated count
	}{
		{
			name:   "consistent count",
			counts: []int{92, 92, 92},
			want:   92,
		},
		{
			name:   "mode (2/3 replicas agree)",
			counts: []int{92, 13, 92},
			want:   92,
		},
		{
			name:   "median (3 replicas disagree)",
			counts: []int{80085, 33, 92},
			want:   92,
		},
		{
			name:   "shifted median (4 replicas disagree)",
			counts: []int{1, 2, 3, 4},
			want:   2,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			var replicas []types.Replica
			for i, count := range tt.counts {
				server := startServer(t, count)
				replicas = append(replicas, types.Replica{
					NodeName:  fmt.Sprintf("node-%d", i),
					HostAddr:  server.URL[7:],
					ShardName: "abc",
				})
			}
			readSet := types.ReadReplicaSet{
				Replicas: replicas,
			}

			mockSchemaReader := schemaUC.NewMockSchemaReader(t)
			mockSchemaReader.On("Shards", "Abc").Return([]string{"abc"}, nil)

			index := Index{
				router: &fakeRouter{
					readPlan: types.ReadRoutingPlan{
						LocalHostname:       "localhost",
						Shard:               "abc",
						IntConsistencyLevel: 3,
						ConsistencyLevel:    types.ConsistencyLevelAll,
						ReplicaSet:          readSet,
					},
					readSet: readSet,
				},
				shardCreateLocks: esync.NewKeyRWLocker(),
				Config:           IndexConfig{ClassName: schema.ClassName("Abc")},
				schemaReader:     mockSchemaReader,
				replicaClient:    clients.NewReplicationClient(&http.Client{}),
				metrics:          metrics,
				replicaMetrics:   replicaMetrics,
			}

			// Act
			res, err := index.aggregate(t.Context(), nil, aggregation.Params{
				IncludeMetaCount: true,
			}, nil, "")

			// Assert
			require.NoError(t, err, "aggregate")
			require.Len(t, res.Groups, 1, "number of groups")
			require.Equal(t, tt.want, res.Groups[0].Count, "object count")
		})
	}
}

type fakeRouter struct {
	hostnames map[string]string
	options   types.RoutingPlanBuildOptions
	readPlan  types.ReadRoutingPlan
	writePlan types.WriteRoutingPlan
	readSet   types.ReadReplicaSet
	writeSet  types.WriteReplicaSet
}

// AllHostnames implements [types.Router].
func (f *fakeRouter) AllHostnames() []string {
	return slices.Collect(maps.Values(f.hostnames))
}

var _ types.Router = (*fakeRouter)(nil)

func (f *fakeRouter) BuildReadRoutingPlan(types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
	return f.readPlan, nil
}

func (f *fakeRouter) BuildRoutingPlanOptions(tenant, shard string, cl types.ConsistencyLevel, directCandidate string) types.RoutingPlanBuildOptions {
	return f.options
}

func (f *fakeRouter) BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error) {
	return f.writePlan, nil
}

func (f *fakeRouter) GetReadReplicasLocation(collection, tenant, shard string) (types.ReadReplicaSet, error) {
	return f.readSet, nil
}

func (f *fakeRouter) GetReadWriteReplicasLocation(collection, tenant, shard string) (types.ReadReplicaSet, types.WriteReplicaSet, error) {
	return f.readSet, f.writeSet, nil
}

func (f *fakeRouter) GetWriteReplicasLocation(collection, tenant, shard string) (types.WriteReplicaSet, error) {
	panic("unimplemented")
}

func (f *fakeRouter) NodeHostname(nodeName string) (string, bool) {
	host, ok := f.hostnames[nodeName]
	return host, ok
}
