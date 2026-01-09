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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
)

func Test_coordinatorPush(t *testing.T) {
	class := "C1"
	shard := "S1"
	requestId := "R1"

	fakeObj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    "8c29da7a-600a-43dc-85fb-83ab2b08c294",
			Class: class,
			Properties: map[string]interface{}{
				"stringField": "somevalue",
			},
		},
	}

	logger, _ := test.NewNullLogger()

	promMetrics := monitoring.GetMetrics()
	metrics, err := replica.NewMetrics(promMetrics)
	require.NoError(t, err)

	setupRouter := func(cl types.ConsistencyLevel, replicas []types.Replica) *types.MockRouter {
		mockRouter := types.NewMockRouter(t)

		routingPlanOptions := types.RoutingPlanBuildOptions{Shard: shard, Tenant: "", ConsistencyLevel: cl, DirectCandidateNode: ""}
		mockRouter.EXPECT().BuildRoutingPlanOptions(shard, shard, cl, "").Return(routingPlanOptions).Once()
		writeRoutingPlan := types.WriteRoutingPlan{
			Shard:               shard,
			Tenant:              "",
			ReplicaSet:          types.WriteReplicaSet{Replicas: replicas},
			ConsistencyLevel:    cl,
			IntConsistencyLevel: cl.ToInt(len(replicas)),
		}
		mockRouter.EXPECT().BuildWriteRoutingPlan(routingPlanOptions).Return(writeRoutingPlan, nil).Once()

		return mockRouter
	}

	broadcast := func(client replica.Client) func(ctx context.Context, host, requestID string) error {
		return func(ctx context.Context, host, requestID string) error {
			resp, err := client.PutObject(ctx, host, class, shard, requestID, fakeObj, 0)
			if err == nil {
				err = resp.FirstError()
			}
			if err != nil {
				return fmt.Errorf("%q: %w", host, err)
			}
			return nil
		}
	}

	commit := func(client replica.Client) func(ctx context.Context, host, requestID string) (replica.SimpleResponse, error) {
		return func(ctx context.Context, host, requestID string) (replica.SimpleResponse, error) {
			resp := replica.SimpleResponse{}
			err := client.Commit(ctx, host, class, shard, requestID, &resp)
			if err == nil {
				err = resp.FirstError()
			}
			if err != nil {
				err = fmt.Errorf("%s: %w", host, err)
			}
			return resp, err
		}
	}

	success := func(w http.ResponseWriter, r *http.Request) {
		b, err := json.Marshal(replica.SimpleResponse{})
		require.NoError(t, err)
		w.Write(b)
	}
	failure := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		b, err := json.Marshal(replica.SimpleResponse{
			Errors: []replica.Error{{Msg: "simulated error"}},
		})
		require.NoError(t, err)
		w.Write(b)
	}
	newServer := func(handler func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(handler))
	}

	for _, tt := range []struct {
		name  string
		cl    types.ConsistencyLevel
		node1 *httptest.Server
		node2 *httptest.Server
		node3 *httptest.Server
	}{
		{
			"SUCCESS:CL.ALL",
			types.ConsistencyLevelAll,
			newServer(success),
			newServer(success),
			newServer(success),
		},
		{
			"SUCCESS:CL.QUORUM",
			types.ConsistencyLevelQuorum,
			newServer(success),
			newServer(success),
			newServer(failure),
		},
		{
			"SUCCESS:CL.ONE",
			types.ConsistencyLevelOne,
			newServer(success),
			newServer(failure),
			newServer(failure),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cl := tt.cl
			defer func() {
				tt.node1.Close()
				tt.node2.Close()
				tt.node3.Close()
			}()

			replicas := []types.Replica{
				{NodeName: "node1", ShardName: shard, HostAddr: tt.node1.URL[7:]},
				{NodeName: "node2", ShardName: shard, HostAddr: tt.node2.URL[7:]},
				{NodeName: "node3", ShardName: shard, HostAddr: tt.node3.URL[7:]},
			}

			client := clients.NewReplicationClient(&http.Client{})
			coordinator := replica.NewCoordinator[replica.SimpleResponse](
				client,
				setupRouter(cl, replicas),
				metrics,
				class,
				shard,
				requestId,
				logger,
			)

			ch, _, err := coordinator.Push(context.Background(), cl, broadcast(client), commit(client))
			require.NoError(t, err)
			for res := range ch {
				require.NoError(t, res.Err)
			}
		})
	}
}
