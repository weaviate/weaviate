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
	"strings"
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

type errorType string

const (
	broadcastError          errorType = "broadcast"
	commitError             errorType = "commit"
	broadcastAndCommitError errorType = "both"
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

	failure := func(status int, msg string, typ errorType) func(w http.ResponseWriter, r *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) {
			if typ == broadcastError && strings.Contains(r.URL.Path, "commit") {
				success(w, r)
				return
			}
			if typ == commitError && !strings.Contains(r.URL.Path, "commit") {
				success(w, r)
				return
			}
			w.WriteHeader(status)
			b, err := json.Marshal(replica.SimpleResponse{
				Errors: []replica.Error{{Msg: msg}},
			})
			require.NoError(t, err)
			w.Write(b)
		}
	}

	parse := func(level int, commitCh <-chan replica.Result[replica.SimpleResponse]) []error {
		return replica.NewStream().Read(1, level, commitCh)
	}

	eventualSuccess := func(failures int, typ errorType) func(w http.ResponseWriter, r *http.Request) {
		count := 0
		return func(w http.ResponseWriter, r *http.Request) {
			if count < failures {
				failure(http.StatusInternalServerError, "temporary failure", typ)(w, r)
				count++
				return
			}
			success(w, r)
		}
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
			"SUCCESS:CL.ALL;broadcast and commit errors",
			types.ConsistencyLevelAll,
			newServer(eventualSuccess(0, broadcastAndCommitError)),
			newServer(eventualSuccess(1, broadcastAndCommitError)),
			newServer(eventualSuccess(2, broadcastAndCommitError)),
		},
		{
			"SUCCESS:CL.QUORUM;broadcast failure on one node",
			types.ConsistencyLevelQuorum,
			newServer(success),
			newServer(success),
			newServer(failure(http.StatusInternalServerError, "node3 failed", broadcastError)),
		},
		{
			"SUCCESS:CL.QUORUM;commit failure on one node",
			types.ConsistencyLevelQuorum,
			newServer(success),
			newServer(success),
			newServer(failure(http.StatusInternalServerError, "node3 failed", commitError)),
		},
		{
			"SUCCESS:CL.ONE;two broadcast failures",
			types.ConsistencyLevelOne,
			newServer(success),
			newServer(failure(http.StatusInternalServerError, "node2 failed", broadcastError)),
			newServer(failure(http.StatusInternalServerError, "node3 failed", broadcastError)),
		},
		{
			"SUCCESS:CL.ONE;one broadcast and one commit failures",
			types.ConsistencyLevelOne,
			newServer(success),
			newServer(failure(http.StatusInternalServerError, "node2 failed", broadcastError)),
			newServer(failure(http.StatusInternalServerError, "node3 failed", commitError)),
		},
		{
			"SUCCESS:CL.ONE;two commit failures",
			types.ConsistencyLevelOne,
			newServer(success),
			newServer(failure(http.StatusInternalServerError, "node2 failed", commitError)),
			newServer(failure(http.StatusInternalServerError, "node3 failed", commitError)),
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
			coordinator := replica.NewWriteCoordinator[replica.SimpleResponse, error](
				client,
				setupRouter(cl, replicas),
				metrics,
				class,
				shard,
				requestId,
				logger,
			)

			errs, err := coordinator.Push(context.Background(), cl, broadcast(client), commit(client), parse)
			require.NoError(t, err)
			require.Len(t, errs, 0)
		})
	}
}
