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
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
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

	read := func(x replica.Result[replica.SimpleResponse], successes []replica.SimpleResponse, failures []replica.SimpleResponse) ([]replica.SimpleResponse, []replica.SimpleResponse, bool, error) {
		var err error
		decreaseLevel := true
		if x.Err != nil {
			failures = append(failures, x.Value)
			if len(x.Value.Errors) == 0 {
				err = x.Err
			}
			decreaseLevel = false
		}
		return successes, failures, decreaseLevel, err
	}

	flatten := func(batchSize int, rs []replica.SimpleResponse, defaultError error) []error {
		errs := make([]error, 0, batchSize)
		for _, r := range rs {
			if e := r.FirstError(); e != nil {
				errs = append(errs, e)
			}
		}
		if len(errs) == 0 && defaultError != nil {
			for i := 0; i < batchSize; i++ {
				errs = append(errs, defaultError)
			}
		}
		return errs
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

	teapotFailure := failure(http.StatusTeapot, "what a weird error", broadcastError)
	internalBroadcastFailure := failure(http.StatusInternalServerError, "internal server error", broadcastError)
	internalCommitFailure := failure(http.StatusInternalServerError, "internal server error", commitError)

	for _, tt := range []struct {
		name      string
		cl        types.ConsistencyLevel
		node1     *httptest.Server
		node2     *httptest.Server
		node3     *httptest.Server
		shouldErr bool
	}{
		{
			"SUCCESS:CL.ALL;eventually successful broadcast and commit errors",
			types.ConsistencyLevelAll,
			newServer(eventualSuccess(0, broadcastAndCommitError)),
			newServer(eventualSuccess(1, broadcastAndCommitError)),
			newServer(eventualSuccess(2, broadcastAndCommitError)),
			false,
		},
		{
			"FAILURE:CL.ALL;non-retriable broadcast error",
			types.ConsistencyLevelAll,
			newServer(success),
			newServer(success),
			newServer(teapotFailure),
			true,
		},
		{
			"SUCCESS:CL.QUORUM;broadcast failure on one node",
			types.ConsistencyLevelQuorum,
			newServer(success),
			newServer(success),
			newServer(internalBroadcastFailure),
			false,
		},
		{
			"SUCCESS:CL.QUORUM;commit failure on one node",
			types.ConsistencyLevelQuorum,
			newServer(success),
			newServer(success),
			newServer(internalCommitFailure),
			false,
		},
		{
			"SUCCESS:CL.ONE;two broadcast failures",
			types.ConsistencyLevelOne,
			newServer(success),
			newServer(internalBroadcastFailure),
			newServer(internalBroadcastFailure),
			false,
		},
		{
			"SUCCESS:CL.ONE;one broadcast and one commit failures",
			types.ConsistencyLevelOne,
			newServer(success),
			newServer(internalBroadcastFailure),
			newServer(internalCommitFailure),
			false,
		},
		{
			"SUCCESS:CL.ONE;two commit failures",
			types.ConsistencyLevelOne,
			newServer(success),
			newServer(internalCommitFailure),
			newServer(internalCommitFailure),
			false,
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
			// shuffle the replicas to ensure no ordering dependency
			for i := range replicas {
				j := rand.Intn(i + 1)
				replicas[i], replicas[j] = replicas[j], replicas[i]
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

			errs, err := coordinator.Push(context.Background(), cl, broadcast(client), commit(client), read, flatten, 1)
			if tt.shouldErr {
				require.NoError(t, err)
				require.Greater(t, len(errs), 0)
				return
			}
			require.NoError(t, err)
			require.Len(t, errs, 0)
		})
	}
}

func Test_coordinatorPull(t *testing.T) {
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
		readRoutingPlan := types.ReadRoutingPlan{
			Shard:               shard,
			Tenant:              "",
			ReplicaSet:          types.ReadReplicaSet{Replicas: replicas},
			ConsistencyLevel:    cl,
			IntConsistencyLevel: cl.ToInt(len(replicas)),
		}
		mockRouter.EXPECT().BuildReadRoutingPlan(routingPlanOptions).Return(readRoutingPlan, nil).Once()

		return mockRouter
	}

	op := func(client replica.Client) func(ctx context.Context, host string, fullRead bool) (types.RepairResponse, error) {
		return func(ctx context.Context, host string, fullRead bool) (types.RepairResponse, error) {
			xs, err := client.DigestObjects(ctx, host, class, shard, []strfmt.UUID{fakeObj.Object.ID}, 0)
			var x types.RepairResponse
			if len(xs) == 1 {
				x = xs[0]
			}
			return x, err
		}
	}

	success := func(w http.ResponseWriter, r *http.Request) {
		b, err := json.Marshal([]types.RepairResponse{{
			ID: fakeObj.Object.ID.String(),
		}})
		require.NoError(t, err)
		w.Write(b)
	}

	failure := func(status int, msg string) func(w http.ResponseWriter, r *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, msg, status)
		}
	}

	eventualSuccess := func(failures int) func(w http.ResponseWriter, r *http.Request) {
		count := 0
		return func(w http.ResponseWriter, r *http.Request) {
			if count < failures {
				failure(http.StatusInternalServerError, "temporary failure")(w, r)
				count++
				return
			}
			success(w, r)
		}
	}

	newServer := func(handler func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(handler))
	}

	teapotFailure := failure(http.StatusTeapot, "what a weird error")
	internalServerFailure := failure(http.StatusInternalServerError, "internal server error")

	for _, tt := range []struct {
		name      string
		cl        types.ConsistencyLevel
		node1     *httptest.Server
		node2     *httptest.Server
		node3     *httptest.Server
		shouldErr bool
	}{
		{
			"SUCCESS:CL.ALL;eventually successful errors",
			types.ConsistencyLevelAll,
			newServer(eventualSuccess(0)),
			newServer(eventualSuccess(1)),
			newServer(eventualSuccess(2)),
			false,
		},
		{
			"SUCCESS:CL.QUORUM;retriable failure on first node",
			types.ConsistencyLevelQuorum,
			newServer(internalServerFailure),
			newServer(success),
			newServer(success),
			false,
		},
		{
			"SUCCESS:CL.ONE;retriable failures on first and second nodes",
			types.ConsistencyLevelOne,
			newServer(internalServerFailure),
			newServer(internalServerFailure),
			newServer(success),
			false,
		},
		{
			"SUCCESS:CL.ONE;non-retriable error on first node",
			types.ConsistencyLevelOne,
			newServer(teapotFailure),
			newServer(success),
			newServer(success),
			false,
		},
		// TODO(tommy/mooga): This test immediately below currently succeeds after ~6-7 seconds. Is this expected or is it a bug?
		{
			"SUCCESS:CL.ONE;first is eventually successful, others fail",
			types.ConsistencyLevelOne,
			newServer(eventualSuccess(3)),
			newServer(internalServerFailure),
			newServer(internalServerFailure),
			false,
		},
		// {
		// 	"FAILURE:CL.ALL;one non-retriable error",
		// 	types.ConsistencyLevelAll,
		// 	newServer(success),
		// 	newServer(success),
		// 	newServer(teapotFailure),
		// 	true,
		// },
		// {
		// 	"FAILURE:CL.QUORUM;two non-retriable errors",
		// 	types.ConsistencyLevelQuorum,
		// 	newServer(success),
		// 	newServer(teapotFailure),
		// 	newServer(teapotFailure),
		// 	true,
		// },
		// {
		// 	"FAILURE:CL.ONE;all non-retriable errors",
		// 	types.ConsistencyLevelOne,
		// 	newServer(teapotFailure),
		// 	newServer(teapotFailure),
		// 	newServer(teapotFailure),
		// 	true,
		// },
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
			coordinator := replica.NewReadCoordinator[types.RepairResponse](
				setupRouter(cl, replicas),
				metrics,
				class,
				shard,
				requestId,
				logger,
			)

			ch, _, err := coordinator.Pull(context.Background(), cl, op(client), "", 100*time.Second)
			resps := []replica.Result[types.RepairResponse]{}
			level := cl.ToInt(len(replicas))
			for {
				res, ok := <-ch
				if !ok {
					break
				}
				resps = append(resps, res)
				level--
				if level == 0 {
					break
				}
			}
			var firstError error
			ids := []string{}
			for _, resp := range resps {
				if firstError == nil {
					firstError = resp.Err
				}
				if resp.Err == nil {
					ids = append(ids, resp.Value.ID)
				}
			}
			if tt.shouldErr {
				require.NoError(t, err)
				require.Error(t, firstError)
				return
			}
			require.NoError(t, err)
			require.Len(t, ids, cl.ToInt(len(replicas)))
			for i, id := range ids {
				require.Equal(t, fakeObj.Object.ID.String(), id, i)
			}
		})
	}
}
