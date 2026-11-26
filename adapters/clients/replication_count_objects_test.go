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

package clients

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/usecases/replica"
)

// countReplica is a REST replica answering GET .../_count with a fixed status.
func countReplica(t *testing.T, name string, status, count int) types.Replica {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status == http.StatusTeapot {
			http.Error(w, "418 Maintenance mode", status)
			return
		}
		w.WriteHeader(status)
		if status == http.StatusOK {
			fmt.Fprint(w, count)
		}
	}))
	t.Cleanup(ts.Close)
	return types.Replica{NodeName: name, ShardName: "S1", HostAddr: strings.TrimPrefix(ts.URL, "http://")}
}

// A count at ALL waits for every replica. A replica in maintenance answers 418 for as long
// as it stays in maintenance, so it must be given up on rather than retried to the deadline.
func TestFinderCountObjects_ReplicaInMaintenance(t *testing.T) {
	const (
		ok          = http.StatusOK
		maintenance = http.StatusTeapot
	)

	for _, tt := range []struct {
		name     string
		statuses []int
		want     int
	}{
		{name: "first replica", statuses: []int{maintenance, ok, ok}, want: 7},
		{name: "middle replica", statuses: []int{ok, maintenance, ok}, want: 7},
		{name: "last replica", statuses: []int{ok, ok, maintenance}, want: 7},
		{name: "two replicas", statuses: []int{maintenance, ok, maintenance}, want: 7},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var replicas []types.Replica
			for i, s := range tt.statuses {
				replicas = append(replicas, countReplica(t, fmt.Sprintf("node%d", i), s, 7))
			}

			logger, _ := test.NewNullLogger()
			metrics, err := replica.NewMetrics(nil)
			require.NoError(t, err)
			finder := replica.NewFinder("C1", nil, nil, "node0", newReplicationClient(t, http.DefaultClient),
				metrics, logger, func() string { return "" })

			plan := types.ReadRoutingPlan{
				Shard:               "S1",
				ReplicaSet:          types.ReadReplicaSet{Replicas: replicas},
				ConsistencyLevel:    types.ConsistencyLevelAll,
				IntConsistencyLevel: len(replicas),
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			started := time.Now()
			got, err := finder.CountObjects(ctx, plan)
			elapsed := time.Since(started)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Less(t, elapsed, 2*time.Second,
				"a replica in maintenance must not hold the count until the deadline")
		})
	}
}

func TestReplicationClient_CountObjects_ErrorClassification(t *testing.T) {
	for _, tt := range []struct {
		name        string
		status      int
		body        string
		maintenance bool
	}{
		{name: "418 maintenance mode", status: http.StatusTeapot, body: "418 Maintenance mode", maintenance: true},
		{name: "500 internal", status: http.StatusInternalServerError, body: "boom"},
		{name: "503 not ready", status: http.StatusServiceUnavailable, body: "503 Node not ready"},
		{name: "422 unprocessable", status: http.StatusUnprocessableEntity, body: "class not found"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, tt.body, tt.status)
			}))
			defer ts.Close()

			client := newReplicationClient(t, ts.Client())
			_, err := client.CountObjects(context.Background(), strings.TrimPrefix(ts.URL, "http://"), "C1", "S1")

			require.Error(t, err)
			assert.Equal(t, tt.maintenance, errors.Is(err, replica.ErrReplicaMaintenance))
			assert.ErrorContains(t, err, tt.body, "the peer's reason must stay in the message")
		})
	}
}

func TestGRPCReplicationClient_CountObjects_ErrorClassification(t *testing.T) {
	for _, tt := range []struct {
		name        string
		err         error
		maintenance bool
	}{
		{
			name:        "maintenance mode",
			err:         status.Error(codes.FailedPrecondition, "server is in maintenance mode"),
			maintenance: true,
		},
		{name: "internal", err: status.Error(codes.Internal, "boom")},
		{name: "not found", err: status.Error(codes.NotFound, "class not found")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeGRPCReplicationServer(t)
			fake.countObjectsErr = tt.err
			client, cleanup := setupGRPCTestServer(t, fake)
			defer cleanup()

			_, err := client.CountObjects(context.Background(), "passthrough:bufnet", "C1", "S1")

			require.Error(t, err)
			assert.ErrorContains(t, err, status.Convert(tt.err).Message(), "the call must reach the server")
			assert.Equal(t, tt.maintenance, errors.Is(err, replica.ErrReplicaMaintenance))
		})
	}
}
