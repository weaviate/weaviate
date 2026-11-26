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

package consistency_level_metric

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const (
	className  = "ClMetric"
	metricOpen = "weaviate_consistency_level_requests_total{"
)

func TestConsistencyLevelMetric(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true").
		WithWeaviateEnv("EXPERIMENTAL_REST_SEARCH_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	node := compose.GetWeaviate()
	defer helper.ResetClient()
	helper.SetupClient(node.URI())
	helper.SetupGRPCClient(t, node.GrpcURI())
	grpcClient := helper.ClientGRPC(t)

	// Three shards, so a batch spanning them must still count once.
	helper.CreateClass(t, &models.Class{
		Class:          className,
		Vectorizer:     "none",
		ShardingConfig: map[string]any{"desiredCount": 3},
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
	})
	defer helper.DeleteClass(t, className)

	id := strfmt.UUID(uuid.NewString())
	all := pb.ConsistencyLevel_CONSISTENCY_LEVEL_ALL.Enum()

	cases := []struct {
		name      string
		operation string
		level     string
		send      func(t *testing.T)
	}{
		{"REST create", "write", "ALL", func(t *testing.T) {
			require.NoError(t, helper.CreateObjectCL(t, &models.Object{Class: className, ID: id}, types.ConsistencyLevelAll))
		}},
		{"REST get", "read", "ALL", func(t *testing.T) {
			_, err := helper.GetObjectCL(t, className, id, types.ConsistencyLevelAll)
			require.NoError(t, err)
		}},
		{"REST batch across shards", "write", "ALL", func(t *testing.T) {
			helper.CreateObjectsBatchCL(t, newObjects(30), types.ConsistencyLevelAll)
		}},
		{"REST get without level", "read", "UNSET", func(t *testing.T) {
			_, err := helper.GetObject(t, className, id)
			require.NoError(t, err)
		}},
		{"gRPC batch objects across shards", "write", "ALL", func(t *testing.T) {
			objs := make([]*pb.BatchObject, 30)
			for i := range objs {
				objs[i] = &pb.BatchObject{Collection: className, Uuid: uuid.NewString()}
			}
			reply, err := grpcClient.BatchObjects(ctx, &pb.BatchObjectsRequest{Objects: objs, ConsistencyLevel: all})
			require.NoError(t, err)
			require.Empty(t, reply.Errors)
		}},
		{"gRPC search", "read", "ALL", func(t *testing.T) {
			_, err := grpcClient.Search(ctx, &pb.SearchRequest{Collection: className, ConsistencyLevel: all, Uses_127Api: true})
			require.NoError(t, err)
		}},
		{"gRPC batch stream with several messages", "write", "ALL", func(t *testing.T) {
			batchStream(ctx, t, grpcClient, all, 3)
		}},
		// GraphQL exposes consistencyLevel only on classes with RF > 1.
		{"GraphQL get", "read", "UNSET", func(t *testing.T) {
			graphqlhelper.AssertGraphQL(t, helper.RootAuth,
				fmt.Sprintf("{ Get { %s { title } } }", className))
		}},
		{"REST search", "read", "ALL", func(t *testing.T) {
			restSearchBm25(t, node.URI(), map[string]any{"query": "x", "consistencyLevel": "ALL"})
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := scrapeConsistencyLevelRequests(ctx, t, node)
			tc.send(t)
			after := scrapeConsistencyLevelRequests(ctx, t, node)

			key := tc.operation + "/" + tc.level
			require.Equal(t, before[key]+1, after[key], "series %s", key)
			require.Equal(t, sum(before)+1, sum(after), "no other series may move")
		})
	}
}

func newObjects(n int) []*models.Object {
	objs := make([]*models.Object, n)
	for i := range objs {
		objs[i] = &models.Object{Class: className, ID: strfmt.UUID(uuid.NewString())}
	}
	return objs
}

// batchStream opens one stream and sends its objects over several data
// messages, so the server runs several internal sub-batches for one stream.
func batchStream(ctx context.Context, t *testing.T, client pb.WeaviateClient, cl *pb.ConsistencyLevel, messages int) {
	t.Helper()
	stream, err := client.BatchStream(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Start_{Start: &pb.BatchStreamRequest_Start{ConsistencyLevel: cl}},
	}))
	msg, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, msg.GetStarted())

	for range messages {
		objs := make([]*pb.BatchObject, 10)
		for i := range objs {
			objs[i] = &pb.BatchObject{Collection: className, Uuid: uuid.NewString()}
		}
		require.NoError(t, stream.Send(&pb.BatchStreamRequest{
			Message: &pb.BatchStreamRequest_Data_{Data: &pb.BatchStreamRequest_Data{
				Objects: &pb.BatchStreamRequest_Data_Objects{Values: objs},
			}},
		}))
	}
	require.NoError(t, stream.Send(&pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Stop_{Stop: &pb.BatchStreamRequest_Stop{}},
	}))
	require.NoError(t, stream.CloseSend())

	var succeeded int
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.Empty(t, msg.GetResults().GetErrors())
		succeeded += len(msg.GetResults().GetSuccesses())
	}
	require.Equal(t, messages*10, succeeded)
}

func restSearchBm25(t *testing.T, uri string, body map[string]any) {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(fmt.Sprintf("http://%s/v1/search/%s/bm25", uri, className), "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// scrapeConsistencyLevelRequests returns the counter's series keyed by
// "operation/consistency_level".
func scrapeConsistencyLevelRequests(ctx context.Context, t *testing.T, node *docker.DockerContainer) map[string]float64 {
	t.Helper()
	code, reader, err := node.Container().Exec(ctx, []string{
		"sh", "-c", "wget -qO- http://127.0.0.1:2112/metrics || curl -s http://127.0.0.1:2112/metrics",
	}, tcexec.Multiplexed())
	require.NoError(t, err)
	require.Equal(t, 0, code)
	out, err := io.ReadAll(reader)
	require.NoError(t, err)

	series := map[string]float64{}
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.HasPrefix(line, metricOpen) {
			continue
		}
		labels, value, ok := strings.Cut(strings.TrimPrefix(line, metricOpen), "} ")
		require.True(t, ok, line)
		v, err := strconv.ParseFloat(value, 64)
		require.NoError(t, err)
		series[label(labels, "operation")+"/"+label(labels, "consistency_level")] = v
	}
	return series
}

func label(labels, name string) string {
	for _, kv := range strings.Split(labels, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok && k == name {
			return strings.Trim(v, `"`)
		}
	}
	return ""
}

func sum(series map[string]float64) float64 {
	var total float64
	for _, v := range series {
		total += v
	}
	return total
}
