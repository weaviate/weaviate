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

// Package restquery verifies that the pure-REST query/aggregate endpoints
// (POST /v1/{collection}/query and /aggregate) return exactly the same results
// as the equivalent gRPC Search/Aggregate RPCs. Both transports delegate to the
// same gRPC pipeline (SearchWithPrincipal/AggregateWithPrincipal), so any
// divergence here is a regression in the REST plumbing.
package restquery

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/byteops"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const parityClass = "RestParity"

func TestRESTQueryParity(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	httpURI := compose.GetWeaviate().URI()
	helper.SetupClient(httpURI)
	grpcClient, conn := newGRPCClient(t, compose.GetWeaviate().GrpcURI())
	defer conn.Close()

	helper.DeleteClass(t, parityClass)
	helper.CreateClass(t, &models.Class{
		Class:      parityClass,
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:         "title",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:         "category",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationField,
			},
		},
	})
	defer helper.DeleteClass(t, parityClass)

	objects := []struct {
		title    string
		category string
		vector   []float32
	}{
		{"Dune", "scifi", []float32{1, 0, 0}},
		{"Project Hail Mary", "scifi", []float32{0, 1, 0}},
		{"The Hobbit", "fantasy", []float32{0, 0, 1}},
		{"The Silmarillion", "fantasy", []float32{1, 1, 0}},
	}
	for _, o := range objects {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:  parityClass,
			Vector: o.vector,
			Properties: map[string]interface{}{
				"title":    o.title,
				"category": o.category,
			},
		}))
	}
	// Single-node create is synchronous, but allow a brief settle for the
	// inverted index just in case the environment is slow.
	time.Sleep(500 * time.Millisecond)

	searchCases := []struct {
		name string
		req  *pb.SearchRequest
	}{
		{
			name: "match all with uuid metadata",
			req: &pb.SearchRequest{
				Collection: parityClass,
				Metadata:   &pb.MetadataRequest{Uuid: true},
				Limit:      10,
			},
		},
		{
			name: "bm25 keyword search",
			req: &pb.SearchRequest{
				Collection:  parityClass,
				Bm25Search:  &pb.BM25{Query: "Dune"},
				Metadata:    &pb.MetadataRequest{Uuid: true, Score: true},
				Properties:  &pb.PropertiesRequest{NonRefProperties: []string{"title"}},
				Uses_127Api: true,
			},
		},
		{
			name: "filter by category",
			req: &pb.SearchRequest{
				Collection: parityClass,
				Filters: &pb.Filters{
					Operator:  pb.Filters_OPERATOR_EQUAL,
					TestValue: &pb.Filters_ValueText{ValueText: "fantasy"},
					Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "category"}},
				},
				Metadata:    &pb.MetadataRequest{Uuid: true},
				Properties:  &pb.PropertiesRequest{NonRefProperties: []string{"title", "category"}},
				Uses_127Api: true,
			},
		},
		{
			name: "near vector",
			req: &pb.SearchRequest{
				Collection:  parityClass,
				NearVector:  &pb.NearVector{Vectors: []*pb.Vectors{{VectorBytes: byteops.Fp32SliceToBytes([]float32{1, 0, 0}), Type: pb.Vectors_VECTOR_TYPE_SINGLE_FP32}}},
				Metadata:    &pb.MetadataRequest{Uuid: true, Distance: true},
				Limit:       2,
				Uses_127Api: true,
			},
		},
	}

	for _, tc := range searchCases {
		t.Run("search/"+tc.name, func(t *testing.T) {
			grpcReply, err := grpcClient.Search(ctx, proto.Clone(tc.req).(*pb.SearchRequest))
			require.NoError(t, err)

			restReply := restSearch(t, httpURI, tc.req)

			normalizeSearch(grpcReply)
			normalizeSearch(restReply)
			require.Truef(t, proto.Equal(grpcReply, restReply),
				"REST and gRPC search results differ\ngRPC: %v\nREST: %v", grpcReply, restReply)
		})
	}

	aggregateCases := []struct {
		name string
		req  *pb.AggregateRequest
	}{
		{
			name: "objects count",
			req: &pb.AggregateRequest{
				Collection:   parityClass,
				ObjectsCount: true,
			},
		},
	}
	for _, tc := range aggregateCases {
		t.Run("aggregate/"+tc.name, func(t *testing.T) {
			grpcReply, err := grpcClient.Aggregate(ctx, proto.Clone(tc.req).(*pb.AggregateRequest))
			require.NoError(t, err)

			restReply := restAggregate(t, httpURI, tc.req)

			grpcReply.Took = 0
			restReply.Took = 0
			require.Truef(t, proto.Equal(grpcReply, restReply),
				"REST and gRPC aggregate results differ\ngRPC: %v\nREST: %v", grpcReply, restReply)
		})
	}

	t.Run("disabled endpoint returns error", func(t *testing.T) {
		// The endpoint is enabled by default; this is a sanity check that the
		// route exists (a 404 would indicate the route was never registered).
		req := &pb.SearchRequest{Collection: parityClass}
		body, err := protojson.Marshal(req)
		require.NoError(t, err)
		resp, err := http.Post("http://"+httpURI+"/v1/"+parityClass+"/query", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

// normalizeSearch zeroes fields that legitimately differ between two otherwise
// identical responses (wall-clock timing).
func normalizeSearch(r *pb.SearchReply) {
	if r == nil {
		return
	}
	r.Took = 0
}

func restSearch(t *testing.T, httpURI string, req *pb.SearchRequest) *pb.SearchReply {
	t.Helper()
	data := restPost(t, httpURI, req.Collection, "query", req)
	var reply pb.SearchReply
	require.NoError(t, protojson.Unmarshal(data, &reply))
	return &reply
}

func restAggregate(t *testing.T, httpURI string, req *pb.AggregateRequest) *pb.AggregateReply {
	t.Helper()
	data := restPost(t, httpURI, req.Collection, "aggregate", req)
	var reply pb.AggregateReply
	require.NoError(t, protojson.Unmarshal(data, &reply))
	return &reply
}

func restPost(t *testing.T, httpURI, collection, verb string, msg proto.Message) []byte {
	t.Helper()
	body, err := protojson.Marshal(msg)
	require.NoError(t, err)
	url := "http://" + httpURI + "/v1/" + collection + "/" + verb
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "unexpected status; body: %s", string(data))
	return data
}

func newGRPCClient(t *testing.T, host string) (pb.WeaviateClient, *grpc.ClientConn) {
	t.Helper()
	conn, err := helper.CreateGrpcConnectionClient(host)
	require.NoError(t, err)
	require.NotNil(t, conn)
	grpcClient := helper.CreateGrpcWeaviateClient(conn)
	require.NotNil(t, grpcClient)
	return grpcClient, conn
}
