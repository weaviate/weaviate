//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

func Test_AliasesAPI_gRPC(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithText2VecModel2Vec().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	gRPCClient := func(t *testing.T, addr string) (pb.WeaviateClient, *grpc.ClientConn) {
		conn, err := helper.CreateGrpcConnectionClient(addr)
		require.NoError(t, err)
		require.NotNil(t, conn)
		grpcClient := helper.CreateGrpcWeaviateClient(conn)
		require.NotNil(t, grpcClient)
		return grpcClient, conn
	}

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))

	helper.SetupClient(compose.GetWeaviate().URI())
	grpcClient, _ := gRPCClient(t, compose.GetWeaviate().GrpcURI())
	require.NotNil(t, gRPCClient)

	booksAliasName := "BooksAlias"

	t.Run("create schema", func(t *testing.T) {
		booksClass := books.ClassModel2VecVectorizer()
		helper.CreateClass(t, booksClass)
		for _, book := range books.Objects() {
			helper.CreateObject(t, book)
			helper.AssertGetObjectEventually(t, book.Class, book.ID)
		}
	})

	t.Run("create alias", func(t *testing.T) {
		alias := &models.Alias{Alias: booksAliasName, Class: books.DefaultClassName}
		helper.CreateAlias(t, alias)
		resp := helper.GetAliases(t, &alias.Class)
		require.NotNil(t, resp)
		require.NotEmpty(t, resp.Aliases)
	})

	tests := []struct {
		name       string
		collection string
	}{
		{
			name:       "search using collection name",
			collection: books.DefaultClassName,
		},
		{
			name:       "search using alias",
			collection: booksAliasName,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("search", func(t *testing.T) {
				t.Run("get", func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
						Collection:  tt.collection,
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Len(t, resp.Results, 3)
				})
				t.Run("get with filters", func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
						Collection: tt.collection,
						Metadata:   &pb.MetadataRequest{Vector: true, Uuid: true},
						Filters: &pb.Filters{
							Operator:  pb.Filters_OPERATOR_EQUAL,
							On:        []string{"title"},
							TestValue: &pb.Filters_ValueText{ValueText: "Dune"},
						},
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Len(t, resp.Results, 1)
					assert.Equal(t, resp.Results[0].Metadata.Id, books.Dune.String())
					assert.NotEmpty(t, resp.Results[0].Metadata.GetVectorBytes())
				})
				t.Run("nearText", func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
						Collection: tt.collection,
						Metadata:   &pb.MetadataRequest{Uuid: true},
						NearText: &pb.NearTextSearch{
							Query: []string{"Dune"},
						},
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Len(t, resp.Results, 3)
					assert.Equal(t, resp.Results[0].Metadata.Id, books.Dune.String())
				})
				t.Run("bm25", func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
						Collection: tt.collection,
						Metadata:   &pb.MetadataRequest{Uuid: true},
						Bm25Search: &pb.BM25{
							Query:      "Dune",
							Properties: []string{"title"},
						},
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Len(t, resp.Results, 1)
					assert.Equal(t, resp.Results[0].Metadata.Id, books.Dune.String())
				})
				t.Run("hybrid", func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
						Collection: tt.collection,
						Metadata:   &pb.MetadataRequest{Uuid: true},
						HybridSearch: &pb.Hybrid{
							Query: "Project",
							Alpha: 0.75,
						},
						Properties: &pb.PropertiesRequest{
							NonRefProperties: []string{"title"},
						},
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Len(t, resp.Results, 3)
					assert.Equal(t, resp.Results[0].Metadata.Id, books.ProjectHailMary.String())
				})
			})
			t.Run("aggregate using alias", func(t *testing.T) {
				t.Run("count", func(t *testing.T) {
					resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
						Collection:   tt.collection,
						ObjectsCount: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					require.NotNil(t, resp.GetSingleResult())
					require.Equal(t, int64(3), resp.GetSingleResult().GetObjectsCount())
				})
				t.Run("count with filters", func(t *testing.T) {
					resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
						Collection: tt.collection,
						Filters: &pb.Filters{
							Operator:  pb.Filters_OPERATOR_EQUAL,
							On:        []string{"title"},
							TestValue: &pb.Filters_ValueText{ValueText: "Dune"},
						},
						ObjectsCount: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					require.NotNil(t, resp.GetSingleResult())
					require.Equal(t, int64(1), resp.GetSingleResult().GetObjectsCount())
				})
				t.Run("count with nearText", func(t *testing.T) {
					certainty := float64(0.8)
					resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
						Collection: tt.collection,
						Filters: &pb.Filters{
							Operator:  pb.Filters_OPERATOR_EQUAL,
							On:        []string{"title"},
							TestValue: &pb.Filters_ValueText{ValueText: "Dune"},
						},
						Search: &pb.AggregateRequest_NearText{
							NearText: &pb.NearTextSearch{
								Query:     []string{"Dune"},
								Certainty: &certainty,
							},
						},
						ObjectsCount: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					require.NotNil(t, resp.GetSingleResult())
					require.Equal(t, int64(1), resp.GetSingleResult().GetObjectsCount())
				})
			})
		})
	}

	t.Run("batch insert using alias", func(t *testing.T) {
		theMartian := "67b79643-cf8b-4b22-b206-000000000001"
		resp, err := grpcClient.BatchObjects(ctx, &pb.BatchObjectsRequest{
			Objects: []*pb.BatchObject{
				{
					Collection: booksAliasName,
					Uuid:       theMartian,
					Properties: &pb.BatchObject_Properties{
						NonRefProperties: &structpb.Struct{
							Fields: map[string]*structpb.Value{
								"title":       structpb.NewStringValue("The Martian"),
								"description": structpb.NewStringValue("Stranded on Mars after a dust storm forces his crew to evacuate, astronaut Mark Watney is presumed dead and left alone on the hostile planet."),
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		tests := []struct {
			name       string
			collection string
		}{
			{
				name:       "search using collection name",
				collection: books.DefaultClassName,
			},
			{
				name:       "search using alias",
				collection: booksAliasName,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Run("count", func(t *testing.T) {
					resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
						Collection:   tt.collection,
						ObjectsCount: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					require.NotNil(t, resp.GetSingleResult())
					require.Equal(t, int64(4), resp.GetSingleResult().GetObjectsCount())
				})
				t.Run("search using id", func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
						Collection: tt.collection,
						Metadata:   &pb.MetadataRequest{Vector: true, Uuid: true},
						Filters: &pb.Filters{
							Operator:  pb.Filters_OPERATOR_EQUAL,
							On:        []string{filters.InternalPropID},
							TestValue: &pb.Filters_ValueText{ValueText: theMartian},
						},
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					require.Len(t, resp.Results, 1)
					assert.Equal(t, theMartian, resp.Results[0].Metadata.Id)
				})
			})
		}
	})
}
