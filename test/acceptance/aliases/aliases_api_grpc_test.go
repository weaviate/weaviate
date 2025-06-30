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
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
	"google.golang.org/grpc"
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

	t.Run("search using alias", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:  booksAliasName,
			Uses_123Api: true,
			Uses_125Api: true,
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Results, 3)
	})
}
