//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/weaviate/weaviate/grpc"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func TestGRPC(t *testing.T) {
	grpcHost := "localhost:50051"
	grpcClient, err := helper.CreateGrpcClient(grpcHost)
	require.NoError(t, err)
	require.NotNil(t, grpcClient)

	// create Books class
	booksClass := books.ClassContextionaryVectorizer()
	helper.CreateClass(t, booksClass)
	defer helper.DeleteClass(t, booksClass.Class)

	t.Run("gRPC Batch import", func(t *testing.T) {
		resp, err := grpcClient.BatchObjects(context.TODO(), &pb.BatchObjectsRequest{
			Objects: books.BatchObjects(),
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("gRPC Search", func(t *testing.T) {
		resp, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{
			ClassName: booksClass.Class,
			Properties: &pb.Properties{
				NonRefProperties: []string{"title"},
			},
			AdditionalProperties: &pb.AdditionalProperties{
				Uuid: true,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Results)
		assert.Equal(t, len(books.BatchObjects()), len(resp.Results))
		for i := range resp.Results {
			res := resp.Results[i]
			id := res.AdditionalProperties.Id
			assert.True(t, id == books.Dune.String() || id == books.ProjectHailMary.String() || id == books.TheLordOfTheIceGarden.String())
			title, ok := res.Properties.NonRefProperties.AsMap()["title"]
			require.True(t, ok)
			expectedTitle := ""
			if id == books.Dune.String() {
				expectedTitle = "Dune"
			}
			if id == books.ProjectHailMary.String() {
				expectedTitle = "Project Hail Mary"
			}
			if id == books.TheLordOfTheIceGarden.String() {
				expectedTitle = "The Lord of the Ice Garden"
			}
			assert.Equal(t, expectedTitle, title)
		}
	})
}
