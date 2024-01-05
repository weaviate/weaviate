//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestGRPC(t *testing.T) {
	conn, err := helper.CreateGrpcConnectionClient(":50051")
	require.NoError(t, err)
	require.NotNil(t, conn)
	grpcClient := helper.CreateGrpcWeaviateClient(conn)
	require.NotNil(t, grpcClient)

	// create Books class
	booksClass := books.ClassContextionaryVectorizer()
	helper.CreateClass(t, booksClass)
	defer helper.DeleteClass(t, booksClass.Class)

	t.Run("Health Check", func(t *testing.T) {
		client := grpc_health_v1.NewHealthClient(conn)
		check, err := client.Check(context.TODO(), &grpc_health_v1.HealthCheckRequest{})
		require.NoError(t, err)
		require.NotNil(t, check)
		assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING.Enum().Number(), check.Status.Number())
	})

	t.Run("Batch import", func(t *testing.T) {
		resp, err := grpcClient.BatchObjects(context.TODO(), &pb.BatchObjectsRequest{
			Objects: books.BatchObjects(),
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	tests := []struct {
		name string
		req  *pb.SearchRequest
	}{
		{
			name: "Search with props",
			req: &pb.SearchRequest{
				Collection: booksClass.Class,
				Properties: &pb.PropertiesRequest{
					NonRefProperties: []string{"title"},
					ObjectProperties: []*pb.ObjectPropertiesRequest{
						{
							PropName:            "meta",
							PrimitiveProperties: []string{"isbn"},
							ObjectProperties: []*pb.ObjectPropertiesRequest{
								{
									PropName:            "obj",
									PrimitiveProperties: []string{"text"},
								},
								{
									PropName:            "objs",
									PrimitiveProperties: []string{"text"},
								},
							},
						},
						{PropName: "reviews", PrimitiveProperties: []string{"tags"}},
					},
				},
				Metadata: &pb.MetadataRequest{
					Uuid: true,
				},
				Uses_123Api: true,
			},
		},
		{
			name: "Search without props",
			req: &pb.SearchRequest{
				Collection: booksClass.Class,
				Metadata: &pb.MetadataRequest{
					Uuid: true,
				},
				Uses_123Api: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scifi := "sci-fi"
			resp, err := grpcClient.Search(context.TODO(), tt.req)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.Results)
			assert.Equal(t, len(books.BatchObjects()), len(resp.Results))
			for i := range resp.Results {
				res := resp.Results[i]
				id := res.Metadata.Id

				assert.True(t, id == books.Dune.String() || id == books.ProjectHailMary.String() || id == books.TheLordOfTheIceGarden.String())
				titleRaw := res.Properties.NonRefProps.Fields["title"]
				require.NotNil(t, titleRaw)
				title := titleRaw.GetStringValue()
				require.NotNil(t, title)

				metaRaw := res.Properties.NonRefProps.Fields["meta"]
				require.NotNil(t, metaRaw)
				meta := metaRaw.GetObjectValue()
				require.NotNil(t, meta)
				isbnRaw := meta.GetFields()["isbn"]
				require.NotNil(t, isbnRaw)
				isbn := isbnRaw.GetStringValue()
				require.NotNil(t, isbn)

				objRaw := meta.GetFields()["obj"]
				require.NotNil(t, objRaw)
				obj := objRaw.GetObjectValue()
				require.NotNil(t, obj)

				objsRaw := meta.GetFields()["objs"]
				require.NotNil(t, objsRaw)
				objs := objsRaw.GetListValue()
				require.NotNil(t, objs)

				objEntryRaw := objs.Values[0]
				require.NotNil(t, objEntryRaw)
				objEntry := objEntryRaw.GetObjectValue()
				require.NotNil(t, objEntry)

				reviewsRaw := res.Properties.NonRefProps.Fields["reviews"]
				require.NotNil(t, reviewsRaw)
				reviews := reviewsRaw.GetListValue()
				require.NotNil(t, reviews)
				require.Len(t, reviews.Values, 1)

				review := reviews.Values[0].GetObjectValue()
				require.NotNil(t, review)

				tags := review.Fields["tags"].GetListValue()
				require.NotNil(t, tags)

				strTags := make([]string, len(tags.Values))
				for i, tag := range tags.Values {
					strTags[i] = tag.GetStringValue()
				}

				expectedTitle := ""
				expectedIsbn := ""
				expectedTags := []string{}
				if id == books.Dune.String() {
					expectedTitle = "Dune"
					expectedIsbn = "978-0593099322"
					expectedTags = []string{scifi, "epic"}
				}
				if id == books.ProjectHailMary.String() {
					expectedTitle = "Project Hail Mary"
					expectedIsbn = "978-0593135204"
					expectedTags = []string{scifi}
				}
				if id == books.TheLordOfTheIceGarden.String() {
					expectedTitle = "The Lord of the Ice Garden"
					expectedIsbn = "978-8374812962"
					expectedTags = []string{scifi, "fantasy"}
				}
				assert.Equal(t, expectedTitle, title)
				assert.Equal(t, expectedIsbn, isbn)
				assert.Equal(t, expectedTags, strTags)

				expectedObj := &pb.Properties{
					Fields: map[string]*pb.Value{
						"text": {Kind: &pb.Value_StringValue{StringValue: "some text"}},
					},
				}
				assert.Equal(t, expectedObj, obj)
				assert.Equal(t, expectedObj, objEntry)
			}
		})
	}

	t.Run("gRPC Search removed", func(t *testing.T) {
		_, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{})
		require.NotNil(t, err)
	})
}
