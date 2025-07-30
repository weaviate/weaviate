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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

const (
	UUID0 = "00000000-0000-0000-0000-000000000001"
	UUID1 = "00000000-0000-0000-0000-000000000002"
	UUID2 = "00000000-0000-0000-0000-000000000003"
)

func TestGrpcBatchReferences(t *testing.T) {
	ctx := context.Background()

	helper.SetupClient("localhost:8080")
	grpcClient, _ := newClient(t)

	clsA := articles.ArticlesClass()
	clsP := articles.ParagraphsClass()

	helper.DeleteClass(t, clsP.Class)
	helper.CreateClass(t, clsP)
	defer helper.DeleteClass(t, clsP.Class)

	helper.DeleteClass(t, clsA.Class)
	helper.CreateClass(t, clsA)
	defer helper.DeleteClass(t, clsA.Class)

	// Add two paragraphs
	paragraphs := []*models.Object{
		articles.NewParagraph().WithContents("Paragraph 1").WithID(UUID1).Object(),
		articles.NewParagraph().WithContents("Paragraph 2").WithID(UUID2).Object(),
	}
	helper.CreateObjectsBatch(t, paragraphs)

	// Add an article
	article := articles.NewArticle().WithTitle("Article 1").WithID(UUID0).Object()
	helper.CreateObject(t, article)

	// Batch add references
	res, err := grpcClient.BatchReferences(ctx, &pb.BatchReferencesRequest{
		References: []*pb.BatchReference{
			{
				Name:           "hasParagraphs",
				FromCollection: clsA.Class,
				ToCollection:   clsP.Class,
				FromUuid:       UUID0,
				ToUuid:         UUID1,
			},
			{
				Name:           "hasParagraphs",
				FromCollection: clsA.Class,
				ToCollection:   clsP.Class,
				FromUuid:       UUID0,
				ToUuid:         UUID2,
			},
		},
	})
	require.NoError(t, err, "BatchReferences should not return an error")
	require.Len(t, res.Errors, 0, "Expected no errors in batch references response")

	obj, err := helper.GetObject(t, clsA.Class, UUID0)
	require.NoError(t, err, "GetObject should not return an error")
	require.Len(t, obj.Properties.(map[string]any)["hasParagraphs"], 2, "Expected two references in hasParagraphs")
}
