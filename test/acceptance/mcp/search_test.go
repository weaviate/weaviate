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

package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/search"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestQueryHybridTool(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := articles.ParagraphsClass()
	helper.DeleteClassAuth(t, cls.Class, apiKey)
	helper.CreateClassAuth(t, cls, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	contents := "A para"
	objects := []*models.Object{
		articles.NewParagraph().WithContents(contents).Object(),
	}
	helper.CreateObjectsBatchAuth(t, objects, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var results *search.QueryHybridResp
	alpha := 0.0 // Use pure BM25 keyword search (no vectorization needed)
	err := helper.CallToolOnce(ctx, t, "weaviate-query-hybrid", &search.QueryHybridArgs{
		CollectionName:   cls.Class,
		Query:            contents,
		Alpha:            &alpha,
		TargetProperties: []string{cls.Properties[0].Name},
	}, &results)
	require.Nil(t, err)

	require.NotNil(t, results)
	require.Len(t, results.Results, 1)
	parsed, ok := results.Results[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, contents, parsed[cls.Properties[0].Name])
}
