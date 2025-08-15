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

func TestSearchWithHybridTool(t *testing.T) {
	helper.SetupClient("localhost:8080")

	cls := articles.ParagraphsClass()
	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)
	defer helper.DeleteClass(t, cls.Class)

	contents := "A para"
	objects := []*models.Object{
		articles.NewParagraph().WithContents(contents).Object(),
	}
	helper.CreateObjectsBatch(t, objects)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var results []any
	err := callToolOnce[any](ctx, t, "search-with-hybrid", &search.SearchWithHybridArgs{
		Collection:       cls.Class,
		Query:            contents,
		TargetProperties: []string{cls.Properties[0].Name},
	}, &results)
	require.Nil(t, err)

	require.Len(t, results, 1)
	parsed, ok := results[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, contents, parsed[cls.Properties[0].Name])
}
