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
	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestInsertOneTool(t *testing.T) {
	helper.SetupClient("localhost:8080")

	cls := articles.ParagraphsClass()
	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)
	defer helper.DeleteClass(t, cls.Class)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var resp *create.InsertOneResp
	err := callToolOnce(ctx, t, "insert-one", &create.InsertOneArgs{
		Collection: cls.Class,
		Properties: map[string]any{
			"contents": "Test Content",
		},
	}, &resp)
	require.Nil(t, err)

	require.NotNil(t, resp)
	require.NotEmpty(t, resp.ID)
}
