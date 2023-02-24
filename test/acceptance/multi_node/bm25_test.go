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

package multi_node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

var paragraphs = []string{
	"Some random text",
	"Other text",
	"completely unrelated",
	"this has nothing to do with the rest",
}

func TestBm25MultiNode(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for i := 0; i < 5; i++ {
		runBM25MultinodeTest(t, ctx)
	}
}

func runBM25MultinodeTest(t *testing.T, ctx context.Context) {
	compose, err := docker.New().
		WithWeaviateCluster().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminte test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	helper.CreateClass(t, paragraphClass)
	for _, par := range paragraphs {
		obj := articles.NewParagraph().
			WithContents(par).
			Object()
		helper.SetupClient(compose.GetWeaviate().URI())
		helper.CreateObject(t, obj)
	}

	query := `
		{
			Get {
				Paragraph (bm25:{query:"random"}){
					contents
				}
			}
		}
		`
	result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	resParagraph := result.Get("Get", "Paragraph").AsSlice()
	require.Equal(t, resParagraph[0].(map[string]interface{})["contents"], paragraphs[0])
	timeout := time.Second
	require.Nil(t, compose.Stop(ctx, compose.GetWeaviate().Name(), &timeout))
	require.Nil(t, compose.Stop(ctx, compose.GetWeaviateNode2().Name(), &timeout))
}
