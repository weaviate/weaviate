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

package test_suits

import (
	"context"
	"testing"

	acceptance_with_go_client "acceptance_tests_with_client"

	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
)

func testMixedVectorsHybrid(host string) func(t *testing.T) {
	return func(t *testing.T) {
		var (
			ctx = context.Background()

			field = graphql.Field{
				Name: "_additional",
				Fields: []graphql.Field{
					{Name: "id"},
				},
			}
		)

		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		require.NoError(t, client.Schema().AllDeleter().Do(context.Background()))
		class := createMixedVectorsSchema(t, client)

		// insert objects
		for _, id := range []string{id1, id2} {
			_, err = client.Data().Creator().
				WithClassName(class.Class).
				WithID(id).
				WithProperties(map[string]interface{}{
					"text": "Some text goes here",
				}).
				Do(ctx)
			require.NoError(t, err)
		}

		namedResp, err := client.GraphQL().Get().
			WithClassName(class.Class).
			WithHybrid(client.GraphQL().
				HybridArgumentBuilder().
				WithQuery("Some text goes here").
				WithAlpha(0.5).
				WithTargetVectors(contextionary)).
			WithFields(field).
			Do(ctx)
		require.NoError(t, err)

		namedIds := acceptance_with_go_client.GetIds(t, namedResp, class.Class)
		require.ElementsMatch(t, namedIds, []string{id1, id2})

		legacyResp, err := client.GraphQL().Get().
			WithClassName(class.Class).
			WithHybrid(client.GraphQL().
				HybridArgumentBuilder().
				WithQuery("Some text goes here").
				WithAlpha(0.5)).
			WithFields(field).
			Do(ctx)
		require.NoError(t, err)

		require.Equal(t, namedResp, legacyResp)
	}
}
