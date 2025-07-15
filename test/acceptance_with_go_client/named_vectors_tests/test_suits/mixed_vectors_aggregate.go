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

package test_suits

import (
	"context"
	"fmt"
	"testing"

	acceptance_with_go_client "acceptance_tests_with_client"

	"github.com/weaviate/weaviate/entities/modelsext"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
)

func testMixedVectorsAggregate(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.NoError(t, err)

		require.NoError(t, client.Schema().AllDeleter().Do(ctx))
		class := createMixedVectorsSchema(t, client)

		creator := client.Data().Creator()
		_, err = creator.WithClassName(class.Class).WithID(id1).
			WithProperties(map[string]any{"text": "Hello", "number": 1}).
			Do(ctx)
		require.NoError(t, err)

		_, err = creator.WithClassName(class.Class).WithID(id2).
			WithProperties(map[string]any{"text": "World", "number": 2}).
			Do(ctx)
		require.NoError(t, err)

		testAllObjectsIndexed(t, client, class.Class)
		for _, targetVector := range []string{"", modelsext.DefaultNamedVectorName, contextionary} {
			t.Run(fmt.Sprintf("vector=%q", targetVector), func(t *testing.T) {
				no := &graphql.NearObjectArgumentBuilder{}
				no = no.WithID(id1).WithCertainty(0.9)
				if targetVector != "" {
					no = no.WithTargetVectors(targetVector)
				}

				agg, err := client.GraphQL().
					Aggregate().
					WithClassName(class.Class).
					WithNearObject(no).
					WithFields(graphql.Field{Name: "number", Fields: []graphql.Field{{Name: "maximum"}}}).
					Do(ctx)
				require.NoError(t, err)

				maximums := acceptance_with_go_client.ExtractGraphQLField[float64](t, agg, "Aggregate", class.Class, "number", "maximum")
				assert.Equal(t, []float64{1}, maximums)
			})
		}
	}
}
