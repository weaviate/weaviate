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

package named_vectors_tests

import (
	"acceptance_tests_with_client/fixtures"
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

func testRestart(compose *docker.DockerCompose) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		host := compose.GetWeaviate().URI()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		t.Run("multiple named vectors", func(t *testing.T) {
			cleanup()

			t.Run("create schema", func(t *testing.T) {
				createNamedVectorsClass(t, client)
			})

			t.Run("batch create objects", func(t *testing.T) {
				objs := []*models.Object{}
				for id, book := range fixtures.Books() {
					obj := &models.Object{
						Class: className,
						ID:    strfmt.UUID(id),
						Properties: map[string]interface{}{
							"text": book.Description,
						},
					}
					objs = append(objs, obj)
				}

				resp, err := client.Batch().ObjectsBatcher().
					WithObjects(objs...).
					Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, resp)
			})

			t.Run("check existence", func(t *testing.T) {
				for id := range fixtures.Books() {
					exists, err := client.Data().Checker().
						WithID(id).
						WithClassName(className).
						Do(ctx)
					require.NoError(t, err)
					require.True(t, exists)
				}
			})

			t.Run("GraphQL get vectors", func(t *testing.T) {
				for id := range fixtures.Books() {
					resultVectors := getVectors(t, client, className, id, targetVectors...)
					checkTargetVectors(t, resultVectors)
				}
			})

			t.Run("GraphQL near<Media> check", func(t *testing.T) {
				for id, book := range fixtures.Books() {
					for _, targetVector := range targetVectors {
						nearText := client.GraphQL().NearTextArgBuilder().
							WithConcepts([]string{book.Title}).
							WithTargetVectors(targetVector)
						resultVectors := getVectorsWithNearText(t, client, className, id, nearText, targetVectors...)
						checkTargetVectors(t, resultVectors)
					}
				}
			})

			t.Run("GraphQL near<Media> check after restart", func(t *testing.T) {
				err := compose.Stop(ctx, compose.GetWeaviate().Name(), nil)
				require.NoError(t, err)

				err = compose.Start(ctx, compose.GetWeaviate().Name())
				require.NoError(t, err)

				host := compose.GetWeaviate().URI()
				client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
				require.Nil(t, err)

				for id, book := range fixtures.Books() {
					for _, targetVector := range targetVectors {
						nearText := client.GraphQL().NearTextArgBuilder().
							WithConcepts([]string{book.Title}).
							WithTargetVectors(targetVector)
						resultVectors := getVectorsWithNearText(t, client, className, id, nearText, targetVectors...)
						checkTargetVectors(t, resultVectors)
					}
				}
			})
		})
	}
}
