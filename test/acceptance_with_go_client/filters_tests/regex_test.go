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

package filters_tests

import (
	"context"
	"testing"

	acceptance_with_go_client "acceptance_tests_with_client"

	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestWhereFilter_Regex(t *testing.T) {
	ctx := context.Background()
	config := wvt.Config{Scheme: "http", Host: "localhost:8080"}
	client, err := wvt.NewClient(config)
	require.NoError(t, err)
	require.NotNil(t, client)
	// clean DB
	err = client.Schema().AllDeleter().Do(ctx)
	require.NoError(t, err)

	t.Run("special characters in where filter value", func(t *testing.T) {
		vectorizer := "text2vec-contextionary"
		id1 := "00000000-0000-0000-0000-000000000001"
		className := "Regex"

		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:         "summary",
					DataType:     []string{schema.DataTypeText.String()},
					Tokenization: "field",
					ModuleConfig: map[string]interface{}{
						vectorizer: map[string]interface{}{
							"skip":                  true,
							"vectorizePropertyName": "false",
						},
					},
				},
				{
					Name:         "summaries",
					DataType:     []string{schema.DataTypeTextArray.String()},
					Tokenization: "field",
					ModuleConfig: map[string]interface{}{
						vectorizer: map[string]interface{}{
							"skip":                  false,
							"vectorizePropertyName": "false",
						},
					},
				},
			},
		}
		err = client.Schema().ClassCreator().WithClass(class).Do(ctx)
		require.NoError(t, err)
		text := `The Hitchhiker's Guide to the Galaxy by Douglas Adams is a comedic science) fiction masterpiece that follows the bewildering adventures of Arthur Dent, an unwitting Earthling, after the destruction of his planet to make way for an interstellar highway. Joined by Ford Prefect, a researcher for the titular guidebook, Zaphod Beeblebrox, a two-headed ex-president, Trillian, the sole human survivor, and Marvin, a depressed robot, Arthur navigates the cosmos while encountering absurdity and philosophical musings. Adams' satirical wit and irreverent humor make this quintessential work a hilarious exploration of the absurdities of life, space, and everything in between.`
		_, err = client.Data().Creator().
			WithClassName(className).
			WithID(id1).
			WithProperties(map[string]interface{}{
				"summary":   text,
				"summaries": []string{text},
			}).
			Do(ctx)
		require.NoError(t, err)
		exists, err := client.Data().Checker().WithClassName(className).WithID(id1).Do(ctx)
		require.NoError(t, err)
		require.True(t, exists)
		tests := []struct {
			name  string
			where *filters.WhereBuilder
		}{
			{
				name: "special character in text array property",
				where: filters.Where().
					WithPath([]string{"summaries"}).
					WithOperator(filters.Like).
					WithValueString("*science)*"),
			},
			{
				name: "special character in text property",
				where: filters.Where().
					WithPath([]string{"summary"}).
					WithOperator(filters.Like).
					WithValueString("*science)*"),
			},
			{
				name: "without special character in text array property",
				where: filters.Where().
					WithPath([]string{"summaries"}).
					WithOperator(filters.Like).
					WithValueString("*science*"),
			},
			{
				name: "without special character in text property",
				where: filters.Where().
					WithPath([]string{"summary"}).
					WithOperator(filters.Like).
					WithValueString("*science*"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				fields := []graphql.Field{
					{Name: "_additional", Fields: []graphql.Field{{Name: "id"}}},
				}
				resp, err := client.GraphQL().Get().
					WithClassName(className).
					WithWhere(tt.where).
					WithFields(fields...).
					Do(ctx)
				require.Nil(t, err)
				require.Empty(t, resp.Errors)
				ids := acceptance_with_go_client.GetIds(t, resp, className)
				require.NotEmpty(t, ids)
				require.ElementsMatch(t, ids, []string{id1})
			})
		}
	})
}
