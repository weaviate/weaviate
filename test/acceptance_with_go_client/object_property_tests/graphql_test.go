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

package object_property_tests

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
)

func TestObjectProperty_GraphQL(t *testing.T) {
	ctx := context.Background()
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	id1 := strfmt.UUID("00000000-0000-0000-0000-000000000001")
	id2 := strfmt.UUID("00000000-0000-0000-0000-000000000002")

	// clean up DB
	err = client.Schema().AllDeleter().Do(context.Background())
	require.Nil(t, err)

	t.Run("graphql check", func(t *testing.T) {
		className := "Person"
		create, err := client.Batch().ObjectsBatcher().
			WithObjects(
				&models.Object{
					Class: className,
					ID:    id1,
					Properties: map[string]interface{}{
						"name": "John Doe",
						"json": map[string]interface{}{
							"firstName":   "John",
							"lastName":    "Doe",
							"proffession": "Accountant",
							"birthdate":   "2012-05-05T07:16:30+02:00",
							"phoneNumber": map[string]interface{}{
								"input":          "020 1234567",
								"defaultCountry": "nl",
							},
							"location": map[string]interface{}{
								"latitude":  52.366667,
								"longitude": 4.9,
							},
						},
					},
				},
				&models.Object{
					Class: className,
					ID:    id2,
					Properties: map[string]interface{}{
						"name": "Stacey Spears",
						"json": map[string]interface{}{
							"firstName":   "Stacey",
							"lastName":    "Spears",
							"proffession": "Singer",
							"birthdate":   "2011-05-05T07:16:30+02:00",
							"phoneNumber": map[string]interface{}{
								"input":          "020 1555444",
								"defaultCountry": "nl",
							},
							"location": map[string]interface{}{
								"latitude":  51.366667,
								"longitude": 5.9,
							},
						},
					},
				}).
			Do(ctx)
		require.NoError(t, err)
		require.NotNil(t, create)
		resp, err := client.GraphQL().Get().
			WithClassName(className).
			WithWhere(filters.Where().
				WithPath([]string{"id"}).
				WithOperator(filters.Equal).
				WithValueText(id1.String()),
			).
			WithFields(
				graphql.Field{Name: "name"},
				graphql.Field{Name: "json{firstName lastName phoneNumber{input} location{latitude longitude}}"},
				graphql.Field{Name: "_additional{id}"},
			).
			Do(ctx)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Empty(t, resp.Errors)
		require.NotNil(t, resp.Data)
		get, ok := resp.Data["Get"].(map[string]interface{})
		require.True(t, ok)
		require.Equal(t, 1, len(get))
		person, ok := get["Person"].([]interface{})
		require.True(t, ok)
		require.Equal(t, 1, len(person))
		p, ok := person[0].(map[string]interface{})
		require.True(t, ok)
		require.Equal(t, 3, len(p))
		_additional, ok := p["_additional"].(map[string]interface{})
		require.True(t, ok)
		require.Equal(t, 1, len(_additional))
		assert.Equal(t, id1.String(), _additional["id"])
		assert.Equal(t, "John Doe", p["name"])
		data, ok := p["json"].(map[string]interface{})
		require.True(t, ok)
		require.Equal(t, 4, len(data))
		assert.Equal(t, "John", data["firstName"])
		assert.Equal(t, "Doe", data["lastName"])
		phoneNumber, ok := data["phoneNumber"].(map[string]interface{})
		require.True(t, ok)
		require.Equal(t, 1, len(phoneNumber))
		assert.Equal(t, "020 1234567", phoneNumber["input"])
	})
}
