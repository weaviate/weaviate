//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package esvector

import (
	"context"
	"testing"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEsVectorMigrator(t *testing.T) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9201"},
	})
	require.Nil(t, err)
	waitForEsToBeReady(t, client)
	logger, _ := test.NewNullLogger()
	repo := NewRepo(client, logger)
	migrator := NewMigrator(repo)

	t.Run("adding a class", func(t *testing.T) {
		type testCase struct {
			name  string
			kind  kind.Kind
			class *models.Class
		}

		tests := []testCase{
			{
				name: "thing class without props",
				kind: kind.Thing,
				class: &models.Class{
					Class: "MyThingClass",
				},
			},
			{
				name: "thing class with a string prop",
				kind: kind.Thing,
				class: &models.Class{
					Class: "MyThingClass",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
					},
				},
			},
			{
				name: "action class with an int prop",
				kind: kind.Action,
				class: &models.Class{
					Class: "MyClass",
					Properties: []*models.Property{
						&models.Property{
							Name:     "age",
							DataType: []string{string(schema.DataTypeInt)},
						},
					},
				},
			},
			{
				name: "action class with an float prop",
				kind: kind.Action,
				class: &models.Class{
					Class: "MyClass",
					Properties: []*models.Property{
						&models.Property{
							Name:     "weight",
							DataType: []string{string(schema.DataTypeNumber)},
						},
					},
				},
			},
			{
				name: "action class with bool prop",
				kind: kind.Action,
				class: &models.Class{
					Class: "MyClass",
					Properties: []*models.Property{
						&models.Property{
							Name:     "awesome",
							DataType: []string{string(schema.DataTypeBoolean)},
						},
					},
				},
			},
			{
				name: "action class with text, date and geo prop",
				kind: kind.Action,
				class: &models.Class{
					Class: "MyClass",
					Properties: []*models.Property{
						&models.Property{
							Name:     "content",
							DataType: []string{string(schema.DataTypeText)},
						},
						&models.Property{
							Name:     "date",
							DataType: []string{string(schema.DataTypeDate)},
						},
						&models.Property{
							Name:     "location",
							DataType: []string{string(schema.DataTypeGeoCoordinates)},
						},
					},
				},
			},
			{
				name: "action class with a ref prop",
				kind: kind.Action,
				class: &models.Class{
					Class: "MyClass",
					Properties: []*models.Property{
						&models.Property{
							Name:     "awesome",
							DataType: []string{"SomeClass"},
						},
					},
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				err := migrator.AddClass(context.Background(), test.kind, test.class)
				require.Nil(t, err)
			})

		}

	})

	t.Run("trying to update a class", func(t *testing.T) {
		t.Run("only modifying keywords", func(t *testing.T) {
			newKeywords := &models.Keywords{}
			err := migrator.UpdateClass(context.Background(), kind.Action, "MyClass",
				nil, newKeywords)
			assert.Nil(t, err, "should not error")
		})

		t.Run("trying to rename a prop", func(t *testing.T) {
			newName := "newName"
			err := migrator.UpdateClass(context.Background(), kind.Action, "MyClass",
				&newName, nil)
			assert.NotNil(t, err,
				"should error because props are immutable when the es vector index is activated")
		})
	})

	t.Run("trying to update a property", func(t *testing.T) {
		t.Run("only modifying keywords", func(t *testing.T) {
			newKeywords := &models.Keywords{}
			err := migrator.UpdateProperty(context.Background(), kind.Action, "MyClass",
				"myProp", nil, newKeywords)
			assert.Nil(t, err, "should not error")
		})

		t.Run("adding another data type", func(t *testing.T) {
			err := migrator.UpdatePropertyAddDataType(context.Background(), kind.Action,
				"MyClass", "myProp", "Foo")
			assert.Nil(t, err, "should not error")
		})

		t.Run("trying to rename a prop", func(t *testing.T) {
			newName := "newName"
			err := migrator.UpdateProperty(context.Background(), kind.Action, "MyClass",
				"myProp", &newName, nil)
			assert.NotNil(t, err,
				"should error because props are immutable when the es vector index is activated")
		})

	})

	t.Run("extending an existing class with a new property", func(t *testing.T) {
		prop := &models.Property{
			Name:     "aNewProp",
			DataType: []string{string(schema.DataTypeString)},
		}

		err := migrator.AddProperty(context.Background(), kind.Action, "MyClass", prop)
		assert.Nil(t, err)
	})

	t.Run("deleting a property", func(t *testing.T) {
		// elasticsearch does not support deleting a property, however, there is
		// also no harm in leaving it in the index. It is importing that it doesn't
		// error, though

		prop := "myProp"
		err := migrator.DropProperty(context.Background(), kind.Action, "MyClass", prop)
		assert.Nil(t, err)
	})

	t.Run("deleting a previously created class", func(t *testing.T) {
		err := migrator.DropClass(context.Background(), kind.Thing, "MyThingClass")
		assert.Nil(t, err)
		err = migrator.DropClass(context.Background(), kind.Action, "MyClass")
		assert.Nil(t, err)
	})

}

func TestEsVectorMigrator_ImportingConcepts(t *testing.T) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9201"},
	})
	require.Nil(t, err)
	waitForEsToBeReady(t, client)
	logger, _ := test.NewNullLogger()
	repo := NewRepo(client, logger)
	migrator := NewMigrator(repo)

	t.Run("add a thing to the schema", func(t *testing.T) {
		kind := kind.Thing
		class := &models.Class{
			Class: "MyTestClassWithProps",
			Properties: []*models.Property{
				&models.Property{
					Name:     "name",
					DataType: []string{string(schema.DataTypeString)},
				},
				&models.Property{
					Name:     "date",
					DataType: []string{string(schema.DataTypeDate)},
				},
				&models.Property{
					Name:     "location",
					DataType: []string{string(schema.DataTypeGeoCoordinates)},
				},
			},
		}

		err := migrator.AddClass(context.Background(), kind, class)
		require.Nil(t, err)
	})

	t.Run("importing an instance with only a string prop", func(t *testing.T) {
		thing := &models.Thing{
			Class: "MyTestClassWithProps",
			ID:    strfmt.UUID("id1"),
			Schema: map[string]interface{}{
				"name": "foo",
			},
		}

		err := repo.PutThing(context.Background(), thing, []float32{0, 0, 0})
		require.Nil(t, err)
	})

	t.Run("importing an instance with a geolocation prop", func(t *testing.T) {
		thing := &models.Thing{
			Class: "MyTestClassWithProps",
			ID:    strfmt.UUID("id2"),
			Schema: map[string]interface{}{
				"name": "bar",
				"location": &models.GeoCoordinates{
					Latitude:  1,
					Longitude: 2,
				},
			},
		}

		err := repo.PutThing(context.Background(), thing, []float32{0, 0, 0})
		require.Nil(t, err)
	})
}
