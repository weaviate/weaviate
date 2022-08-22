//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package schema

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/models"
	schemauc "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_SchemaRepo(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	r, err := NewRepo(dirName, logger)
	require.Nil(t, err)

	t.Run("asking for a schema before any has been imported", func(t *testing.T) {
		res, err := r.LoadSchema(context.Background())
		require.Nil(t, err)
		assert.Nil(t, res)
	})

	t.Run("storing a schema", func(t *testing.T) {
		err := r.SaveSchema(context.Background(), exampleSchema())
		require.Nil(t, err)
	})

	t.Run("retrieveing a stored schema", func(t *testing.T) {
		res, err := r.LoadSchema(context.Background())
		require.Nil(t, err)
		expected := exampleSchema()
		assert.Equal(t, &expected, res)
	})
}

func exampleSchema() schemauc.State {
	return schemauc.State{
		ObjectSchema: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "MyAction",
					Properties: []*models.Property{
						{
							Name:     "myActionProp",
							DataType: []string{"string"},
						},
					},
				},
				{
					Class: "MyThing",
					Properties: []*models.Property{
						{
							Name:     "myThingProp",
							DataType: []string{"string"},
						},
					},
				},
			},
		},
	}
}
