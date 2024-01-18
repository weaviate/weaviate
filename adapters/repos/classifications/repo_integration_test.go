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

//go:build integrationTest
// +build integrationTest

package classifications

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func Test_ClassificationsRepo(t *testing.T) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	r, err := NewRepo(dirName, logger)
	require.Nil(t, err)
	_ = r

	t.Run("asking for a non-existing classification", func(t *testing.T) {
		res, err := r.Get(context.Background(), "wrong-id")
		require.Nil(t, err)
		assert.Nil(t, res)
	})

	t.Run("storing classifications", func(t *testing.T) {
		err := r.Put(context.Background(), exampleOne())
		require.Nil(t, err)

		err = r.Put(context.Background(), exampleTwo())
		require.Nil(t, err)
	})

	t.Run("retrieving stored classifications", func(t *testing.T) {
		expectedOne := exampleOne()
		expectedTwo := exampleTwo()

		res, err := r.Get(context.Background(), expectedOne.ID)
		require.Nil(t, err)
		assert.Equal(t, &expectedOne, res)

		res, err = r.Get(context.Background(), expectedTwo.ID)
		require.Nil(t, err)
		assert.Equal(t, &expectedTwo, res)
	})
}

func exampleOne() models.Classification {
	return models.Classification{
		ID:                "01ed111a-919c-4dd5-ab9e-7b247b11e18c",
		Class:             "ExampleClassOne",
		BasedOnProperties: []string{"prop1"},
	}
}

func exampleTwo() models.Classification {
	return models.Classification{
		ID:                "4fbaebf3-41a9-414b-ac1d-433d74d4ef2c",
		Class:             "ExampleClassTwo",
		BasedOnProperties: []string{"prop2"},
	}
}
