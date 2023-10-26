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

package schema

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

/// TODO-RAFT START
// Fix Unit Tests
// With class-related transactions refactored into class.go, we need to re-implement the following test cases:
// - Validation (previously located in validation_test.go)
// - Updating existing classes (previously in update_test.go)
// - Restore classes once Restore is implemented (previously in restore_test.go)
// Please consult the previous implementation's test files for reference.
/// TODO-RAFT END

func TestHandler_GetSchema(t *testing.T) {
	handler, shutdown := newTestHandler(t, &fakeDB{})
	defer func() {
		fut := shutdown()
		require.Nil(t, fut.Error())
	}()

	sch, err := handler.GetSchema(nil)
	assert.Nil(t, err)
	assert.NotNil(t, sch)
}

func TestHandler_AddClass(t *testing.T) {
	ctx := context.Background()

	t.Run("happy path", func(t *testing.T) {
		handler, shutdown := newTestHandler(t, &fakeDB{})
		defer func() {
			fut := shutdown()
			require.Nil(t, fut.Error())
		}()

		class := models.Class{
			Class: "NewClass",
			Properties: []*models.Property{
				{DataType: []string{"text"}, Name: "textProp"},
				{DataType: []string{"int"}, Name: "intProp"},
			},
			Vectorizer: "none",
		}
		err := handler.AddClass(ctx, nil, &class)
		assert.Nil(t, err)

		sch := handler.GetSchemaSkipAuth()
		require.Nil(t, err)
		require.NotNil(t, sch)
		require.NotNil(t, sch.Objects)
		require.Len(t, sch.Objects.Classes, 1)
		assert.Equal(t, class.Class, sch.Objects.Classes[0].Class)
		assert.Equal(t, class.Properties, sch.Objects.Classes[0].Properties)
	})

	t.Run("with empty class name", func(t *testing.T) {
		handler, shutdown := newTestHandler(t, &fakeDB{})
		defer func() {
			fut := shutdown()
			require.Nil(t, fut.Error())
		}()
		class := models.Class{}
		err := handler.AddClass(ctx, nil, &class)
		assert.EqualError(t, err, "'' is not a valid class name")
	})

	t.Run("with permuted-casing class names", func(t *testing.T) {
		handler, shutdown := newTestHandler(t, &fakeDB{})
		defer func() {
			fut := shutdown()
			require.Nil(t, fut.Error())
		}()
		class1 := models.Class{Class: "NewClass", Vectorizer: "none"}
		err := handler.AddClass(ctx, nil, &class1)
		require.Nil(t, err)
		class2 := models.Class{Class: "NewCLASS", Vectorizer: "none"}
		err = handler.AddClass(ctx, nil, &class2)
		assert.EqualError(t, err,
			`class name "NewCLASS" already exists as a permutation of: "NewClass". `+
				`class names must be unique when lowercased`)
	})
}
