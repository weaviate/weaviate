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

package modules

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorizer(t *testing.T) {
	t.Run("when there are no models registered", func(t *testing.T) {
		p := NewProvider()
		_, err := p.Vectorizer("some-module", "MyClass")
		require.NotNil(t, err)
		assert.Equal(t, "no module with name \"some-module\" present", err.Error())
	})

	t.Run("module exist, but doesn't provide vectorizer", func(t *testing.T) {
		p := NewProvider()
		p.Register(newDummyVectorizerModule("some-module", ""))
		_, err := p.Vectorizer("some-module", "MyClass")
		require.NotNil(t, err)
		assert.Equal(t, "module \"some-module\" exists, but does not provide the "+
			"Vectorizer capability", err.Error())
	})
	t.Run("module exists, but the class doesn't", func(t *testing.T) {
		p := NewProvider()
		p.SetSchemaGetter(&fakeSchemaGetter{schema.Schema{}})
		p.Register(newDummyVectorizerModule("some-module", modulecapabilities.Text2Vec))
		_, err := p.Vectorizer("some-module", "MyClass")
		require.NotNil(t, err)
		assert.Equal(t, "class \"MyClass\" not found in schema", err.Error())
	})

	t.Run("module exist, and provides a vectorizer", func(t *testing.T) {
		p := NewProvider()
		sch := schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "MyClass",
					},
				},
			},
		}
		p.SetSchemaGetter(&fakeSchemaGetter{sch})
		p.Register(newDummyVectorizerModule("some-module", modulecapabilities.Text2Vec))
		vec, err := p.Vectorizer("some-module", "MyClass")
		require.Nil(t, err)

		obj := &models.Object{Class: "Test"}
		err = vec.UpdateObject(context.Background(), obj)
		require.Nil(t, err)

		assert.Equal(t, models.C11yVector{1, 2, 3}, obj.Vector)
	})
}

func TestReferenceVectorizer(t *testing.T) {
	t.Run("when there are no models registered", func(t *testing.T) {
		p := NewProvider()
		_, err := p.ReferenceVectorizer("some-module", "MyClass")
		require.NotNil(t, err)
		assert.Equal(t, "no module with name \"some-module\" present", err.Error())
	})

	t.Run("module exist, but doesn't provide reference vectorizer", func(t *testing.T) {
		p := NewProvider()
		p.Register(newDummyVectorizerModule("some-module", ""))
		_, err := p.ReferenceVectorizer("some-module", "MyClass")
		require.NotNil(t, err)
		assert.Equal(t, "module \"some-module\" exists, but does not provide the "+
			"ReferenceVectorizer capability", err.Error())
	})
	t.Run("module exists, but the class doesn't", func(t *testing.T) {
		p := NewProvider()
		p.SetSchemaGetter(&fakeSchemaGetter{schema.Schema{}})
		p.Register(newDummyVectorizerModule("some-module", modulecapabilities.Ref2Vec))
		_, err := p.ReferenceVectorizer("some-module", "MyClass")
		require.NotNil(t, err)
		assert.Equal(t, "class \"MyClass\" not found in schema", err.Error())
	})

	t.Run("module exist, and provides a reference vectorizer", func(t *testing.T) {
		p := NewProvider()
		sch := schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "MyClass",
					},
				},
			},
		}
		p.SetSchemaGetter(&fakeSchemaGetter{sch})
		p.Register(newDummyVectorizerModule("some-module", modulecapabilities.Ref2Vec))
		vec, err := p.ReferenceVectorizer("some-module", "MyClass")
		require.Nil(t, err)

		obj := &models.Object{Class: "Test"}
		err = vec.UpdateObject(context.Background(), obj)
		require.Nil(t, err)

		assert.Equal(t, models.C11yVector{1, 2, 3}, obj.Vector)
	})
}
