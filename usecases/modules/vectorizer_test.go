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
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProvider_Vectorizer(t *testing.T) {
	t.Run("when there are no models registered", func(t *testing.T) {
		p := NewProvider()
		_, err := p.Vectorizer("some-module", "MyClass")
		require.NotNil(t, err)
		assert.Equal(t, "no module with name \"some-module\" present", err.Error())
	})

	t.Run("module exist, but doesn't provide vectorizer", func(t *testing.T) {
		p := NewProvider()
		p.Register(newDummyModule("some-module", ""))
		_, err := p.Vectorizer("some-module", "MyClass")
		require.NotNil(t, err)
		assert.Equal(t, "module \"some-module\" exists, but does not provide the "+
			"Vectorizer capability", err.Error())
	})
	t.Run("module exists, but the class doesn't", func(t *testing.T) {
		p := NewProvider()
		p.SetSchemaGetter(&fakeSchemaGetter{schema.Schema{}})
		p.Register(newDummyModule("some-module", modulecapabilities.Text2Vec))
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
		p.Register(newDummyModule("some-module", modulecapabilities.Text2Vec))
		vec, err := p.Vectorizer("some-module", "MyClass")
		require.Nil(t, err)

		obj := &models.Object{Class: "Test"}
		err = vec.UpdateObject(context.Background(), obj)
		require.Nil(t, err)

		assert.Equal(t, models.C11yVector{1, 2, 3}, obj.Vector)
	})
}

func TestProvider_ReferenceVectorizer(t *testing.T) {
	t.Run("when there are no models registered", func(t *testing.T) {
		p := NewProvider()
		_, err := p.ReferenceVectorizer("some-module", "MyClass")
		require.NotNil(t, err)
		assert.Equal(t, "no module with name \"some-module\" present", err.Error())
	})

	t.Run("module exist, but doesn't provide reference vectorizer", func(t *testing.T) {
		p := NewProvider()
		p.Register(newDummyModule("some-module", ""))
		_, err := p.ReferenceVectorizer("some-module", "MyClass")
		require.NotNil(t, err)
		assert.Equal(t, "module \"some-module\" exists, but does not provide the "+
			"ReferenceVectorizer capability", err.Error())
	})
	t.Run("module exists, but the class doesn't", func(t *testing.T) {
		p := NewProvider()
		p.SetSchemaGetter(&fakeSchemaGetter{schema.Schema{}})
		p.Register(newDummyModule("some-module", modulecapabilities.Ref2Vec))
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
		p.Register(newDummyModule("some-module", modulecapabilities.Ref2Vec))
		vec, err := p.ReferenceVectorizer("some-module", "MyClass")
		require.Nil(t, err)

		obj := &models.Object{Class: "Test"}
		err = vec.UpdateObject(context.Background(), obj)
		require.Nil(t, err)

		assert.Equal(t, models.C11yVector{1, 2, 3}, obj.Vector)
	})
}

func TestProvider_ValidateVectorizer(t *testing.T) {
	t.Run("with vectorizer module", func(t *testing.T) {
		p := NewProvider()
		vec := newDummyModule("some-module", modulecapabilities.Text2Vec)
		p.Register(vec)

		err := p.ValidateVectorizer(vec.Name())
		assert.Nil(t, err)
	})

	t.Run("with reference vectorizer module", func(t *testing.T) {
		p := NewProvider()
		refVec := newDummyModule("some-module", modulecapabilities.Ref2Vec)
		p.Register(refVec)

		err := p.ValidateVectorizer(refVec.Name())
		assert.Nil(t, err)
	})

	t.Run("with non-vectorizer module", func(t *testing.T) {
		modName := "some-module"
		p := NewProvider()
		nonVec := newDummyModule(modName, "")
		p.Register(nonVec)

		expectedErr := fmt.Sprintf(
			"module %q exists, but does not provide the Vectorizer or ReferenceVectorizer capability",
			modName)
		err := p.ValidateVectorizer(nonVec.Name())
		assert.EqualError(t, err, expectedErr)
	})

	t.Run("with unregistered module", func(t *testing.T) {
		modName := "does-not-exist"
		p := NewProvider()
		expectedErr := fmt.Sprintf(
			"no module with name %q present",
			modName)
		err := p.ValidateVectorizer(modName)
		assert.EqualError(t, err, expectedErr)
	})
}

func TestProvider_UsingRef2Vec(t *testing.T) {
	t.Run("with ReferenceVectorizer", func(t *testing.T) {
		modName := "some-module"
		mod := newDummyModule(modName, modulecapabilities.Ref2Vec)
		p := NewProvider()
		p.Register(mod)
		assert.True(t, p.UsingRef2Vec(modName))
	})

	t.Run("with Vectorizer", func(t *testing.T) {
		modName := "some-module"
		mod := newDummyModule(modName, modulecapabilities.Text2Vec)
		p := NewProvider()
		p.Register(mod)
		assert.False(t, p.UsingRef2Vec(modName))
	})

	t.Run("with unregistered module", func(t *testing.T) {
		modName := "some-module"
		p := NewProvider()
		assert.False(t, p.UsingRef2Vec(modName))
	})
}

func TestProvider_TargetReferenceProperties(t *testing.T) {
	moduleName := "some-mod"
	className := "SomeClass"

	sch := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:      className,
					Vectorizer: moduleName,
					ModuleConfig: map[string]interface{}{
						"mod": map[string]interface{}{
							"some-config": "some-config-value",
						},
					},
				},
			},
		},
	}

	t.Run("with non-ReferenceVectorizer", func(t *testing.T) {
		mod := newDummyModule(moduleName, modulecapabilities.Text2Vec)
		p := NewProvider()
		p.Register(mod)
		p.SetSchemaGetter(newFakeSchemaGetter(sch))

		_, err := p.TargetReferenceProperties(className)
		assert.EqualError(t, err, "no ref2vec module found")
	})

	t.Run("with non-existent class", func(t *testing.T) {
		dneClass := "DoesNotExist"

		mod := newDummyModule(moduleName, modulecapabilities.Ref2Vec)
		p := NewProvider()
		p.Register(mod)
		p.SetSchemaGetter(newFakeSchemaGetter(sch))

		_, err := p.TargetReferenceProperties(dneClass)
		expectedErr := fmt.Sprintf("class %q not found in schema", dneClass)
		assert.EqualError(t, err, expectedErr)
	})

	t.Run("expected success - ref2vec only", func(t *testing.T) {
		mod := newDummyModule(moduleName, modulecapabilities.Ref2Vec)
		p := NewProvider()
		p.Register(mod)
		p.SetSchemaGetter(newFakeSchemaGetter(sch))

		props, err := p.TargetReferenceProperties(className)
		assert.Nil(t, err)
		assert.NotNil(t, props)
	})

	t.Run("expected success - multiple module types", func(t *testing.T) {
		ref2Vec := newDummyModule(moduleName, modulecapabilities.Ref2Vec)
		text2Vec := newDummyModule("some-other-module", modulecapabilities.Text2Vec)

		p := NewProvider()
		p.Register(ref2Vec)
		p.Register(text2Vec)
		p.SetSchemaGetter(newFakeSchemaGetter(sch))

		props, err := p.TargetReferenceProperties(className)
		assert.Nil(t, err)
		assert.NotNil(t, props)
	})
}
