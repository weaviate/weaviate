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

package modules

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestProvider_ValidateVectorizer(t *testing.T) {
	logger, _ := test.NewNullLogger()
	t.Run("with vectorizer module", func(t *testing.T) {
		p := NewProvider(logger, config.Config{})
		vec := newDummyModule("some-module", modulecapabilities.Text2Vec)
		p.Register(vec)

		err := p.ValidateVectorizer(vec.Name())
		assert.Nil(t, err)
	})

	t.Run("with reference vectorizer module", func(t *testing.T) {
		p := NewProvider(logger, config.Config{})
		refVec := newDummyModule("some-module", modulecapabilities.Ref2Vec)
		p.Register(refVec)

		err := p.ValidateVectorizer(refVec.Name())
		assert.Nil(t, err)
	})

	t.Run("with non-vectorizer module", func(t *testing.T) {
		modName := "some-module"
		p := NewProvider(logger, config.Config{})
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
		p := NewProvider(logger, config.Config{})
		expectedErr := fmt.Sprintf(
			"no module with name %q present",
			modName)
		err := p.ValidateVectorizer(modName)
		assert.EqualError(t, err, expectedErr)
	})
}

func TestProvider_UsingRef2Vec(t *testing.T) {
	logger, _ := test.NewNullLogger()
	t.Run("with ReferenceVectorizer", func(t *testing.T) {
		modName := "some-module"
		className := "SomeClass"
		mod := newDummyModule(modName, modulecapabilities.Ref2Vec)
		sch := schema.Schema{Objects: &models.Schema{
			Classes: []*models.Class{{
				Class: className,
				ModuleConfig: map[string]interface{}{
					modName: struct{}{},
				},
			}},
		}}
		p := NewProvider(logger, config.Config{})
		p.SetSchemaGetter(&fakeSchemaGetter{sch})
		p.Register(mod)
		assert.True(t, p.UsingRef2Vec(className))
	})

	t.Run("with Vectorizer", func(t *testing.T) {
		modName := "some-module"
		className := "SomeClass"
		mod := newDummyModule(modName, modulecapabilities.Text2Vec)
		sch := schema.Schema{Objects: &models.Schema{
			Classes: []*models.Class{{
				Class: className,
				ModuleConfig: map[string]interface{}{
					modName: struct{}{},
				},
			}},
		}}
		p := NewProvider(logger, config.Config{})
		p.SetSchemaGetter(&fakeSchemaGetter{sch})
		p.Register(mod)
		assert.False(t, p.UsingRef2Vec(className))
	})

	t.Run("with nonexistent class", func(t *testing.T) {
		className := "SomeClass"
		mod := newDummyModule("", "")

		p := NewProvider(logger, config.Config{})
		p.SetSchemaGetter(&fakeSchemaGetter{schema.Schema{}})
		p.Register(mod)
		assert.False(t, p.UsingRef2Vec(className))
	})

	t.Run("with empty class module config", func(t *testing.T) {
		modName := "some-module"
		className := "SomeClass"
		mod := newDummyModule(modName, modulecapabilities.Text2Vec)
		sch := schema.Schema{Objects: &models.Schema{
			Classes: []*models.Class{{
				Class: className,
			}},
		}}
		p := NewProvider(logger, config.Config{})
		p.SetSchemaGetter(&fakeSchemaGetter{sch})
		p.Register(mod)
		assert.False(t, p.UsingRef2Vec(className))
	})

	t.Run("with unregistered module", func(t *testing.T) {
		modName := "some-module"
		className := "SomeClass"
		sch := schema.Schema{Objects: &models.Schema{
			Classes: []*models.Class{{
				Class: className,
				ModuleConfig: map[string]interface{}{
					modName: struct{}{},
				},
			}},
		}}
		p := NewProvider(logger, config.Config{})
		p.SetSchemaGetter(&fakeSchemaGetter{sch})
		assert.False(t, p.UsingRef2Vec(className))
	})
}

func TestProvider_UpdateVector(t *testing.T) {
	t.Run("with Vectorizer", func(t *testing.T) {
		ctx := context.Background()
		modName := "some-vzr"
		className := "SomeClass"
		mod := newDummyModule(modName, modulecapabilities.Text2Vec)
		class := models.Class{
			Class: className,
			ModuleConfig: map[string]interface{}{
				modName: map[string]interface{}{},
			},
			Vectorizer:        "text2vec-contextionary",
			VectorIndexConfig: hnsw.UserConfig{},
		}
		sch := schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{&class},
			},
		}
		repo := &fakeObjectsRepo{}
		logger, _ := test.NewNullLogger()

		p := NewProvider(logger, config.Config{})
		p.Register(mod)
		p.SetSchemaGetter(&fakeSchemaGetter{sch})

		obj := &models.Object{Class: className, ID: newUUID()}
		err := p.UpdateVector(ctx, obj, &class, repo.Object, logger)
		assert.Nil(t, err)
	})

	t.Run("with missing vectorizer modconfig", func(t *testing.T) {
		ctx := context.Background()
		class := &models.Class{
			Class:             "SomeClass",
			VectorIndexConfig: hnsw.UserConfig{},
			Vectorizer:        "text2vec-contextionary",
		}
		mod := newDummyModule("", "")
		logger, _ := test.NewNullLogger()

		p := NewProvider(logger, config.Config{})
		p.Register(mod)
		p.SetSchemaGetter(&fakeSchemaGetter{schema.Schema{}})

		obj := &models.Object{Class: class.Class, ID: newUUID()}
		err := p.UpdateVector(ctx, obj, class, (&fakeObjectsRepo{}).Object, logger)
		expectedErr := fmt.Sprintf("no moduleconfig for class %v present", class.Class)
		assert.EqualError(t, err, expectedErr)
	})

	t.Run("with no vectors configuration", func(t *testing.T) {
		ctx := context.Background()
		class := &models.Class{
			Class:      "SomeClass",
			Vectorizer: "none",
		}

		logger, _ := test.NewNullLogger()
		p := NewProvider(logger, config.Config{})

		obj := &models.Object{Class: class.Class, ID: newUUID()}
		err := p.UpdateVector(ctx, obj, class, (&fakeObjectsRepo{}).Object, logger)
		require.NoError(t, err)
	})

	t.Run("with ReferenceVectorizer", func(t *testing.T) {
		ctx := context.Background()
		modName := "some-vzr"
		className := "SomeClass"
		mod := newDummyModule(modName, modulecapabilities.Ref2Vec)
		class := &models.Class{
			Class: className,
			ModuleConfig: map[string]interface{}{
				modName: struct{}{},
			},
			Vectorizer:        "text2vec-contextionary",
			VectorIndexConfig: hnsw.UserConfig{},
		}

		sch := schema.Schema{Objects: &models.Schema{
			Classes: []*models.Class{class},
		}}
		repo := &fakeObjectsRepo{}
		logger, _ := test.NewNullLogger()

		p := NewProvider(logger, config.Config{})
		p.Register(mod)
		p.SetSchemaGetter(&fakeSchemaGetter{sch})

		obj := &models.Object{Class: className, ID: newUUID()}
		err := p.UpdateVector(ctx, obj, class, repo.Object, logger)
		assert.Nil(t, err)
	})

	t.Run("with nonexistent vector index config type", func(t *testing.T) {
		ctx := context.Background()
		modName := "some-vzr"
		className := "SomeClass"
		mod := newDummyModule(modName, modulecapabilities.Ref2Vec)
		class := &models.Class{
			Class: className,
			ModuleConfig: map[string]interface{}{
				modName: struct{}{},
			},
			Vectorizer:        "text2vec-contextionary",
			VectorIndexConfig: struct{}{},
		}
		sch := schema.Schema{Objects: &models.Schema{
			Classes: []*models.Class{class},
		}}
		repo := &fakeObjectsRepo{}
		logger, _ := test.NewNullLogger()

		p := NewProvider(logger, config.Config{})
		p.Register(mod)
		p.SetSchemaGetter(&fakeSchemaGetter{sch})

		obj := &models.Object{Class: className, ID: newUUID()}

		err := p.UpdateVector(ctx, obj, class, repo.Object, logger)
		expectedErr := "vector index config (struct {}) is not of type HNSW, " +
			"but objects manager is restricted to HNSW"
		require.ErrorContains(t, err, expectedErr)
	})

	t.Run("with ColBERT Vectorizer", func(t *testing.T) {
		ctx := context.Background()
		modName := "colbert"
		className := "SomeClass"
		mod := newDummyModule(modName, modulecapabilities.Text2Multivec)
		class := models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				"colbert": {
					Vectorizer:        map[string]interface{}{modName: map[string]interface{}{}},
					VectorIndexConfig: hnsw.UserConfig{Multivector: hnsw.MultivectorConfig{Enabled: true}},
					VectorIndexType:   "hnsw",
				},
			},
		}
		sch := schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{&class},
			},
		}
		repo := &fakeObjectsRepo{}
		logger, _ := test.NewNullLogger()

		p := NewProvider(logger, config.Config{})
		p.Register(mod)
		p.SetSchemaGetter(&fakeSchemaGetter{sch})

		obj := &models.Object{Class: className, ID: newUUID()}
		err := p.UpdateVector(ctx, obj, &class, repo.Object, logger)
		assert.NoError(t, err)
		assert.NotEmpty(t, obj.Vectors)
		assert.Equal(t, [][]float32{{0.11, 0.22, 0.33}, {0.11, 0.22, 0.33}}, obj.Vectors["colbert"])
	})
}

func newUUID() strfmt.UUID {
	return strfmt.UUID(uuid.NewString())
}
