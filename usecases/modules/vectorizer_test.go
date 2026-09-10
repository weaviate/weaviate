//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
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

// moduleVectorEntry builds a named-vector config backed by the modName
// vectorizer; a dropped entry carries the mid-drop marker (VectorIndexType
// "none", no index config).
func moduleVectorEntry(modName string, dropped bool) models.VectorConfig {
	entry := models.VectorConfig{
		Vectorizer:      map[string]interface{}{modName: map[string]interface{}{}},
		VectorIndexType: "none",
	}
	if !dropped {
		entry.VectorIndexType = "hnsw"
		entry.VectorIndexConfig = hnsw.UserConfig{}
	}
	return entry
}

// droppedAndLiveVectorClass builds a class whose "dropped" named vector is
// mid-drop while "live" still has its index, both backed by the same module.
func droppedAndLiveVectorClass(className, modName string) *models.Class {
	return &models.Class{
		Class: className,
		VectorConfig: map[string]models.VectorConfig{
			"dropped": moduleVectorEntry(modName, true),
			"live":    moduleVectorEntry(modName, false),
		},
	}
}

// dropVectorTestProvider serves class through the schema getter with a dummy
// text2vec module registered under modName.
func dropVectorTestProvider(class *models.Class, modName string) (*Provider, *logrus.Logger) {
	logger, _ := test.NewNullLogger()
	p := NewProvider(logger, config.Config{})
	p.Register(newDummyModule(modName, modulecapabilities.Text2Vec))
	sch := schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}
	p.SetSchemaGetter(&fakeSchemaGetter{sch})
	return p, logger
}

func TestProvider_BatchUpdateVector(t *testing.T) {
	t.Run("module vectorizer skips a mid-drop named vector", func(t *testing.T) {
		// Regression for weaviate/0-weaviate-issues#481: a mid-drop named
		// vector keeps its vectorizer config, but its index and queue are gone
		// and the shard put rejects any object carrying the dropped vector.
		// Computing it here would therefore fail every write to the collection
		// until the drop finalizes. Single-object twin in TestProvider_UpdateVector.
		class := droppedAndLiveVectorClass("SomeClass", "some-vzr")
		p, logger := dropVectorTestProvider(class, "some-vzr")

		objs := []*models.Object{
			{Class: class.Class, ID: newUUID()},
			{Class: class.Class, ID: newUUID()},
		}
		vecErrs, err := p.BatchUpdateVector(context.Background(), class, objs, (&fakeObjectsRepo{}).Object, logger)
		require.NoError(t, err)
		require.Empty(t, vecErrs)
		for i, obj := range objs {
			assert.NotContains(t, obj.Vectors, "dropped",
				"object %d: mid-drop target must not be vectorized — the write path rejects objects carrying it", i)
			assert.Contains(t, obj.Vectors, "live", "object %d missing the live vector", i)
		}
	})

	t.Run("last remaining vector mid-drop is a no-op, not an error", func(t *testing.T) {
		// Filtering the only named vector leaves modConfigs empty, which must
		// take the vector-less short-circuit (class.Vectorizer is "" for
		// named-vector classes), not the "no vectorizer configs" error.
		class := &models.Class{
			Class: "SomeClass",
			VectorConfig: map[string]models.VectorConfig{
				"dropped": moduleVectorEntry("some-vzr", true),
			},
		}
		p, logger := dropVectorTestProvider(class, "some-vzr")

		objs := []*models.Object{{Class: class.Class, ID: newUUID()}}
		vecErrs, err := p.BatchUpdateVector(context.Background(), class, objs, (&fakeObjectsRepo{}).Object, logger)
		require.NoError(t, err)
		require.Empty(t, vecErrs)
		assert.Empty(t, objs[0].Vectors)

		obj := &models.Object{Class: class.Class, ID: newUUID()}
		require.NoError(t, p.UpdateVector(context.Background(), obj, class, (&fakeObjectsRepo{}).Object, logger))
		assert.Empty(t, obj.Vectors)
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

	t.Run("with a dropped named vector index", func(t *testing.T) {
		// Regression for #11917: dropping a named vector index leaves the vector
		// in the schema with VectorIndexType "none" and a nil index config. A
		// write must not be validated against the removed HNSW index.
		className := "DropVectorBug"
		dropped := models.VectorConfig{
			Vectorizer:        map[string]interface{}{"none": map[string]interface{}{}},
			VectorIndexType:   "none",
			VectorIndexConfig: nil,
		}
		live := models.VectorConfig{
			Vectorizer:        map[string]interface{}{"none": map[string]interface{}{}},
			VectorIndexType:   "hnsw",
			VectorIndexConfig: hnsw.UserConfig{},
		}

		tests := []struct {
			name         string
			vectorConfig map[string]models.VectorConfig
			object       *models.Object
		}{
			{
				name:         "write without the dropped vector succeeds",
				vectorConfig: map[string]models.VectorConfig{"foo": dropped},
				object:       &models.Object{Class: className},
			},
			{
				name:         "write that still carries the dropped vector succeeds",
				vectorConfig: map[string]models.VectorConfig{"foo": dropped},
				object:       &models.Object{Class: className, Vectors: models.Vectors{"foo": []float32{0.1, 0.2}}},
			},
			{
				name:         "write succeeds when a dropped index coexists with a live one",
				vectorConfig: map[string]models.VectorConfig{"foo": dropped, "bar": live},
				object:       &models.Object{Class: className},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctx := context.Background()
				class := &models.Class{Class: className, VectorConfig: tt.vectorConfig}
				sch := schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}
				logger, _ := test.NewNullLogger()

				p := NewProvider(logger, config.Config{})
				p.SetSchemaGetter(&fakeSchemaGetter{sch})

				tt.object.ID = newUUID()
				err := p.UpdateVector(ctx, tt.object, class, (&fakeObjectsRepo{}).Object, logger)
				require.NoError(t, err)
			})
		}
	})

	t.Run("module vectorizer skips a mid-drop named vector", func(t *testing.T) {
		// Regression for weaviate/0-weaviate-issues#481 — see
		// TestProvider_BatchUpdateVector for the batch twin.
		class := droppedAndLiveVectorClass("SomeClass", "some-vzr")
		p, logger := dropVectorTestProvider(class, "some-vzr")

		obj := &models.Object{Class: class.Class, ID: newUUID()}
		err := p.UpdateVector(context.Background(), obj, class, (&fakeObjectsRepo{}).Object, logger)
		require.NoError(t, err)
		assert.NotContains(t, obj.Vectors, "dropped",
			"mid-drop target must not be vectorized — the write path rejects objects carrying it")
		assert.Contains(t, obj.Vectors, "live")
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

// countingText2VecModule / countingText2ColBERTModule count how often the
// embedding model is called; in batches, skipped objects do not count.

type countingText2VecModule struct {
	dummyText2VecModuleNoCapabilities
	calls *int
}

func (m *countingText2VecModule) VectorizeObject(ctx context.Context,
	in *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	*m.calls++
	return []float32{9, 9, 9}, nil, nil
}

func (m *countingText2VecModule) VectorizeBatch(ctx context.Context,
	objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig,
) ([][]float32, []models.AdditionalProperties, map[int]error) {
	vecs := make([][]float32, len(objs))
	for i := range objs {
		if !skipObject[i] {
			*m.calls++
		}
		vecs[i] = []float32{9, 9, 9}
	}
	return vecs, nil, map[int]error{}
}

type countingText2ColBERTModule struct {
	dummyText2ColBERTModuleNoCapabilities
	calls *int
}

func (m *countingText2ColBERTModule) VectorizeObject(ctx context.Context,
	in *models.Object, cfg moduletools.ClassConfig,
) ([][]float32, models.AdditionalProperties, error) {
	*m.calls++
	return [][]float32{{9, 9, 9}, {9, 9, 9}}, nil, nil
}

func (m *countingText2ColBERTModule) VectorizeBatch(ctx context.Context,
	objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig,
) ([][][]float32, []models.AdditionalProperties, map[int]error) {
	vecs := make([][][]float32, len(objs))
	for i := range objs {
		if !skipObject[i] {
			*m.calls++
		}
		vecs[i] = [][]float32{{9, 9, 9}, {9, 9, 9}}
	}
	return vecs, nil, map[int]error{}
}

func newCountingProvider(moduleName string, multiVector bool) (*Provider, *int) {
	logger, _ := test.NewNullLogger()
	p := NewProvider(logger, config.Config{
		RevectorizeCheckDisabled: configRuntime.NewDynamicValue(false),
	})
	calls := 0
	if multiVector {
		p.Register(&countingText2ColBERTModule{
			dummyText2ColBERTModuleNoCapabilities: newDummyText2ColBERTModule(moduleName, nil),
			calls:                                 &calls,
		})
	} else {
		p.Register(&countingText2VecModule{
			dummyText2VecModuleNoCapabilities: newDummyText2VecModule(moduleName, nil),
			calls:                             &calls,
		})
	}
	return p, &calls
}

// newNamedVectorClass builds a "Products" class whose only vector is the named
// vector targetVector, vectorized by moduleName with the given source properties.
func newNamedVectorClass(moduleName, targetVector string, sourceProperties any,
	props ...*models.Property,
) *models.Class {
	return &models.Class{
		Class:      "Products",
		Vectorizer: config.VectorizerModuleNone, // no legacy vector; only the named vector
		Properties: props,
		VectorConfig: map[string]models.VectorConfig{
			targetVector: {
				Vectorizer: map[string]any{
					moduleName: map[string]any{
						"vectorizeClassName": false,
						"properties":         sourceProperties,
					},
				},
				VectorIndexConfig: hnsw.UserConfig{},
				VectorIndexType:   "hnsw",
			},
		},
	}
}

func newSourcePropsTestClass(moduleName, targetVector string, sourceProperties any) *models.Class {
	return newNamedVectorClass(moduleName, targetVector, sourceProperties,
		&models.Property{Name: "vector_input", DataType: []string{schema.DataTypeText.String()}},
		&models.Property{Name: "delivery_label", DataType: []string{schema.DataTypeText.String()}},
	)
}

func staticFindObject(targetVector string, oldProps map[string]any, oldVector models.Vector) modulecapabilities.FindObjectFn {
	return func(ctx context.Context, className string, oid strfmt.UUID,
		props search.SelectProperties, adds additional.Properties, tenant string,
	) (*search.Result, error) {
		return &search.Result{
			Schema:  oldProps,
			Vectors: models.Vectors{targetVector: oldVector},
		}, nil
	}
}

func sourcePropsTestVectors(multiVector bool) (stored, recomputed models.Vector) {
	if multiVector {
		return [][]float32{{1, 2, 3}, {1, 2, 3}}, [][]float32{{9, 9, 9}, {9, 9, 9}}
	}
	return []float32{1, 2, 3}, []float32{9, 9, 9}
}

func mergedPropsForChange(changedProp string) map[string]any {
	props := map[string]any{"vector_input": "embed me", "delivery_label": "1 day"}
	switch changedProp {
	case "delivery_label":
		props["delivery_label"] = "2 days"
	case "vector_input":
		props["vector_input"] = "embed me differently"
	}
	return props
}

func runUpdateVector(t *testing.T, p *Provider, class *models.Class, obj *models.Object,
	findObject modulecapabilities.FindObjectFn, batch bool,
) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	if batch {
		vecErrors, err := p.BatchUpdateVector(context.Background(), class, []*models.Object{obj}, findObject, logger)
		require.NoError(t, err)
		require.Empty(t, vecErrors)
		return
	}
	require.NoError(t, p.UpdateVector(context.Background(), obj, class, findObject, logger))
}

func assertStoredVector(t *testing.T, obj *models.Object, targetVector string, multiVector, revectorized bool) {
	t.Helper()
	stored, recomputed := sourcePropsTestVectors(multiVector)
	want := stored
	if revectorized {
		want = recomputed
	}
	require.Equal(t, want, obj.Vectors[targetVector])
}

// TestUpdateVector_RespectsNamedVectorSourceProperties is a regression test for
// https://github.com/weaviate/weaviate/issues/11781: a partial update must only
// re-vectorize when a configured source_property actually changed.
func TestUpdateVector_RespectsNamedVectorSourceProperties(t *testing.T) {
	const targetVector = "vector_input"
	const moduleName = "my-module"

	journeys := []struct {
		name        string
		multiVector bool
		batch       bool
	}{
		{name: "single-object/regular-vector", multiVector: false, batch: false},
		{name: "single-object/multi-vector", multiVector: true, batch: false},
		{name: "batch/regular-vector", multiVector: false, batch: true},
		{name: "batch/multi-vector", multiVector: true, batch: true},
	}

	cases := []struct {
		name               string
		sourceProperties   any
		changedProp        string
		wantVectorizeCalls int
	}{
		{
			name:               "[]any source props; change NON-source prop -> skip",
			sourceProperties:   []any{"vector_input"},
			changedProp:        "delivery_label",
			wantVectorizeCalls: 0,
		},
		{
			name:               "[]any source props; change SOURCE prop -> re-vectorize",
			sourceProperties:   []any{"vector_input"},
			changedProp:        "vector_input",
			wantVectorizeCalls: 1,
		},
		{
			name:               "[]string source props; change NON-source prop -> skip",
			sourceProperties:   []string{"vector_input"},
			changedProp:        "delivery_label",
			wantVectorizeCalls: 0,
		},
		{
			// an empty list means no source properties, so all text props are compared.
			name:               "empty source props; change a text prop -> re-vectorize",
			sourceProperties:   []any{},
			changedProp:        "delivery_label",
			wantVectorizeCalls: 1,
		},
	}

	for _, j := range journeys {
		for _, tc := range cases {
			t.Run(j.name+"/"+tc.name, func(t *testing.T) {
				p, calls := newCountingProvider(moduleName, j.multiVector)
				class := newSourcePropsTestClass(moduleName, targetVector, tc.sourceProperties)
				oldProps := map[string]any{"vector_input": "embed me", "delivery_label": "1 day"}
				storedVector, _ := sourcePropsTestVectors(j.multiVector)

				obj := &models.Object{
					Class:      class.Class,
					ID:         newUUID(),
					Properties: mergedPropsForChange(tc.changedProp),
					Vectors:    models.Vectors{},
				}
				findObject := staticFindObject(targetVector, oldProps, storedVector)

				runUpdateVector(t, p, class, obj, findObject, j.batch)

				require.Equalf(t, tc.wantVectorizeCalls, *calls,
					"unexpected number of embedding-model invocations")
				assertStoredVector(t, obj, targetVector, j.multiVector, tc.wantVectorizeCalls > 0)
			})
		}
	}
}

// TestBatchUpdateVector_MixedSkipAndRevectorize: in one batch, the changed-source
// object must re-vectorize while the unchanged one keeps its vector — checks that
// each object gets its own result (a 1-object batch can't catch this).
func TestBatchUpdateVector_MixedSkipAndRevectorize(t *testing.T) {
	logger, _ := test.NewNullLogger()
	const targetVector = "vector_input"
	const moduleName = "my-module"

	p, calls := newCountingProvider(moduleName, false)
	class := newSourcePropsTestClass(moduleName, targetVector, []any{"vector_input"})

	changedID := newUUID()   // source property changes -> must re-vectorize
	unchangedID := newUUID() // only a non-source property changes -> must skip

	oldProps := map[strfmt.UUID]map[string]any{
		changedID:   {"vector_input": "A old", "delivery_label": "x"},
		unchangedID: {"vector_input": "B keep", "delivery_label": "x"},
	}
	oldVecs := map[strfmt.UUID][]float32{
		changedID:   {1, 1, 1},
		unchangedID: {2, 2, 2},
	}
	findObject := func(ctx context.Context, className string, oid strfmt.UUID,
		props search.SelectProperties, adds additional.Properties, tenant string,
	) (*search.Result, error) {
		return &search.Result{
			Schema:  oldProps[oid],
			Vectors: models.Vectors{targetVector: oldVecs[oid]},
		}, nil
	}

	objChanged := &models.Object{
		Class: class.Class, ID: changedID, Vectors: models.Vectors{},
		Properties: map[string]any{"vector_input": "A NEW", "delivery_label": "x"},
	}
	objUnchanged := &models.Object{
		Class: class.Class, ID: unchangedID, Vectors: models.Vectors{},
		Properties: map[string]any{"vector_input": "B keep", "delivery_label": "y changed"},
	}

	vecErrors, err := p.BatchUpdateVector(context.Background(), class,
		[]*models.Object{objChanged, objUnchanged}, findObject, logger)
	require.NoError(t, err)
	require.Empty(t, vecErrors)

	require.Equal(t, 1, *calls,
		"only the object whose source property changed should be re-vectorized")
	require.Equal(t, []float32{9, 9, 9}, objChanged.Vectors[targetVector],
		"changed-source object must get the freshly computed vector")
	require.Equal(t, []float32{2, 2, 2}, objUnchanged.Vectors[targetVector],
		"unchanged-source object must keep its stored vector (correct result-to-object mapping)")
}

// TestUpdateVector_BlobHashSourceProperty: one blobHash smoke case through the
// provider — sent to the model as base64 but stored as a hash, so an unchanged
// payload must not re-vectorize. Value coverage lives in compare_test.go.
func TestUpdateVector_BlobHashSourceProperty(t *testing.T) {
	const targetVector = "vec"
	const moduleName = "my-module"
	const base64A = "QQ=="
	storedHash := schema.HashBlob(base64A)

	class := newNamedVectorClass(moduleName, targetVector, []any{"thumbnail"},
		&models.Property{Name: "thumbnail", DataType: []string{schema.DataTypeBlobHash.String()}},
		&models.Property{Name: "label", DataType: []string{schema.DataTypeText.String()}},
	)

	for _, batch := range []bool{false, true} {
		mode := "single-object"
		if batch {
			mode = "batch"
		}
		t.Run(mode+"/unchanged base64 -> skip", func(t *testing.T) {
			p, calls := newCountingProvider(moduleName, false)
			findObject := staticFindObject(targetVector,
				map[string]any{"thumbnail": storedHash, "label": "x"}, []float32{1, 2, 3})
			obj := &models.Object{
				Class: class.Class, ID: newUUID(), Vectors: models.Vectors{},
				Properties: map[string]any{"thumbnail": base64A, "label": "x"},
			}
			runUpdateVector(t, p, class, obj, findObject, batch)
			require.Equal(t, 0, *calls)
		})
	}
}
