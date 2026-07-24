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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
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

// countingText2VecModule / countingText2ColBERTModule count embedding-model calls in
// both the single-object and batch (honoring skipObject) paths.

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

func newCountingProvider(moduleName string, multiVector, revectorizeCheckDisabled bool) (*Provider, *int) {
	logger, _ := test.NewNullLogger()
	p := NewProvider(logger, config.Config{
		RevectorizeCheckDisabled: configRuntime.NewDynamicValue(revectorizeCheckDisabled),
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

func newSourcePropsTestClass(moduleName, targetVector string, sourceProperties any) *models.Class {
	return &models.Class{
		Class:      "Products",
		Vectorizer: config.VectorizerModuleNone, // no legacy vector; only the named vector
		Properties: []*models.Property{
			{Name: "vector_input", DataType: []string{schema.DataTypeText.String()}},
			{Name: "delivery_label", DataType: []string{schema.DataTypeText.String()}},
		},
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
			name:               "[]interface{} source props; change NON-source prop -> skip",
			sourceProperties:   []any{"vector_input"},
			changedProp:        "delivery_label",
			wantVectorizeCalls: 0,
		},
		{
			name:               "[]interface{} source props; change SOURCE prop -> re-vectorize",
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
			// empty source props => no explicit list => all text props compared.
			name:               "empty source props; change a text prop -> re-vectorize",
			sourceProperties:   []any{},
			changedProp:        "delivery_label",
			wantVectorizeCalls: 1,
		},
	}

	for _, j := range journeys {
		for _, tc := range cases {
			t.Run(j.name+"/"+tc.name, func(t *testing.T) {
				p, calls := newCountingProvider(moduleName, j.multiVector, false)
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
// object must re-vectorize while the unchanged one keeps its vector — checks the
// per-object skip and result-to-object mapping (a 1-object batch can't catch this).
func TestBatchUpdateVector_MixedSkipAndRevectorize(t *testing.T) {
	logger, _ := test.NewNullLogger()
	const targetVector = "vector_input"
	const moduleName = "my-module"

	p, calls := newCountingProvider(moduleName, false, false)
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

// TestUpdateVector_RevectorizeCheckDisabled_AlwaysVectorizes: with the check
// disabled, an update re-vectorizes even when only a non-source property changed.
func TestUpdateVector_RevectorizeCheckDisabled_AlwaysVectorizes(t *testing.T) {
	logger, _ := test.NewNullLogger()
	const targetVector = "vector_input"
	const moduleName = "my-module"

	p, calls := newCountingProvider(moduleName, false, true)
	class := newSourcePropsTestClass(moduleName, targetVector, []any{"vector_input"})

	findObject := staticFindObject(targetVector,
		map[string]any{"vector_input": "embed me", "delivery_label": "1 day"},
		[]float32{1, 2, 3})

	obj := &models.Object{
		Class: class.Class, ID: newUUID(), Vectors: models.Vectors{},
		Properties: map[string]any{"vector_input": "embed me", "delivery_label": "2 days"},
	}

	require.NoError(t, p.UpdateVector(context.Background(), obj, class, findObject, logger))
	require.Equal(t, 1, *calls,
		"with RevectorizeCheckDisabled=true, updates must always re-vectorize")
}

// TestUpdateVector_NonTextSourcePropertyEndToEnd: through Provider.UpdateVector,
// a changed numeric source property re-vectorizes; a non-source change does not.
func TestUpdateVector_NonTextSourcePropertyEndToEnd(t *testing.T) {
	logger, _ := test.NewNullLogger()
	const targetVector = "vec"
	const moduleName = "my-module"

	newClass := func() *models.Class {
		return &models.Class{
			Class:      "Products",
			Vectorizer: config.VectorizerModuleNone,
			Properties: []*models.Property{
				{Name: "price", DataType: []string{schema.DataTypeNumber.String()}},
				{Name: "label", DataType: []string{schema.DataTypeText.String()}},
			},
			VectorConfig: map[string]models.VectorConfig{
				targetVector: {
					Vectorizer: map[string]any{
						moduleName: map[string]any{
							"vectorizeClassName": false,
							"properties":         []any{"price"},
						},
					},
					VectorIndexConfig: hnsw.UserConfig{},
					VectorIndexType:   "hnsw",
				},
			},
		}
	}

	cases := []struct {
		name      string
		merged    map[string]any
		wantCalls int
	}{
		{name: "source (price) changed -> re-vectorize", merged: map[string]any{"price": 19.99, "label": "x"}, wantCalls: 1},
		{name: "non-source (label) changed -> skip", merged: map[string]any{"price": 9.99, "label": "y"}, wantCalls: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, calls := newCountingProvider(moduleName, false, false)
			class := newClass()
			findObject := staticFindObject(targetVector,
				map[string]any{"price": 9.99, "label": "x"}, []float32{1, 2, 3})
			obj := &models.Object{
				Class: class.Class, ID: newUUID(), Properties: tc.merged, Vectors: models.Vectors{},
			}
			require.NoError(t, p.UpdateVector(context.Background(), obj, class, findObject, logger))
			require.Equal(t, tc.wantCalls, *calls)
		})
	}
}

// TestBatchUpdateVector_SubSecondDateSourceProperty: on the batch path a date stored
// as an RFC3339Nano string vs supplied as time.Time must not re-vectorize the same instant.
func TestBatchUpdateVector_SubSecondDateSourceProperty(t *testing.T) {
	logger, _ := test.NewNullLogger()
	const targetVector = "vec"
	const moduleName = "my-module"

	newClass := func() *models.Class {
		return &models.Class{
			Class:      "Products",
			Vectorizer: config.VectorizerModuleNone,
			Properties: []*models.Property{
				{Name: "released", DataType: []string{schema.DataTypeDate.String()}},
				{Name: "label", DataType: []string{schema.DataTypeText.String()}},
			},
			VectorConfig: map[string]models.VectorConfig{
				targetVector: {
					Vectorizer: map[string]any{
						moduleName: map[string]any{
							"vectorizeClassName": false,
							"properties":         []any{"released"},
						},
					},
					VectorIndexConfig: hnsw.UserConfig{},
					VectorIndexType:   "hnsw",
				},
			},
		}
	}

	cases := []struct {
		name      string
		diskDate  any // RFC3339(Nano) string, as read from disk
		newDate   any // time.Time, as produced by validation on the update
		wantCalls int
	}{
		{name: "same instant, microsecond string vs time.Time -> skip", diskDate: "2024-01-01T12:30:45.123456Z", newDate: time.Date(2024, 1, 1, 12, 30, 45, 123456000, time.UTC), wantCalls: 0},
		{name: "same instant, millisecond string vs time.Time -> skip", diskDate: "2024-01-01T12:30:45.123Z", newDate: time.Date(2024, 1, 1, 12, 30, 45, 123000000, time.UTC), wantCalls: 0},
		{name: "changed second -> re-vectorize", diskDate: "2024-01-01T12:30:45.123456Z", newDate: time.Date(2024, 1, 1, 12, 30, 46, 0, time.UTC), wantCalls: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, calls := newCountingProvider(moduleName, false, false)
			class := newClass()
			findObject := staticFindObject(targetVector,
				map[string]any{"released": tc.diskDate, "label": "x"}, []float32{1, 2, 3})
			obj := &models.Object{
				Class: class.Class, ID: newUUID(), Vectors: models.Vectors{},
				Properties: map[string]any{"released": tc.newDate, "label": "x"},
			}
			vecErrors, err := p.BatchUpdateVector(context.Background(), class, []*models.Object{obj}, findObject, logger)
			require.NoError(t, err)
			require.Empty(t, vecErrors)
			require.Equal(t, tc.wantCalls, *calls)
		})
	}
}
