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

package text2vecbase

import (
	"context"
	"errors"
	"fmt"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/batch"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
	"github.com/weaviate/weaviate/usecases/modules"
)

func TestBatchTokenizerWithEmptyProperties(t *testing.T) {
	className := "TestClass"
	tests := []struct {
		name                   string
		vectorizableProperties []string
		objects                []*models.Object
		wantTexts              []string
		wantSkipObjects        []bool
	}{
		{
			name:                   "all objects vectorizable",
			vectorizableProperties: []string{"text_prop"},
			objects: []*models.Object{
				{Class: className, Properties: map[string]any{"text_prop": "a"}},
				{Class: className, Properties: map[string]any{"text_prop": "b"}},
				{Class: className, Properties: map[string]any{"text_prop": "c"}},
			},
			wantTexts:       []string{"a", "b", "c"},
			wantSkipObjects: []bool{false, false, false},
		},
		{
			name:                   "one empty",
			vectorizableProperties: []string{"text_prop"},
			objects: []*models.Object{
				{Class: className, Properties: map[string]any{"text_prop": "a"}},
				{Class: className, Properties: map[string]any{"text_prop": ""}},
				{Class: className, Properties: map[string]any{"text_prop": "c"}},
			},
			wantTexts:       []string{"a", "", "c"},
			wantSkipObjects: []bool{false, true, false},
		},
		{
			name:                   "all empty",
			vectorizableProperties: []string{"text_prop"},
			objects: []*models.Object{
				{Class: className, Properties: map[string]any{"text_prop": ""}},
				{Class: className, Properties: map[string]any{"text_prop": ""}},
				{Class: className, Properties: map[string]any{"text_prop": ""}},
			},
			wantTexts:       []string{"", "", ""},
			wantSkipObjects: []bool{true, true, true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			targetVector := "targetVector"
			class := &models.Class{
				Class: className,
				VectorConfig: map[string]models.VectorConfig{
					targetVector: {
						Vectorizer: map[string]any{
							"my-module": map[string]any{
								"vectorizeClassName": false,
								"properties":         tt.vectorizableProperties,
							},
						},
						VectorIndexType: "hnsw",
					},
				},
			}
			cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant", targetVector, nil)
			tokenizer := batch.ReturnBatchTokenizer(1, "", false)
			vectorizer := objectsvectorizer.New()
			logger, _ := logrustest.NewNullLogger()
			encoderCache := batch.NewEncoderCache(logger)
			skipObjects := make([]bool, len(tt.objects))
			texts, tokenCounts, skippedObjects, skipAll, err := tokenizer(context.TODO(), tt.objects, skipObjects, cfg, vectorizer, encoderCache)
			require.NoError(t, err)
			require.False(t, skipAll)
			assert.Equal(t, len(texts), len(tokenCounts))
			assert.Equal(t, tt.wantTexts, texts)
			assert.Equal(t, tt.wantSkipObjects, skippedObjects)
		})
	}
}

func TestBatchVectorizerInvalidClientResult(t *testing.T) {
	vectorTests := []struct {
		name    string
		res     *modulecomponents.VectorizationResult[[]float32]
		inputs  int
		wantErr string
	}{
		{name: "nil result", res: nil, inputs: 1, wantErr: "empty embeddings in vectorizer response"},
		{name: "nil vectors", res: &modulecomponents.VectorizationResult[[]float32]{Vector: nil}, inputs: 1, wantErr: "empty embeddings in vectorizer response"},
		{name: "no vectors", res: &modulecomponents.VectorizationResult[[]float32]{Vector: [][]float32{}}, inputs: 1, wantErr: "empty embeddings in vectorizer response"},
		{name: "empty vector", res: &modulecomponents.VectorizationResult[[]float32]{Vector: [][]float32{{}}}, inputs: 1, wantErr: "empty embedding for input 0"},
		{
			name:    "error for one input",
			res:     &modulecomponents.VectorizationResult[[]float32]{Vector: [][]float32{nil}, Errors: []error{errors.New("input too long")}},
			inputs:  1,
			wantErr: "input too long",
		},
		{
			name: "error for second input",
			res: &modulecomponents.VectorizationResult[[]float32]{
				Vector: [][]float32{{0.1, 0.2}, nil},
				Errors: []error{nil, errors.New("input too long")},
			},
			inputs:  2,
			wantErr: "input too long",
		},
		{
			name:    "fewer vectors than inputs",
			res:     &modulecomponents.VectorizationResult[[]float32]{Vector: [][]float32{{0.1, 0.2}, {0.3, 0.4}}},
			inputs:  3,
			wantErr: "vectorizer returned 2 embeddings for 3 inputs",
		},
		{
			name:    "more vectors than inputs",
			res:     &modulecomponents.VectorizationResult[[]float32]{Vector: [][]float32{{1, 2}, {3, 4}}},
			inputs:  1,
			wantErr: "vectorizer returned 2 embeddings for 1 inputs",
		},
	}
	for _, tt := range vectorTests {
		t.Run(tt.name, func(t *testing.T) {
			assertVectorizerFails(t, tt.res, tt.inputs, tt.wantErr)
		})
	}

	multiVectorTests := []struct {
		name    string
		res     *modulecomponents.VectorizationResult[[][]float32]
		inputs  int
		wantErr string
	}{
		{name: "multi-vector: nil vectors", res: &modulecomponents.VectorizationResult[[][]float32]{Vector: nil}, inputs: 1, wantErr: "empty embeddings in vectorizer response"},
		{
			name:    "multi-vector: more than one multi-vector",
			res:     &modulecomponents.VectorizationResult[[][]float32]{Vector: [][][]float32{{{0.1, 0.2}}, {{0.3, 0.4}}}},
			inputs:  2,
			wantErr: "cannot combine 2 multi-vector embeddings",
		},
		{name: "multi-vector: empty token vector", res: &modulecomponents.VectorizationResult[[][]float32]{Vector: [][][]float32{{{}}}}, inputs: 1, wantErr: "empty embedding for input 0"},
		{name: "multi-vector: nil token vector", res: &modulecomponents.VectorizationResult[[][]float32]{Vector: [][][]float32{{nil}}}, inputs: 1, wantErr: "empty embedding for input 0"},
		{name: "multi-vector: empty token vector after a valid one", res: &modulecomponents.VectorizationResult[[][]float32]{Vector: [][][]float32{{{0.1, 0.2}, {}}}}, inputs: 1, wantErr: "empty embedding for input 0"},
	}
	for _, tt := range multiVectorTests {
		t.Run(tt.name, func(t *testing.T) {
			assertVectorizerFails(t, tt.res, tt.inputs, tt.wantErr)
		})
	}
}

func TestBatchVectorizerValidClientResult(t *testing.T) {
	tests := []struct {
		name   string
		res    *modulecomponents.VectorizationResult[[]float32]
		inputs int
		want   []float32
	}{
		{
			name:   "one vector for one input",
			res:    &modulecomponents.VectorizationResult[[]float32]{Vector: [][]float32{{0.1, 0.2}}, Errors: []error{nil}},
			inputs: 1,
			want:   []float32{0.1, 0.2},
		},
		{
			name:   "vectors for two inputs are averaged",
			res:    &modulecomponents.VectorizationResult[[]float32]{Vector: [][]float32{{1, 2}, {3, 4}}},
			inputs: 2,
			want:   []float32{2, 3},
		},
		{
			name:   "one vector for two inputs the client joined",
			res:    &modulecomponents.VectorizationResult[[]float32]{Vector: [][]float32{{0.1, 0.2}}},
			inputs: 2,
			want:   []float32{0.1, 0.2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, object, cfg := newVectorizerWithResult(tt.res)

			if tt.inputs == 1 {
				vec, _, err := v.Object(context.Background(), object, cfg, cfg)
				require.NoError(t, err)
				assert.Equal(t, tt.want, vec)
			}

			vec, err := v.Texts(context.Background(), textInputs(tt.inputs), cfg)
			require.NoError(t, err)
			assert.Equal(t, tt.want, vec)
		})
	}
}

func textInputs(n int) []string {
	inputs := make([]string, n)
	for i := range inputs {
		inputs[i] = fmt.Sprintf("text %d", i)
	}
	return inputs
}

// assertVectorizerFails checks Texts with the given number of inputs. Object
// always sends one input, so it is checked only when inputs is 1.
func assertVectorizerFails[T dto.Embedding](t *testing.T, res *modulecomponents.VectorizationResult[T], inputs int, wantErr string) {
	t.Helper()
	v, object, cfg := newVectorizerWithResult(res)

	if inputs == 1 {
		t.Run("Object", func(t *testing.T) {
			vec, _, err := v.Object(context.Background(), object, cfg, cfg)
			require.ErrorContains(t, err, wantErr)
			assert.Nil(t, vec)
		})
	}
	t.Run("Texts", func(t *testing.T) {
		vec, err := v.Texts(context.Background(), textInputs(inputs), cfg)
		require.ErrorContains(t, err, wantErr)
		assert.Nil(t, vec)
	})
}

func newVectorizerWithResult[T dto.Embedding](res *modulecomponents.VectorizationResult[T]) (*BatchVectorizer[T], *models.Object, fakeClassConfig) {
	v := &BatchVectorizer[T]{
		client:           &fakeResultClient[T]{res: res},
		objectVectorizer: objectsvectorizer.New(),
	}
	object := &models.Object{Class: "Car", Properties: map[string]any{"text": "some text"}}
	cfg := fakeClassConfig{classConfig: map[string]any{"vectorizeClassName": false}}
	return v, object, cfg
}

type fakeResultClient[T dto.Embedding] struct {
	res *modulecomponents.VectorizationResult[T]
}

func (c *fakeResultClient[T]) Vectorize(ctx context.Context, input []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[T], *modulecomponents.RateLimits, int, error) {
	return c.res, nil, 0, nil
}

func (c *fakeResultClient[T]) VectorizeQuery(ctx context.Context, input []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[T], error) {
	return c.res, nil
}

func (c *fakeResultClient[T]) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	return &modulecomponents.RateLimits{}
}

func (c *fakeResultClient[T]) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	return [32]byte{}
}

type fakeClassConfig struct {
	classConfig           map[string]any
	vectorizeClassName    bool
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
	properties            []string
	// module specific settings
	cohereModel  string
	truncateType string
	baseURL      string
}

func (f fakeClassConfig) Class() map[string]any {
	classSettings := map[string]any{
		"vectorizeClassName": f.vectorizeClassName,
		"model":              f.cohereModel,
		"truncate":           f.truncateType,
		"baseURL":            f.baseURL,
	}
	return classSettings
}

func (f fakeClassConfig) PropertyIndexed(property string) bool {
	return !((property == f.skippedProperty) || (property == f.excludedProperty))
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]any {
	if propName == f.skippedProperty {
		return map[string]any{
			"skip": true,
		}
	}
	if propName == f.excludedProperty {
		return map[string]any{
			"vectorizePropertyName": false,
		}
	}
	if f.vectorizePropertyName {
		return map[string]any{
			"vectorizePropertyName": true,
		}
	}
	return nil
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) VectorizeClassName() bool {
	return f.classConfig["vectorizeClassName"].(bool)
}

func (f fakeClassConfig) VectorizePropertyName(propertyName string) bool {
	return f.vectorizePropertyName
}

func (f fakeClassConfig) Properties() []string {
	return f.properties
}

func (f fakeClassConfig) LowerCaseInput() bool {
	return false
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}

func (f fakeClassConfig) Config() *config.Config {
	return nil
}

func BenchmarkBatchVectorizer(b *testing.B) {
	tokenizer := batch.ReturnBatchTokenizer(1, "", false)
	ctx := context.Background()
	cfg := &fakeClassConfig{vectorizePropertyName: false, classConfig: map[string]any{"vectorizeClassName": false}}

	vectorizer := objectsvectorizer.New()
	logger, _ := logrustest.NewNullLogger()
	encoderCache := batch.NewEncoderCache(logger)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _, err := tokenizer(ctx, []*models.Object{}, []bool{false}, cfg, vectorizer, encoderCache)
		require.NoError(b, err)
	}
}
