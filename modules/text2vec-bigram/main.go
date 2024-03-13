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

package t2vbigram

import (
	"context"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"

	"fmt"

	"github.com/weaviate/weaviate/entities/schema"
)

const Name = "text2vec-bigram"

func New() *BigramModule {
	return &BigramModule{}
}

type BigramModule struct {
	vectors                      map[string][]float32
	GraphqlProvider              modulecapabilities.GraphQLArguments
	Searcher                     modulecapabilities.Searcher
	NearTextTransformer          modulecapabilities.TextTransform
	logger                       logrus.FieldLogger
	AdditionalPropertiesProvider modulecapabilities.AdditionalProperties
}

func (m *BigramModule) Name() string {
	return Name

}

func (m *BigramModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Vec
}

func (m *BigramModule) Init(ctx context.Context, params moduletools.ModuleInitParams) error {
	m.logger = params.GetLogger()
	return nil
}

func (m *BigramModule) InitExtension(modules []modulecapabilities.Module) error {
	return nil
}

func (m *BigramModule) InitVectorizer(ctx context.Context, timeout time.Duration, logger logrus.FieldLogger) error {
	return nil
}

func (m *BigramModule) VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error) {
	return true, []string{}, nil
}

func (m *BigramModule) InitAdditionalPropertiesProvider() error {
	return nil
}

func (m *BigramModule) RootHandler() http.Handler {
	return nil
}

func (m *BigramModule) VectorizeObject(ctx context.Context, obj *models.Object, cfg moduletools.ClassConfig) ([]float32, models.AdditionalProperties, error) {
	var text string
	for _, prop := range obj.Properties.(map[string]interface{}) {
		text += prop.(string)
	}
	vector, error := m.VectorizeInput(ctx, text, cfg)
	return vector, nil, error

}

func (m *BigramModule) MetaInfo() (map[string]interface{}, error) {
	return nil, nil
}

func (m *BigramModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return additional.NewText2VecProvider().AdditionalProperties()
}

func text2vector(input string) ([]float32, error) {
	bigramcount := map[string]int{}
	for i := 0; i < len(input)-1; i++ {
		bigram := input[i : i+2]
		bigramcount[bigram]++
	}

	vector := make([]float32, 256*256)
	for bigram, count := range bigramcount {
		index := int(bigram[0]) * int(bigram[1])
		vector[index] = float32(count)
	}
	var sum float32
	for _, v := range vector {
		sum += v
	}

	for i, v := range vector {
		vector[i] = v / sum
	}
	return vector[1:], nil
}

func (m *BigramModule) VectorizeInput(ctx context.Context, input string, cfg moduletools.ClassConfig) ([]float32, error) {
	vector, err := text2vector(input)
	return vector, err
}

func (m *BigramModule) AddVector(text string, vector []float32) error {
	if m.vectors == nil {
		m.vectors = map[string][]float32{}
	}
	m.vectors[text] = vector
	return nil
}

func (m *BigramModule) VectorFromParams(ctx context.Context, params interface{}, className string, findVectorFn modulecapabilities.FindVectorFn, cfg moduletools.ClassConfig) ([]float32, error) {
	switch whatever := params.(type) {
	case *nearText.NearTextParams:
		return m.Texts(ctx, params.(*nearText.NearTextParams).Values, cfg)
	default:
		return nil, fmt.Errorf("unsupported params type: %T, %v", params, whatever)
	}
}

func (m *BigramModule) VectorSearches() map[string]modulecapabilities.VectorForParams {
	vectorSearches := map[string]modulecapabilities.VectorForParams{}

	vectorSearches["nearText"] = m.VectorFromParams
	return vectorSearches
}

func (m *BigramModule) Texts(ctx context.Context, inputs []string, cfg moduletools.ClassConfig) ([]float32, error) {
	var vectors [][]float32
	for _, input := range inputs {
		vector, err := text2vector(input)
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, vector)
	}
	return libvectorizer.CombineVectors(vectors), nil
}

var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.Searcher(New())
)

func (m *BigramModule) ClassConfigDefaults() map[string]interface{} {
	return map[string]interface{}{
		"vectorizeClassName": true,
	}
}

func (m *BigramModule) PropertyConfigDefaults(dt *schema.DataType) map[string]interface{} {
	return map[string]interface{}{
		"skip":                  false,
		"vectorizePropertyName": true,
	}
}

func (m *BigramModule) ValidateClass(ctx context.Context, class *models.Class, cfg moduletools.ClassConfig) error {
	return nil
}

var _ = modulecapabilities.ClassConfigurator(New())
