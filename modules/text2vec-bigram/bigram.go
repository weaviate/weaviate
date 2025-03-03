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
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
	"github.com/weaviate/weaviate/usecases/modulecomponents/text2vecbase"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

const Name = "text2vec-bigram"

func New() *BigramModule {
	m := &BigramModule{}
	m.initNearText()
	return m
}

type BigramModule struct {
	vectorizer                   text2vecbase.TextVectorizer[[]float32]
	vectors                      map[string][]float32
	storageProvider              moduletools.StorageProvider
	GraphqlProvider              modulecapabilities.GraphQLArguments
	Searcher                     modulecapabilities.Searcher[[]float32]
	NearTextTransformer          modulecapabilities.TextTransform
	logger                       logrus.FieldLogger
	AdditionalPropertiesProvider modulecapabilities.AdditionalProperties
	activeVectoriser             string
}

func (m *BigramModule) Name() string {
	return Name
}

func (m *BigramModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Vec
}

func (m *BigramModule) Init(ctx context.Context, params moduletools.ModuleInitParams) error {
	m.storageProvider = params.GetStorageProvider()
	m.logger = params.GetLogger()
	m.initNearText()

	switch strings.ToLower(os.Getenv("BIGRAM")) {
	case "alphabet":
		m.activeVectoriser = "alphabet"
	case "trigram":
		m.activeVectoriser = "trigram"
	case "bytepairs":
		m.activeVectoriser = "bytepairs"
	default:
		m.activeVectoriser = "mod26"
	}

	return nil
}

func (m *BigramModule) InitExtension(modules []modulecapabilities.Module) error {
	return nil
}

func (m *BigramModule) InitVectorizer(ctx context.Context, timeout time.Duration, logger logrus.FieldLogger) error {
	return nil
}

func (m *BigramModule) InitAdditionalPropertiesProvider() error {
	return nil
}

func (m *BigramModule) RootHandler() http.Handler {
	return nil
}

func (m *BigramModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return additional.NewText2VecProvider().AdditionalProperties()
}

func text2vec(input, activeVectoriser string) ([]float32, error) {
	log.Printf("vectorising %s with %s", input, activeVectoriser)
	switch activeVectoriser {
	case "alphabet":
		return alphabet2Vector(input)
	case "trigram":
		return trigramVector(input)
	case "bytepairs":
		return bytePairs2Vector(input)
	case "mod26":
		return mod26Vector(input)
	default:
		log.Printf("unsupported vectoriser: %s", activeVectoriser)
		return nil, fmt.Errorf("unsupported vectoriser: %s", activeVectoriser)
	}
}

func (m *BigramModule) VectorizeInput(ctx context.Context, input string, cfg moduletools.ClassConfig) ([]float32, error) {
	vector, err := text2vec(input, m.activeVectoriser)
	return vector, err
}

func (m *BigramModule) AddVector(text string, vector []float32) error {
	if m.vectors == nil {
		m.vectors = map[string][]float32{}
	}
	m.vectors[text] = vector
	return nil
}

func (m *BigramModule) VectorFromParams(ctx context.Context, params interface{}, className string, findVectorFn modulecapabilities.FindVectorFn[[]float32], cfg moduletools.ClassConfig) ([]float32, error) {
	switch thing := params.(type) {
	case *nearText.NearTextParams:
		return m.Texts(ctx, params.(*nearText.NearTextParams).Values, cfg)
	default:
		return nil, fmt.Errorf("unsupported params type: %T, %v", params, thing)
	}
}

func (m *BigramModule) Texts(ctx context.Context, inputs []string, cfg moduletools.ClassConfig) ([]float32, error) {
	var vectors [][]float32
	for _, input := range inputs {
		vector, err := text2vec(input, m.activeVectoriser)
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, vector)
	}
	return libvectorizer.CombineVectors(vectors), nil
}

var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer[[]float32](New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.Searcher[[]float32](New())
	_ = modulecapabilities.GraphQLArguments(New())
	_ = modulecapabilities.ClassConfigurator(New())
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

func (m *BigramModule) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{
		"name": "Bigram Trigram Frequency Vectoriser",
	}, nil
}

type vectorForParams struct {
	fn func(ctx context.Context,
		params interface{}, className string,
		findVectorFn modulecapabilities.FindVectorFn[[]float32],
		cfg moduletools.ClassConfig,
	) ([]float32, error)
}

func (v *vectorForParams) VectorForParams(ctx context.Context, params interface{}, className string,
	findVectorFn modulecapabilities.FindVectorFn[[]float32],
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	return v.fn(ctx, params, className, findVectorFn, cfg)
}
