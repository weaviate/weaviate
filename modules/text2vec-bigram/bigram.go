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
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

const Name = "text2vec-bigram"

func New() *BigramModule {
	return &BigramModule{}
}

type BigramModule struct {
	vectors                      map[string][]float32
	storageProvider              moduletools.StorageProvider
	GraphqlProvider              modulecapabilities.GraphQLArguments
	Searcher                     modulecapabilities.Searcher[[]float32]
	NearTextTransformer          modulecapabilities.TextTransform
	logger                       logrus.FieldLogger
	AdditionalPropertiesProvider modulecapabilities.AdditionalProperties
	activeVectoriser             string
}

func (m *BigramModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	return map[string]modulecapabilities.GraphQLArgument{}
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
		text += fmt.Sprintf("%v", prop)
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

func alphabetOrdinal(letter rune) int {
	return int(letter - 'a')
}

func ord(letter rune) int {
	return int(letter)
}

func stripNonAlphabets(input string) (string, error) {
	reg, err := regexp.Compile("[^a-zA-Z]+")
	if err != nil {
		return "", err
	}
	return reg.ReplaceAllString(input, ""), nil
}

func alphabet2Vector(input string) ([]float32, error) {
	// Strip everything out of the in that is not a letter
	// and convert to lower case
	in, err := stripNonAlphabets(input)
	if err != nil {
		return nil, err
	}
	in = strings.ToLower(in)
	vector := make([]float32, 26*26)
	for i := 0; i < len(in)-1; i++ {
		first := alphabetOrdinal(rune(in[i]))
		second := alphabetOrdinal(rune(in[i+1]))
		index := first*26 + second
		vector[index] = vector[index] + 1
	}
	var sum float32
	for _, v := range vector {
		sum += v
	}

	for i, v := range vector {
		vector[i] = v / sum
	}
	return vector, nil
}

// Maybe we should do this for bytes instead of letters?
func mod26Vector(input string) ([]float32, error) {
	input = strings.ToLower(input)
	vector := make([]float32, 26*26)
	for i := 0; i < len(input)-1; i++ {
		first := int(input[i]) % 26
		second := int(input[i+1]) % 26
		index := first*26 + second
		vector[index] = vector[index] + 1
	}

	return normaliseVector(vector), nil
}

func normaliseVector(vector []float32) []float32 {
	var sum float32
	for _, v := range vector {
		sum += v
	}

	for i, v := range vector {
		vector[i] = v / sum
	}
	return vector
}

func trigramVector(input string) ([]float32, error) {
	input = strings.ToLower(input)
	vector := make([]float32, 26*26*26)
	for i := 0; i < len(input)-2; i++ {
		first := ord(rune(input[i])) % 26
		second := ord(rune(input[i+1])) % 26
		third := ord(rune(input[i+2])) % 26
		index := first*26*26 + second*26 + third
		vector[index] = vector[index] + 1
	}

	return normaliseVector(vector), nil
}

func bytePairs2Vector(input string) ([]float32, error) {
	vector := make([]float32, 256*256)
	for i := 0; i < len(input)-1; i++ {
		bigram := input[i : i+2]

		index := int(bigram[0]) * int(bigram[1])
		vector[index] = vector[index] + 1
	}

	return normaliseVector(vector[1:]), nil // Max length is 16k-1
}

func text2vec(input, activeVectoriser string) ([]float32, error) {
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

func (m *BigramModule) VectorSearches() map[string]modulecapabilities.VectorForParams[[]float32] {
	vectorSearches := map[string]modulecapabilities.VectorForParams[[]float32]{}

	vectorSearches["nearText"] = &vectorForParams{m.VectorFromParams}
	return vectorSearches
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

func (m *BigramModule) VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, []models.AdditionalProperties, map[int]error) {
	var (
		vectors [][]float32
		errors  = map[int]error{}
	)
	for i, obj := range objs {
		if skipObject[i] {
			continue
		}
		vector, _, err := m.VectorizeObject(ctx, obj, cfg)
		if err != nil {
			errors[i] = err
		}
		vectors = append(vectors, vector)
	}
	return vectors, nil, errors
}

var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer[[]float32](New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.Searcher[[]float32](New())
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
