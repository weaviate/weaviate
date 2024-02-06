package t2vbigram

import (
	"context"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
)

const Name = "text2vec-bigram"

func New() *BigramModule {
	return &BigramModule{}
}

type BigramModule struct {
	vectors                      map[string][]float32
	graphqlProvider              modulecapabilities.GraphQLArguments
	searcher                     modulecapabilities.Searcher
	nearTextTransformer          modulecapabilities.TextTransform
	logger                       logrus.FieldLogger
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

func (m *BigramModule) Name() string {
	return Name
}

func (m *BigramModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Vec
}

func (m *BigramModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	return nil
}

func (m *BigramModule) InitExtension(modules []modulecapabilities.Module) error {
	return nil
}

func (m *BigramModule) initVectorizer(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	return nil
}

func (m *BigramModule) initAdditionalPropertiesProvider() error {
	return nil
}

func (m *BigramModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *BigramModule) VectorizeObject(ctx context.Context,
	obj *models.Object, objDiff *moduletools.ObjectDiff, cfg moduletools.ClassConfig,
) error {
	return nil
}

func (m *BigramModule) MetaInfo() (map[string]interface{}, error) {
	return nil, nil
}

func (m *BigramModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return nil
}

func (m *BigramModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	bigramcount := map[string]int{}
	for i := 0; i < len(input)-1; i++ {
		bigram := input[i : i+2]
		bigramcount[bigram]++
	}
	vector := make([]float32, 256*256)
	for bigram, count := range bigramcount {
		//Calculate vector index by multiply the ascii values of the bigram characters
		index := int(bigram[0]) * int(bigram[1])
		vector[index] = float32(count)
	}
	//Normalize the vector
	var sum float32
	for _, v := range vector {
		sum += v
	}


	for i, v := range vector {
		vector[i] = v / sum
	}
	return vector, nil
}

func (m *BigramModule) AddVector(text string, vector []float32) error {
	if m.vectors == nil {
		m.vectors = map[string][]float32{}
	}
	m.vectors[text] = vector
	return nil
}
