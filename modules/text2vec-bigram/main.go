package t2vbigram

import (
	"context"
	"log"
	"net/http"
	"time"



	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	"fmt"
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
	log.Println("Name bigram module")
	return Name

}

func (m *BigramModule) Type() modulecapabilities.ModuleType {
	log.Println("Type bigram module")
	return modulecapabilities.Text2Vec
}

func (m *BigramModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	log.Printf("Init bigram module\n")
	m.logger = params.GetLogger()
	m.logger.Printf("Init bigram module\n")
	return nil
}

func (m *BigramModule) InitExtension(modules []modulecapabilities.Module) error {
	log.Println("InitExtension bigram module")
	m.logger.Printf("InitExtension bigram module\n")
	return nil
}

func (m *BigramModule) initVectorizer(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	log.Println("InitVectorizer bigram module")
	m.logger.Printf("InitVectorizer bigram module\n")
	return nil
}

func (m *BigramModule) VectorizableProperties (
	cfg moduletools.ClassConfig,
) (bool, []string, error) {
	log.Println("VectorizableProperties bigram module")
	m.logger.Printf("VectorizableProperties bigram module\n")
	return true, []string{}, nil
}

func (m *BigramModule) initAdditionalPropertiesProvider() error {
	log.Println("InitAdditionalPropertiesProvider bigram module")
	return nil
}

func (m *BigramModule) RootHandler() http.Handler {
	log.Println("RootHandler bigram module")
	// TODO: remove once this is a capability interface
	return nil
}

func (m *BigramModule) VectorizeObject(ctx context.Context, obj *models.Object,
cfg moduletools.ClassConfig)  ([]float32, models.AdditionalProperties, error) {
	log.Println("VectorizeObject bigram module")
	m.logger.Printf("VectorizeObject bigram module\n")
	// Concatenate all properties
	var text string
	for _, prop := range obj.Properties.(map[string]interface{}) {
		text += prop.(string)
	}
	log.Printf("Text: %s\n", text)
	vector, error:= m.VectorizeInput(ctx, text, cfg)
	return vector, nil, error

}

func (m *BigramModule) MetaInfo() (map[string]interface{}, error) {
	log.Println("MetaInfo bigram module")
	return nil, nil
}

func (m *BigramModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	log.Println("AdditionalProperties bigram module")
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
	log.Printf("Vector sum: %f\n", sum)
	return vector, nil
}

func (m *BigramModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	log.Println("VectorizeInput bigram module")
	m.logger.Printf("VectorizeInput bigram module\n")
	vector,err := text2vector(input)
	return vector, err
}

func (m *BigramModule) AddVector(text string, vector []float32) error {
	log.Println("AddVector bigram module")
	m.logger.Printf("AddVector bigram module\n")
	if m.vectors == nil {
		m.vectors = map[string][]float32{}
	}
	m.vectors[text] = vector
	return nil
}

func (m *BigramModule) VectorFromParams(ctx context.Context, params interface{},
	className string, findVectorFn modulecapabilities.FindVectorFn, cfg moduletools.ClassConfig) ([]float32, error) {
		log.Println("VectorFromParams bigram module")
		m.logger.Printf("VectorFromParams bigram module\n")
		//Switch on type
		switch  params.(type) {
		case *nearText.NearTextParams:
			return m.Texts(ctx, params.(*nearText.NearTextParams).Values, cfg)
		default:
			return nil, fmt.Errorf("unsupported params type: %T", params)
	}
}

func (m *BigramModule)   VectorSearches() map[string]modulecapabilities.VectorForParams  {
	log.Println("VectorSearches bigram module")
	m.logger.Printf("VectorSearches bigram module\n")
	vectorSearches := map[string]modulecapabilities.VectorForParams{}

	vectorSearches["nearText"] = m.VectorFromParams
	return vectorSearches
}

func (m *BigramModule) Texts(ctx context.Context, inputs []string,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	log.Println("Texts bigram module")
	m.logger.Printf("Texts bigram module\n")
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