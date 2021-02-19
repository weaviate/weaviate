package modtransformers

import (
	"context"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/modules/text2vec-transformers/clients"
	"github.com/semi-technologies/weaviate/modules/text2vec-transformers/vectorizer"
	"github.com/semi-technologies/weaviate/usecases/modules"
)

func New() *TransformersModule {
	return &TransformersModule{}
}

type TransformersModule struct {
	vectorizer textVectorizer
}

type textVectorizer interface {
	Object(ctx context.Context, obj *models.Object,
		icheck vectorizer.ClassIndexCheck) error
}

func (m *TransformersModule) Name() string {
	return "text2vec-transformers"
}

func (m *TransformersModule) Init(params modules.ModuleInitParams) error {
	if err := m.initVectorizer(); err != nil {
		return errors.Wrap(err, "init vectorizer")
	}

	return nil
}

func (m *TransformersModule) initVectorizer() error {
	// TODO: Get discovery information from config
	// TODO: this should be coming from the init method
	ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Minute)
	defer cancel()

	client := clients.New("http://localhost:8000")
	if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
		return errors.Wrap(err, "init remote vectorizer")
	}

	m.vectorizer = vectorizer.New(client)

	return nil
}

func (m *TransformersModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *TransformersModule) VectorizeObject(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig) error {
	icheck := vectorizer.NewIndexChecker(cfg)
	return m.vectorizer.Object(ctx, obj, icheck)
}

// verify we implement the modules.Module interface
var (
	_ = modules.Module(New())
	_ = modulecapabilities.Vectorizer(New())
)

// A placeholder until we support parsing the config
type dummyICheck struct{}

func (d dummyICheck) PropertyIndexed(propName string) bool {
	return true
}

func (d dummyICheck) VectorizePropertyName(propName string) bool {
	return false
}

func (d dummyICheck) VectorizeClassName() bool {
	return true
}
