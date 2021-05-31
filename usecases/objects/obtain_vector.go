package objects

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus"
)

type vectorObtainer struct {
	vectorizerProvider VectorizerProvider
	schemaManager      schemaManager
	logger             logrus.FieldLogger
}

func newVectorObtainer(vectorizerProvider VectorizerProvider,
	schemaManager schemaManager, logger logrus.FieldLogger) *vectorObtainer {
	return &vectorObtainer{
		vectorizerProvider: vectorizerProvider,
		schemaManager:      schemaManager,
		logger:             logger,
	}
}

// Do retrieves the correct vector and makes sure it is set on the passed-in
// *models.Object. (This method mutates its paremeter)
func (vo *vectorObtainer) Do(ctx context.Context, obj *models.Object,
	principal *models.Principal) error {
	vectorizerName, cfg, err := vo.getVectorizerOfClass(obj.Class, principal)
	if err != nil {
		return err
	}

	if vectorizerName == config.VectorizerModuleNone {
		if err := vo.validateVectorPresent(obj, cfg); err != nil {
			return NewErrInvalidUserInput("%v", err)
		}
	} else {
		vectorizer, err := vo.vectorizerProvider.Vectorizer(vectorizerName, obj.Class)
		if err != nil {
			return err
		}

		if err := vectorizer.UpdateObject(ctx, obj); err != nil {
			return NewErrInternal("%v", err)
		}
	}

	return nil
}

func (vo *vectorObtainer) getVectorizerOfClass(className string,
	principal *models.Principal) (string, interface{}, error) {
	s, err := vo.schemaManager.GetSchema(principal)
	if err != nil {
		return "", nil, err
	}

	class := s.FindClassByName(schema.ClassName(className))
	if class == nil {
		// this should be impossible by the time this method gets called, but let's
		// be 100% certain
		return "", nil, errors.Errorf("class %s not present", className)
	}

	return class.Vectorizer, class.VectorIndexConfig, nil
}

func (vo *vectorObtainer) validateVectorPresent(class *models.Object,
	config interface{}) error {
	hnswConfig, ok := config.(hnsw.UserConfig)
	if !ok {
		return errors.Errorf("vector index config (%T) is not of type HNSW, "+
			"but objects manager is restricted to HNSW", config)
	}

	if !hnswConfig.Skip && len(class.Vector) == 0 {
		return errors.Errorf("this class is configured to use vectorizer 'none' " +
			"thus a vector must be present when importing, got: field 'vector' is empty " +
			"or contains a zero-length vector")
	}

	return nil
}
