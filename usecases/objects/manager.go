//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// package objects provides managers for all kind-related items, such as objects.
// Manager provides methods for "regular" interaction, such as
// add, get, delete, update, etc. Additionally BatchManager allows for
// efficient batch-adding of object instances and references.
package objects

import (
	"context"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
)

// Manager manages kind changes at a use-case level, i.e. agnostic of
// underlying databases or storage providers
type Manager struct {
	config             *config.WeaviateConfig
	locks              locks
	schemaManager      schemaManager
	logger             logrus.FieldLogger
	authorizer         authorizer
	vectorizerProvider VectorizerProvider
	vectorRepo         VectorRepo
	timeSource         timeSource
	modulesProvider    ModulesProvider
	autoSchemaManager  *autoSchemaManager
}

type timeSource interface {
	Now() int64
}

type VectorizerProvider interface {
	Vectorizer(moduleName, className string) (Vectorizer, error)
}

type Vectorizer interface {
	UpdateObject(ctx context.Context, obj *models.Object) error
}

type locks interface {
	LockConnector() (func() error, error)
	LockSchema() (func() error, error)
}

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

type VectorRepo interface {
	PutObject(ctx context.Context, concept *models.Object, vector []float32) error
	DeleteObject(ctx context.Context, className string, id strfmt.UUID) error

	ObjectByID(ctx context.Context, id strfmt.UUID, props traverser.SelectProperties,
		additional traverser.AdditionalProperties) (*search.Result, error)
	ObjectSearch(ctx context.Context, limit int, filters *filters.LocalFilter,
		additional traverser.AdditionalProperties) (search.Results, error)

	Exists(ctx context.Context, id strfmt.UUID) (bool, error)

	AddReference(ctx context.Context, className string,
		source strfmt.UUID, propName string, ref *models.SingleRef) error
	Merge(ctx context.Context, merge MergeDocument) error
}

type ModulesProvider interface {
	GetObjectAdditionalExtend(ctx context.Context, in *search.Result,
		moduleParams map[string]interface{}) (*search.Result, error)
	ListObjectsAdditionalExtend(ctx context.Context, in search.Results,
		moduleParams map[string]interface{}) (search.Results, error)
}

// NewManager creates a new manager
func NewManager(locks locks, schemaManager schemaManager,
	config *config.WeaviateConfig, logger logrus.FieldLogger,
	authorizer authorizer, vectorizer VectorizerProvider, vectorRepo VectorRepo,
	modulesProvider ModulesProvider) *Manager {
	return &Manager{
		config:             config,
		locks:              locks,
		schemaManager:      schemaManager,
		logger:             logger,
		vectorizerProvider: vectorizer,
		authorizer:         authorizer,
		vectorRepo:         vectorRepo,
		timeSource:         defaultTimeSource{},
		modulesProvider:    modulesProvider,
		autoSchemaManager:  newAutoSchemaManager(schemaManager, vectorRepo, config),
	}
}

func generateUUID() (strfmt.UUID, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("could not generate uuid v4: %v", err)
	}

	return strfmt.UUID(id.String()), nil
}

type defaultTimeSource struct{}

func (ts defaultTimeSource) Now() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
