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
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"
)

// Manager manages kind changes at a use-case level, i.e. agnostic of
// underlying databases or storage providers
type Manager struct {
	config            *config.WeaviateConfig
	locks             locks
	schemaManager     schemaManager
	logger            logrus.FieldLogger
	authorizer        authorizer
	vectorRepo        VectorRepo
	timeSource        timeSource
	modulesProvider   ModulesProvider
	autoSchemaManager *autoSchemaManager
	metrics           objectsMetrics
}

type objectsMetrics interface {
	BatchInc()
	BatchDec()
	BatchRefInc()
	BatchRefDec()
	BatchDeleteInc()
	BatchDeleteDec()
	AddObjectInc()
	AddObjectDec()
	UpdateObjectInc()
	UpdateObjectDec()
	MergeObjectInc()
	MergeObjectDec()
	DeleteObjectInc()
	DeleteObjectDec()
	GetObjectInc()
	GetObjectDec()
	HeadObjectInc()
	HeadObjectDec()
	AddReferenceInc()
	AddReferenceDec()
	UpdateReferenceInc()
	UpdateReferenceDec()
	DeleteReferenceInc()
	DeleteReferenceDec()
	AddUsageDimensions(className, queryType, operation string, dims int)
}

type timeSource interface {
	Now() int64
}

type locks interface {
	LockConnector() (func() error, error)
	LockSchema() (func() error, error)
}

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

type VectorRepo interface {
	PutObject(ctx context.Context, concept *models.Object, vector []float32,
		repl *additional.ReplicationProperties) error
	DeleteObject(ctx context.Context, className string, id strfmt.UUID,
		repl *additional.ReplicationProperties, tenant string) error
	// Object returns object of the specified class giving by its id
	Object(ctx context.Context, class string, id strfmt.UUID, props search.SelectProperties,
		additional additional.Properties, repl *additional.ReplicationProperties,
		tenant string) (*search.Result, error)
	// Exists returns true if an object of a giving class exists
	Exists(ctx context.Context, class string, id strfmt.UUID,
		repl *additional.ReplicationProperties, tenant string) (bool, error)
	ObjectByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties,
		additional additional.Properties, tenant string) (*search.Result, error)
	ObjectSearch(ctx context.Context, offset, limit int, filters *filters.LocalFilter,
		sort []filters.Sort, additional additional.Properties, tenant string) (search.Results, error)
	AddReference(ctx context.Context, source *crossref.RefSource,
		target *crossref.Ref, repl *additional.ReplicationProperties, tenant string) error
	Merge(ctx context.Context, merge MergeDocument, repl *additional.ReplicationProperties, tenant string) error
	Query(context.Context, *QueryInput) (search.Results, *Error)
}

type ModulesProvider interface {
	GetObjectAdditionalExtend(ctx context.Context, in *search.Result,
		moduleParams map[string]interface{}) (*search.Result, error)
	ListObjectsAdditionalExtend(ctx context.Context, in search.Results,
		moduleParams map[string]interface{}) (search.Results, error)
	UsingRef2Vec(className string) bool
	UpdateVector(ctx context.Context, object *models.Object, class *models.Class,
		objectDiff *moduletools.ObjectDiff, repo modulecapabilities.FindObjectFn,
		logger logrus.FieldLogger) error
	VectorizerName(className string) (string, error)
}

// NewManager creates a new manager
func NewManager(locks locks, schemaManager schemaManager,
	config *config.WeaviateConfig, logger logrus.FieldLogger,
	authorizer authorizer, vectorRepo VectorRepo,
	modulesProvider ModulesProvider, metrics objectsMetrics,
) *Manager {
	return &Manager{
		config:            config,
		locks:             locks,
		schemaManager:     schemaManager,
		logger:            logger,
		authorizer:        authorizer,
		vectorRepo:        vectorRepo,
		timeSource:        defaultTimeSource{},
		modulesProvider:   modulesProvider,
		autoSchemaManager: newAutoSchemaManager(schemaManager, vectorRepo, config, logger),
		metrics:           metrics,
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
