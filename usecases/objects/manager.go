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
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type schemaManager interface {
	AddClass(ctx context.Context, principal *models.Principal, class *models.Class) (*models.Class, uint64, error)
	AddTenants(ctx context.Context, principal *models.Principal, class string, tenants []*models.Tenant) (uint64, error)
	GetClass(ctx context.Context, principal *models.Principal, name string) (*models.Class, error)
	// ReadOnlyClass return class model.
	ReadOnlyClass(name string) *models.Class
	// AddClassProperty it is upsert operation. it adds properties to a class and updates
	// existing properties if the merge bool passed true.
	AddClassProperty(ctx context.Context, principal *models.Principal, class *models.Class, className string, merge bool, prop ...*models.Property) (*models.Class, uint64, error)

	// Consistent methods with the consistency flag.
	// This is used to ensure that internal users will not miss-use the flag and it doesn't need to be set to a default
	// value everytime we use the Manager.

	// GetConsistentClass overrides the default implementation to consider the consistency flag
	GetConsistentClass(ctx context.Context, principal *models.Principal,
		name string, consistency bool,
	) (*models.Class, uint64, error)

	// GetCachedClass extracts class from context. If class was not set it is fetched first
	GetCachedClass(ctx context.Context, principal *models.Principal, names ...string,
	) (map[string]versioned.Class, error)

	GetCachedClassNoAuth(ctx context.Context, names ...string) (map[string]versioned.Class, error)

	// WaitForUpdate ensures that the local schema has caught up to schemaVersion
	WaitForUpdate(ctx context.Context, schemaVersion uint64) error

	// GetConsistentSchema retrieves a locally cached copy of the schema
	GetConsistentSchema(ctx context.Context, principal *models.Principal, consistency bool) (schema.Schema, error)
}

// Manager manages kind changes at a use-case level, i.e. agnostic of
// underlying databases or storage providers
type Manager struct {
	config            *config.WeaviateConfig
	schemaManager     schemaManager
	logger            logrus.FieldLogger
	authorizer        authorization.Authorizer
	vectorRepo        VectorRepo
	timeSource        timeSource
	modulesProvider   ModulesProvider
	autoSchemaManager *AutoSchemaManager
	metrics           objectsMetrics
	allocChecker      *memwatch.Monitor
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

type VectorRepo interface {
	PutObject(ctx context.Context, concept *models.Object, vector []float32,
		vectors map[string][]float32, multiVectors map[string][][]float32,
		repl *additional.ReplicationProperties, schemaVersion uint64) error
	DeleteObject(ctx context.Context, className string, id strfmt.UUID, deletionTime time.Time,
		repl *additional.ReplicationProperties, tenant string, schemaVersion uint64) error
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
		target *crossref.Ref, repl *additional.ReplicationProperties, tenant string, schemaVersion uint64) error
	Merge(ctx context.Context, merge MergeDocument, repl *additional.ReplicationProperties, tenant string, schemaVersion uint64) error
	Query(context.Context, *QueryInput) (search.Results, *Error)
}

type ModulesProvider interface {
	GetObjectAdditionalExtend(ctx context.Context, in *search.Result,
		moduleParams map[string]interface{}) (*search.Result, error)
	ListObjectsAdditionalExtend(ctx context.Context, in search.Results,
		moduleParams map[string]interface{}) (search.Results, error)
	UsingRef2Vec(className string) bool
	UpdateVector(ctx context.Context, object *models.Object, class *models.Class, repo modulecapabilities.FindObjectFn,
		logger logrus.FieldLogger) error
	BatchUpdateVector(ctx context.Context, class *models.Class, objects []*models.Object,
		findObjectFn modulecapabilities.FindObjectFn,
		logger logrus.FieldLogger) (map[int]error, error)
	VectorizerName(className string) (string, error)
}

// NewManager creates a new manager
func NewManager(schemaManager schemaManager,
	config *config.WeaviateConfig, logger logrus.FieldLogger,
	authorizer authorization.Authorizer, vectorRepo VectorRepo,
	modulesProvider ModulesProvider, metrics objectsMetrics, allocChecker *memwatch.Monitor,
	autoSchemaManager *AutoSchemaManager,
) *Manager {
	if allocChecker == nil {
		allocChecker = memwatch.NewDummyMonitor()
	}

	return &Manager{
		config:            config,
		schemaManager:     schemaManager,
		logger:            logger,
		authorizer:        authorizer,
		vectorRepo:        vectorRepo,
		timeSource:        defaultTimeSource{},
		modulesProvider:   modulesProvider,
		autoSchemaManager: autoSchemaManager,
		metrics:           metrics,
		allocChecker:      allocChecker,
	}
}

func generateUUID() (strfmt.UUID, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("could not generate uuid v4: %w", err)
	}

	return strfmt.UUID(id.String()), nil
}

type defaultTimeSource struct{}

func (ts defaultTimeSource) Now() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
