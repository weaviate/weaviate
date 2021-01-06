//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
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
	uuid "github.com/satori/go.uuid"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/projector"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/semi-technologies/weaviate/usecases/vectorizer"
	"github.com/sirupsen/logrus"
)

// Manager manages kind changes at a use-case level, i.e. agnostic of
// underlying databases or storage providers
type Manager struct {
	config        *config.WeaviateConfig
	locks         locks
	schemaManager schemaManager
	logger        logrus.FieldLogger
	authorizer    authorizer
	vectorizer    Vectorizer
	vectorRepo    VectorRepo
	timeSource    timeSource
	nnExtender    nnExtender
	projector     featureProjector
}

type nnExtender interface {
	Single(ctx context.Context, in *search.Result, limit *int) (*search.Result, error)
	Multi(ctx context.Context, in []search.Result, limit *int) ([]search.Result, error)
}

type featureProjector interface {
	Reduce(in []search.Result, params *projector.Params) ([]search.Result, error)
}

type timeSource interface {
	Now() int64
}

type Vectorizer interface {
	Object(ctx context.Context, concept *models.Object) ([]float32, []vectorizer.InputElement, error)
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

	AddReference(ctx context.Context, kind kind.Kind, className string,
		source strfmt.UUID, propName string, ref *models.SingleRef) error
	Merge(ctx context.Context, merge MergeDocument) error
}

// NewManager creates a new manager
func NewManager(locks locks, schemaManager schemaManager,
	config *config.WeaviateConfig, logger logrus.FieldLogger,
	authorizer authorizer, vectorizer Vectorizer, vectorRepo VectorRepo,
	nnExtender nnExtender, projector featureProjector) *Manager {
	return &Manager{
		config:        config,
		locks:         locks,
		schemaManager: schemaManager,
		logger:        logger,
		vectorizer:    vectorizer,
		authorizer:    authorizer,
		vectorRepo:    vectorRepo,
		nnExtender:    nnExtender,
		timeSource:    defaultTimeSource{},
		projector:     projector,
	}
}

func generateUUID() (strfmt.UUID, error) {
	uuid, err := uuid.NewV4()
	if err != nil {
		return "", fmt.Errorf("could not generate uuid v4: %v", err)
	}

	return strfmt.UUID(fmt.Sprintf("%v", uuid)), nil
}

type defaultTimeSource struct{}

func (ts defaultTimeSource) Now() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
