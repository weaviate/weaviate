//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package classification

import (
	"context"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	uuid "github.com/satori/go.uuid"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/filterext"
	libfilters "github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	libvectorizer "github.com/semi-technologies/weaviate/usecases/vectorizer"
	"github.com/sirupsen/logrus"
)

type distancer func(a, b []float32) (float32, error)

type Classifier struct {
	schemaGetter schemaUC.SchemaGetter
	repo         Repo
	vectorRepo   vectorRepo
	authorizer   authorizer
	distancer    distancer
	vectorizer   vectorizer
	logger       logrus.FieldLogger
}

type vectorizer interface {
	// MultiVectorForWord must keep order, if an item cannot be vectorized, the
	// element should be explicit nil, not skipped
	MultiVectorForWord(ctx context.Context, words []string) ([][]float32, error)

	VectorForCorpi(ctx context.Context, corpi []string, overrides map[string]string) ([]float32, error)
}

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

func New(sg schemaUC.SchemaGetter, cr Repo, vr vectorRepo, authorizer authorizer,
	vectorizer vectorizer, logger logrus.FieldLogger) *Classifier {
	return &Classifier{
		logger:       logger,
		schemaGetter: sg,
		repo:         cr,
		vectorRepo:   vr,
		authorizer:   authorizer,
		distancer:    libvectorizer.NormalizedDistance,
		vectorizer:   vectorizer,
	}
}

// Repo to manage classification state, should be consistent, not used to store
// acutal data object vectors, see VectorRepo
type Repo interface {
	Put(ctx context.Context, classification models.Classification) error
	Get(ctx context.Context, id strfmt.UUID) (*models.Classification, error)
}

type VectorRepo interface {
	GetUnclassified(ctx context.Context, kind kind.Kind, class string,
		properties []string, filter *libfilters.LocalFilter) ([]search.Result, error)
	AggregateNeighbors(ctx context.Context, vector []float32,
		kind kind.Kind, class string, properties []string, k int,
		filter *libfilters.LocalFilter) ([]NeighborRef, error)
	VectorClassSearch(ctx context.Context, params traverser.GetParams) ([]search.Result, error)
}

type vectorRepo interface {
	VectorRepo
	PutThing(ctx context.Context, thing *models.Thing, vector []float32) error
	PutAction(ctx context.Context, action *models.Action, vector []float32) error
}

// NeighborRef is the result of an aggregation of the ref properties of k neighbors
type NeighborRef struct {
	// Property indicates which property was aggregated
	Property string

	// The beacon of the most common (kNN) reference
	Beacon strfmt.URI

	// Count (n<=k) of number of the winning Beacon
	Count int

	WinningDistance float32
	LosingDistance  *float32
}

type filters struct {
	source      *libfilters.LocalFilter
	target      *libfilters.LocalFilter
	trainingSet *libfilters.LocalFilter
}

func (c *Classifier) Schedule(ctx context.Context, principal *models.Principal, params models.Classification) (*models.Classification, error) {
	err := c.authorizer.Authorize(principal, "create", "classifications/*")
	if err != nil {
		return nil, err
	}

	err = NewValidator(c.schemaGetter, params).Do()
	if err != nil {
		return nil, err
	}

	c.setDefaultValuesForOptionalFields(&params)

	if err := c.assignNewID(&params); err != nil {
		return nil, fmt.Errorf("classification: assign id: %v", err)
	}

	params.Status = models.ClassificationStatusRunning
	params.Meta = &models.ClassificationMeta{
		Started: strfmt.DateTime(time.Now()),
	}

	if err := c.repo.Put(ctx, params); err != nil {
		return nil, fmt.Errorf("classification: put: %v", err)
	}

	// asynchronously trigger the classification
	kind := c.getKind(params)
	filters, err := extractFilters(params)
	if err != nil {
		return nil, err
	}

	go c.run(params, kind, filters)

	return &params, nil
}

func extractFilters(params models.Classification) (filters, error) {
	source, err := filterext.Parse(params.SourceWhere)
	if err != nil {
		return filters{}, fmt.Errorf("field 'sourceWhere': %v", err)
	}

	trainingSet, err := filterext.Parse(params.TrainingSetWhere)
	if err != nil {
		return filters{}, fmt.Errorf("field 'trainingSetWhere': %v", err)
	}

	target, err := filterext.Parse(params.TargetWhere)
	if err != nil {
		return filters{}, fmt.Errorf("field 'targetWhere': %v", err)
	}

	return filters{
		source:      source,
		trainingSet: trainingSet,
		target:      target,
	}, nil
}

func (c *Classifier) getKind(params models.Classification) kind.Kind {
	s := c.schemaGetter.GetSchemaSkipAuth()
	kind, _ := s.GetKindOfClass(schema.ClassName(params.Class))
	// skip nil-check as we have made it past validation
	return kind
}

func (c *Classifier) assignNewID(params *models.Classification) error {
	id, err := uuid.NewV4()
	if err != nil {
		return err
	}

	params.ID = strfmt.UUID(id.String())
	return nil
}

func (c *Classifier) Get(ctx context.Context, principal *models.Principal, id strfmt.UUID) (*models.Classification, error) {
	err := c.authorizer.Authorize(principal, "get", "classifications/*")
	if err != nil {
		return nil, err
	}

	return c.repo.Get(ctx, id)
}

func (c *Classifier) setDefaultValuesForOptionalFields(params *models.Classification) {
	if params.Type == nil {
		defaultType := "knn"
		params.Type = &defaultType
	}

	if *params.Type == "knn" {
		c.setDefaultsForKNN(params)
	}

	if *params.Type == "contextual" {
		c.setDefaultsForContextual(params)
	}

}

func (c *Classifier) setDefaultsForKNN(params *models.Classification) {
	if params.K == nil {
		defaultK := int32(3)
		params.K = &defaultK
	}
}

func (c *Classifier) setDefaultsForContextual(params *models.Classification) {
	// none at the moment
}
