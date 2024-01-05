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

package classification

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/filterext"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	libfilters "github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

type classificationFilters struct {
	source      *libfilters.LocalFilter
	target      *libfilters.LocalFilter
	trainingSet *libfilters.LocalFilter
}

func (f classificationFilters) Source() *libfilters.LocalFilter {
	return f.source
}

func (f classificationFilters) Target() *libfilters.LocalFilter {
	return f.target
}

func (f classificationFilters) TrainingSet() *libfilters.LocalFilter {
	return f.trainingSet
}

type distancer func(a, b []float32) (float32, error)

type Classifier struct {
	schemaGetter          schemaUC.SchemaGetter
	repo                  Repo
	vectorRepo            vectorRepo
	vectorClassSearchRepo modulecapabilities.VectorClassSearchRepo
	authorizer            authorizer
	distancer             distancer
	modulesProvider       ModulesProvider
	logger                logrus.FieldLogger
}

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

type ModulesProvider interface {
	ParseClassifierSettings(name string,
		params *models.Classification) error
	GetClassificationFn(className, name string,
		params modulecapabilities.ClassifyParams) (modulecapabilities.ClassifyItemFn, error)
}

func New(sg schemaUC.SchemaGetter, cr Repo, vr vectorRepo, authorizer authorizer,
	logger logrus.FieldLogger, modulesProvider ModulesProvider,
) *Classifier {
	return &Classifier{
		logger:                logger,
		schemaGetter:          sg,
		repo:                  cr,
		vectorRepo:            vr,
		authorizer:            authorizer,
		distancer:             libvectorizer.NormalizedDistance,
		vectorClassSearchRepo: newVectorClassSearchRepo(vr),
		modulesProvider:       modulesProvider,
	}
}

// Repo to manage classification state, should be consistent, not used to store
// actual data object vectors, see VectorRepo
type Repo interface {
	Put(ctx context.Context, classification models.Classification) error
	Get(ctx context.Context, id strfmt.UUID) (*models.Classification, error)
}

type VectorRepo interface {
	GetUnclassified(ctx context.Context, class string,
		properties []string, filter *libfilters.LocalFilter) ([]search.Result, error)
	AggregateNeighbors(ctx context.Context, vector []float32,
		class string, properties []string, k int,
		filter *libfilters.LocalFilter) ([]NeighborRef, error)
	VectorSearch(ctx context.Context, params dto.GetParams) ([]search.Result, error)
	ZeroShotSearch(ctx context.Context, vector []float32,
		class string, properties []string,
		filter *libfilters.LocalFilter) ([]search.Result, error)
}

type vectorRepo interface {
	VectorRepo
	BatchPutObjects(ctx context.Context, objects objects.BatchObjects,
		repl *additional.ReplicationProperties) (objects.BatchObjects, error)
}

// NeighborRef is the result of an aggregation of the ref properties of k
// neighbors
type NeighborRef struct {
	// Property indicates which property was aggregated
	Property string

	// The beacon of the most common (kNN) reference
	Beacon strfmt.URI

	OverallCount int
	WinningCount int
	LosingCount  int

	Distances NeighborRefDistances
}

func (c *Classifier) Schedule(ctx context.Context, principal *models.Principal, params models.Classification) (*models.Classification, error) {
	err := c.authorizer.Authorize(principal, "create", "classifications/*")
	if err != nil {
		return nil, err
	}

	err = c.parseAndSetDefaults(&params)
	if err != nil {
		return nil, err
	}

	err = NewValidator(c.schemaGetter, params).Do()
	if err != nil {
		return nil, err
	}

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
	filters, err := c.extractFilters(params)
	if err != nil {
		return nil, err
	}

	go c.run(params, filters)

	return &params, nil
}

func (c *Classifier) extractFilters(params models.Classification) (Filters, error) {
	if params.Filters == nil {
		return classificationFilters{}, nil
	}

	source, err := filterext.Parse(params.Filters.SourceWhere, params.Class)
	if err != nil {
		return classificationFilters{}, fmt.Errorf("field 'sourceWhere': %v", err)
	}

	trainingSet, err := filterext.Parse(params.Filters.TrainingSetWhere, params.Class)
	if err != nil {
		return classificationFilters{}, fmt.Errorf("field 'trainingSetWhere': %v", err)
	}

	target, err := filterext.Parse(params.Filters.TargetWhere, params.Class)
	if err != nil {
		return classificationFilters{}, fmt.Errorf("field 'targetWhere': %v", err)
	}

	filters := classificationFilters{
		source:      source,
		trainingSet: trainingSet,
		target:      target,
	}

	if err = c.validateFilters(&params, &filters); err != nil {
		return nil, err
	}

	return filters, nil
}

func (c *Classifier) validateFilters(params *models.Classification, filters *classificationFilters) (err error) {
	if params.Type == TypeKNN {
		if err = c.validateFilter(filters.Source()); err != nil {
			return fmt.Errorf("invalid sourceWhere: %s", err)
		}
		if err = c.validateFilter(filters.TrainingSet()); err != nil {
			return fmt.Errorf("invalid trainingSetWhere: %s", err)
		}
	}

	if params.Type == TypeContextual || params.Type == TypeZeroShot {
		if err = c.validateFilter(filters.Source()); err != nil {
			return fmt.Errorf("invalid sourceWhere: %s", err)
		}
		if err = c.validateFilter(filters.Target()); err != nil {
			return fmt.Errorf("invalid targetWhere: %s", err)
		}
	}

	return
}

func (c *Classifier) validateFilter(filter *libfilters.LocalFilter) error {
	if filter == nil {
		return nil
	}
	return libfilters.ValidateFilters(c.schemaGetter.GetSchemaSkipAuth(), filter)
}

func (c *Classifier) assignNewID(params *models.Classification) error {
	id, err := uuid.NewRandom()
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

func (c *Classifier) parseAndSetDefaults(params *models.Classification) error {
	if params.Type == "" {
		defaultType := "knn"
		params.Type = defaultType
	}

	if params.Type == "knn" {
		if err := c.parseKNNSettings(params); err != nil {
			return errors.Wrapf(err, "parse knn specific settings")
		}
		return nil
	}

	if c.modulesProvider != nil {
		if err := c.modulesProvider.ParseClassifierSettings(params.Type, params); err != nil {
			return errors.Wrapf(err, "parse %s specific settings", params.Type)
		}
		return nil
	}

	return nil
}

func (c *Classifier) parseKNNSettings(params *models.Classification) error {
	raw := params.Settings
	settings := &ParamsKNN{}
	if raw == nil {
		settings.SetDefaults()
		params.Settings = settings
		return nil
	}

	asMap, ok := raw.(map[string]interface{})
	if !ok {
		return errors.Errorf("settings must be an object got %T", raw)
	}

	v, err := extractNumberFromMap(asMap, "k")
	if err != nil {
		return err
	}
	settings.K = v

	settings.SetDefaults()
	params.Settings = settings

	return nil
}

type ParamsKNN struct {
	K *int32 `json:"k"`
}

func (params *ParamsKNN) SetDefaults() {
	if params.K == nil {
		defaultK := int32(3)
		params.K = &defaultK
	}
}

func extractNumberFromMap(in map[string]interface{}, field string) (*int32, error) {
	unparsed, present := in[field]
	if present {
		parsed, ok := unparsed.(json.Number)
		if !ok {
			return nil, errors.Errorf("settings.%s must be number, got %T",
				field, unparsed)
		}

		asInt64, err := parsed.Int64()
		if err != nil {
			return nil, errors.Wrapf(err, "settings.%s", field)
		}

		asInt32 := int32(asInt64)
		return &asInt32, nil
	}

	return nil, nil
}
