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

package classification

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/filterext"
	libfilters "github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/objects"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	libvectorizer "github.com/semi-technologies/weaviate/usecases/vectorizer"
	"github.com/sirupsen/logrus"
)

type distancer func(a, b []float32) (float32, error)

type Classifier struct {
	schemaGetter    schemaUC.SchemaGetter
	repo            Repo
	vectorRepo      vectorRepo
	authorizer      authorizer
	distancer       distancer
	modulesProvider ModulesProvider
	logger          logrus.FieldLogger
}

type vectorizer interface {
	// MultiVectorForWord must keep order, if an item cannot be vectorized, the
	// element should be explicit nil, not skipped
	MultiVectorForWord(ctx context.Context, words []string) ([][]float32, error)

	VectorOnlyForCorpi(ctx context.Context, corpi []string, overrides map[string]string) ([]float32, error)
}

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

type ModulesProvider interface {
	GetVectorizer(name string) modulecapabilities.VectorizerClient
}

func New(sg schemaUC.SchemaGetter, cr Repo, vr vectorRepo, authorizer authorizer,
	logger logrus.FieldLogger, modulesProvider ModulesProvider) *Classifier {
	return &Classifier{
		logger:          logger,
		schemaGetter:    sg,
		repo:            cr,
		vectorRepo:      vr,
		authorizer:      authorizer,
		distancer:       libvectorizer.NormalizedDistance,
		modulesProvider: modulesProvider,
	}
}

// Repo to manage classification state, should be consistent, not used to store
// acutal data object vectors, see VectorRepo
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
	VectorClassSearch(ctx context.Context, params traverser.GetParams) ([]search.Result, error)
}

type vectorRepo interface {
	VectorRepo
	BatchPutObjects(ctx context.Context, things objects.BatchObjects) (objects.BatchObjects, error)
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
	filters, err := extractFilters(params)
	if err != nil {
		return nil, err
	}

	go c.run(params, filters)

	return &params, nil
}

func (c *Classifier) getVectorizer(name string) vectorizer {
	if c.modulesProvider != nil {
		return c.modulesProvider.GetVectorizer(name)
	}
	return nil
}

func extractFilters(params models.Classification) (filters, error) {
	if params.Filters == nil {
		return filters{}, nil
	}

	source, err := filterext.Parse(params.Filters.SourceWhere)
	if err != nil {
		return filters{}, fmt.Errorf("field 'sourceWhere': %v", err)
	}

	trainingSet, err := filterext.Parse(params.Filters.TrainingSetWhere)
	if err != nil {
		return filters{}, fmt.Errorf("field 'trainingSetWhere': %v", err)
	}

	target, err := filterext.Parse(params.Filters.TargetWhere)
	if err != nil {
		return filters{}, fmt.Errorf("field 'targetWhere': %v", err)
	}

	return filters{
		source:      source,
		trainingSet: trainingSet,
		target:      target,
	}, nil
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

func (c *Classifier) parseAndSetDefaults(params *models.Classification) error {
	if params.Type == "" {
		defaultType := "knn"
		params.Type = defaultType
	}

	if params.Type == "knn" {
		if err := c.parseKNNSettings(params); err != nil {
			return errors.Wrapf(err, "parse knn specific settings")
		}
	}

	// TODO: This must be done as part of the module for full modularization
	if params.Type == "text2vec-contextionary-contextual" {
		if err := c.parseContextualSettings(params); err != nil {
			return errors.Wrapf(err, "parse knn specific settings")
		}
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

func (c *Classifier) parseContextualSettings(params *models.Classification) error {
	raw := params.Settings
	settings := &ParamsContextual{}
	if raw == nil {
		settings.SetDefaults()
		params.Settings = settings
		return nil
	}

	asMap, ok := raw.(map[string]interface{})
	if !ok {
		return errors.Errorf("settings must be an object got %T", raw)
	}

	v, err := extractNumberFromMap(asMap, "minimumUsableWords")
	if err != nil {
		return err
	}
	settings.MinimumUsableWords = v

	v, err = extractNumberFromMap(asMap, "informationGainCutoffPercentile")
	if err != nil {
		return err
	}
	settings.InformationGainCutoffPercentile = v

	v, err = extractNumberFromMap(asMap, "informationGainMaximumBoost")
	if err != nil {
		return err
	}
	settings.InformationGainMaximumBoost = v

	v, err = extractNumberFromMap(asMap, "tfidfCutoffPercentile")
	if err != nil {
		return err
	}
	settings.TfidfCutoffPercentile = v

	settings.SetDefaults()
	params.Settings = settings

	return nil
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

type ParamsKNN struct {
	K *int32 `json:"k"`
}

func (params *ParamsKNN) SetDefaults() {
	if params.K == nil {
		defaultK := int32(3)
		params.K = &defaultK
	}
}

// TODO: this must be provided by the module when actual modularization occurs
type ParamsContextual struct {
	MinimumUsableWords              *int32 `json:"minimumUsableWords"`
	InformationGainCutoffPercentile *int32 `json:"informationGainCutoffPercentile"`
	InformationGainMaximumBoost     *int32 `json:"informationGainMaximumBoost"`
	TfidfCutoffPercentile           *int32 `json:"tfidfCutoffPercentile"`
}

func (params *ParamsContextual) SetDefaults() {
	if params.MinimumUsableWords == nil {
		defaultParam := int32(3)
		params.MinimumUsableWords = &defaultParam
	}

	if params.InformationGainCutoffPercentile == nil {
		defaultParam := int32(50)
		params.InformationGainCutoffPercentile = &defaultParam
	}

	if params.InformationGainMaximumBoost == nil {
		defaultParam := int32(3)
		params.InformationGainMaximumBoost = &defaultParam
	}

	if params.TfidfCutoffPercentile == nil {
		defaultParam := int32(80)
		params.TfidfCutoffPercentile = &defaultParam
	}
}
