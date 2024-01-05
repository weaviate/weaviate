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
	"fmt"
	"time"

	libfilters "github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	libclassification "github.com/weaviate/weaviate/usecases/classification"
)

type tfidfScorer interface {
	GetAllTerms(docIndex int) []TermWithTfIdf
}

type contextualPreparationContext struct {
	tfidf   map[string]tfidfScorer    // map[basedOnProp]scorer
	targets map[string]search.Results // map[classifyProp]targets
}

func (c *Classifier) prepareContextualClassification(schema schema.Schema,
	vectorRepo modulecapabilities.VectorClassSearchRepo, params models.Classification,
	filters libclassification.Filters, items search.Results,
) (contextualPreparationContext, error) {
	p := &contextualPreparer{
		inputItems: items,
		params:     params,
		repo:       vectorRepo,
		filters:    filters,
		schema:     schema,
	}

	return p.do()
}

type contextualPreparer struct {
	inputItems []search.Result
	params     models.Classification
	repo       modulecapabilities.VectorClassSearchRepo
	filters    libclassification.Filters
	schema     schema.Schema
}

func (p *contextualPreparer) do() (contextualPreparationContext, error) {
	pctx := contextualPreparationContext{}

	targets, err := p.findTargetsForProps()
	if err != nil {
		return pctx, err
	}

	pctx.targets = targets

	tfidf, err := p.calculateTfidfForProps()
	if err != nil {
		return pctx, err
	}

	pctx.tfidf = tfidf

	return pctx, nil
}

func (p *contextualPreparer) calculateTfidfForProps() (map[string]tfidfScorer, error) {
	props := map[string]tfidfScorer{}

	for _, basedOnName := range p.params.BasedOnProperties {
		calc := NewTfIdfCalculator(len(p.inputItems))
		for _, obj := range p.inputItems {
			schemaMap, ok := obj.Schema.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("no or incorrect schema map present on source object '%s': %T", obj.ID, obj.Schema)
			}

			var docCorpus string
			if basedOn, ok := schemaMap[basedOnName]; ok {
				basedOnString, ok := basedOn.(string)
				if !ok {
					return nil, fmt.Errorf("property '%s' present on %s, but of unexpected type: want string, got %T",
						basedOnName, obj.ID, basedOn)
				}

				docCorpus = basedOnString
			}

			calc.AddDoc(docCorpus)
		}

		calc.Calculate()
		props[basedOnName] = calc
	}

	return props, nil
}

func (p *contextualPreparer) findTargetsForProps() (map[string]search.Results, error) {
	targetsMap := map[string]search.Results{}

	for _, targetProp := range p.params.ClassifyProperties {
		class, err := p.classAndKindOfTarget(targetProp)
		if err != nil {
			return nil, fmt.Errorf("target prop '%s': find target class: %v", targetProp, err)
		}

		targets, err := p.findTargets(class)
		if err != nil {
			return nil, fmt.Errorf("target prop '%s': find targets: %v", targetProp, err)
		}

		targetsMap[targetProp] = targets
	}

	return targetsMap, nil
}

func (p *contextualPreparer) findTargets(class schema.ClassName) (search.Results, error) {
	ctx, cancel := contextWithTimeout(30 * time.Second)
	defer cancel()
	res, err := p.repo.VectorClassSearch(ctx, modulecapabilities.VectorClassSearchParams{
		Filters: p.filters.Target(),
		Pagination: &libfilters.Pagination{
			Limit: 10000,
		},
		ClassName:  string(class),
		Properties: []string{"id"},
	})
	if err != nil {
		return nil, fmt.Errorf("search closest target: %v", err)
	}

	if len(res) == 0 {
		return nil, fmt.Errorf("no potential targets found of class '%s'", class)
	}

	return res, nil
}

func (p *contextualPreparer) classAndKindOfTarget(propName string) (schema.ClassName, error) {
	prop, err := p.schema.GetProperty(schema.ClassName(p.params.Class), schema.PropertyName(propName))
	if err != nil {
		return "", fmt.Errorf("get target prop '%s': %v", propName, err)
	}

	dataType, err := p.schema.FindPropertyDataType(prop.DataType)
	if err != nil {
		return "", fmt.Errorf("extract dataType of prop '%s': %v", propName, err)
	}

	// we have passed validation, so it is safe to assume that this is a ref prop
	targetClasses := dataType.Classes()

	// len=1 is guaranteed from validation
	targetClass := targetClasses[0]

	return targetClass, nil
}
