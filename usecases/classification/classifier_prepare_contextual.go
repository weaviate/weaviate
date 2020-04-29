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
	"fmt"
	"time"

	libfilters "github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

type tfidfScorer interface {
	GetAllTerms(docIndex int) []TermWithTfIdf
}

type contextualPreparationContext struct {
	tfidf   map[string]tfidfScorer    // map[basedOnProp]scorer
	targets map[string]search.Results // map[classifyProp]targets
}

func (c *Classifier) prepareContextualClassification(kind kind.Kind, params models.Classification,
	filters filters, items search.Results) (contextualPreparationContext, error) {
	schema := c.schemaGetter.GetSchemaSkipAuth()
	p := &contextualPreparer{
		inputItems: items,
		params:     params,
		repo:       c.vectorRepo,
		filters:    filters,
		kind:       kind,
		schema:     schema,
	}

	return p.do()
}

type contextualPreparer struct {
	inputItems []search.Result
	params     models.Classification
	repo       vectorRepo
	filters    filters
	kind       kind.Kind
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
		class, kind, err := p.classAndKindOfTarget(targetProp)
		if err != nil {
			return nil, fmt.Errorf("target prop '%s': find target class and kind: %v", targetProp, err)
		}

		targets, err := p.findTargets(class, kind)
		if err != nil {
			return nil, fmt.Errorf("target prop '%s': find targets: %v", targetProp, err)
		}

		targetsMap[targetProp] = targets
	}

	return targetsMap, nil
}

func (p *contextualPreparer) findTargets(class schema.ClassName, kind kind.Kind) (search.Results, error) {
	ctx, cancel := contextWithTimeout(30 * time.Second)
	defer cancel()
	res, err := p.repo.VectorClassSearch(ctx, traverser.GetParams{
		Filters: p.filters.target,
		Pagination: &libfilters.Pagination{
			Limit: 10000,
		},
		ClassName: string(class),
		Kind:      kind,
		Properties: traverser.SelectProperties{
			traverser.SelectProperty{
				Name: "uuid",
			},
		},
	})

	if err != nil {
		return nil, fmt.Errorf("search closest target: %v", err)
	}

	if res == nil || len(res) == 0 {
		return nil, fmt.Errorf("no potential targets found of class '%s' (%s)", class, kind)
	}

	return res, nil
}

func (p *contextualPreparer) classAndKindOfTarget(propName string) (schema.ClassName, kind.Kind, error) {
	prop, err := p.schema.GetProperty(p.kind, schema.ClassName(p.params.Class), schema.PropertyName(propName))
	if err != nil {
		return "", "", fmt.Errorf("get target prop '%s': %v", propName, err)
	}

	dataType, err := p.schema.FindPropertyDataType(prop.DataType)
	if err != nil {
		return "", "", fmt.Errorf("extract dataType of prop '%s': %v", propName, err)
	}

	// we have passed validation, so it is safe to assume that this is a ref prop
	targetClasses := dataType.Classes()

	// len=1 is guaranteed from validation
	targetClass := targetClasses[0]
	targetKind, _ := p.schema.GetKindOfClass(targetClass)

	return targetClass, targetKind, nil
}
