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
	tfidf   tfidfScorer
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
	pctx := contextualPreparationContext{
		targets: map[string]search.Results{},
	}

	for _, targetProp := range p.params.ClassifyProperties {
		class, kind, err := p.classAndKindOfTarget(targetProp)
		if err != nil {
			return pctx, fmt.Errorf("target prop '%s': find target class and kind: %v", targetProp, err)
		}

		targets, err := p.findTargets(class, kind)
		if err != nil {
			return pctx, fmt.Errorf("target prop '%s': find targets: %v", targetProp, err)
		}

		pctx.targets[targetProp] = targets
	}

	return pctx, nil
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
