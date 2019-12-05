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

	"github.com/go-openapi/strfmt"
	libfilters "github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

type contextualItemClassifier struct {
	item       search.Result
	kind       kind.Kind
	params     models.Classification
	classifier *Classifier
	schema     schema.Schema
}

func (c *Classifier) classifyItemContextual(item search.Result, kind kind.Kind, params models.Classification) error {
	schema := c.schemaGetter.GetSchemaSkipAuth()
	run := &contextualItemClassifier{
		item:       item,
		kind:       kind,
		params:     params,
		classifier: c,
		schema:     schema,
	}

	err := run.do()
	if err != nil {
		return fmt.Errorf("contextual: %v", err)
	}

	return nil
}

func (c *contextualItemClassifier) do() error {
	var classified []string
	for _, propName := range c.params.ClassifyProperties {
		current, err := c.property(propName)
		if err != nil {
			return fmt.Errorf("prop '%s': %v", propName, err)
		}

		// append list of actually classified (can differ from scope!) properties,
		// so we can build the object meta information
		classified = append(classified, current)
	}

	c.classifier.extendItemWithObjectMeta(&c.item, c.params, classified)
	err := c.classifier.store(c.item)
	if err != nil {
		return fmt.Errorf("store %s/%s: %v", c.item.ClassName, c.item.ID, err)
	}

	return nil
}

func (c *contextualItemClassifier) property(propName string) (string, error) {
	targetClass, targetKind, err := c.classAndKindOfTarget(propName)
	if err != nil {
		return "", fmt.Errorf("inspect target: %v", err)
	}

	res, err := c.findTarget(targetClass, targetKind)
	if err != nil {
		return "", fmt.Errorf("find target: %v", err)
	}

	distance, err := c.distance(res.Vector)
	if err != nil {
		return "", fmt.Errorf("calculate distance: %v", err)
	}

	targetBeacon := crossref.New("localhost", res.ID, res.Kind).String()
	c.item.Schema.(map[string]interface{})[propName] = models.MultipleRef{
		&models.SingleRef{
			Beacon: strfmt.URI(targetBeacon),
			Meta: &models.ReferenceMeta{
				Classification: &models.ReferenceMetaClassification{
					WinningDistance: distance,
				},
			},
		},
	}

	return propName, nil
}

func (c *contextualItemClassifier) classAndKindOfTarget(propName string) (schema.ClassName, kind.Kind, error) {
	prop, err := c.schema.GetProperty(c.kind, schema.ClassName(c.params.Class), schema.PropertyName(propName))
	if err != nil {
		return "", "", fmt.Errorf("get target prop '%s': %v", propName, err)
	}

	dataType, err := c.schema.FindPropertyDataType(prop.DataType)
	if err != nil {
		return "", "", fmt.Errorf("extract dataType of prop '%s': %v", propName, err)
	}

	// we have passed validation, so it is safe to assume that this is a ref prop
	targetClasses := dataType.Classes()

	// len=1 is guaranteed from validation
	targetClass := targetClasses[0]
	targetKind, _ := c.schema.GetKindOfClass(targetClass)

	return targetClass, targetKind, nil
}

func (c *contextualItemClassifier) findTarget(targetClass schema.ClassName, targetKind kind.Kind) (*search.Result, error) {
	ctx, cancel := contextWithTimeout(2 * time.Second)
	defer cancel()

	res, err := c.classifier.vectorRepo.VectorClassSearch(ctx, traverser.GetParams{
		SearchVector: c.item.Vector,
		ClassName:    targetClass.String(),
		Kind:         targetKind,
		Pagination: &libfilters.Pagination{
			Limit: 1,
		},
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
		return nil, fmt.Errorf("no potential targets found of class '%s' (%s)", targetClass, targetKind)
	}

	return &res[0], nil
}

func (c *contextualItemClassifier) distance(target []float32) (float64, error) {
	dist, err := c.classifier.distancer(c.item.Vector, target)
	return float64(dist), err
}
