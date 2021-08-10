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
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
)

// nolint:unused // TODO
func (c *Classifier) classifyItemUsingZeroShot(item search.Result, itemIndex int,
	params models.Classification, filters Filters, writer Writer) error {
	ctx, cancel := contextWithTimeout(2 * time.Second)
	defer cancel()

	properties := params.ClassifyProperties

	s := c.schemaGetter.GetSchemaSkipAuth()
	class := s.GetClass(schema.ClassName(item.ClassName))

	classifyProp := []string{}
	for _, prop := range properties {
		for _, classProp := range class.Properties {
			if classProp.Name == prop {
				classifyProp = append(classifyProp, classProp.DataType...)
			}
		}
	}

	var classified []string
	for _, className := range classifyProp {
		for _, prop := range properties {
			res, err := c.vectorRepo.ZeroShotSearch(ctx, item.Vector, className,
				params.ClassifyProperties, filters.TrainingSet())
			if err != nil {
				return errors.Wrap(err, "zeroshot: search")
			}

			if len(res) > 0 {
				cref := crossref.New("localhost", res[0].ID)
				item.Schema.(map[string]interface{})[prop] = models.MultipleRef{
					&models.SingleRef{
						Beacon:         cref.SingleRef().Beacon,
						Classification: &models.ReferenceMetaClassification{},
					},
				}
				classified = append(classified, prop)
			}
		}
	}

	c.extendItemWithObjectMeta(&item, params, classified)
	err := writer.Store(item)
	if err != nil {
		return errors.Errorf("store %s/%s: %v", item.ClassName, item.ID, err)
	}

	return nil
}
