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

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
)

func (c *Classifier) classifyItemUsingKNN(item search.Result, kind kind.Kind, params models.Classification) error {
	ctx, cancel := contextWithTimeout(2 * time.Second)
	defer cancel()

	// K is guaranteed to be set by now, no danger in dereferencing the pointer
	res, err := c.vectorRepo.AggregateNeighbors(ctx, item.Vector,
		kind, item.ClassName,
		params.ClassifyProperties, int(*params.K))

	if err != nil {
		return fmt.Errorf("classify %s/%s: %v", item.ClassName, item.ID, err)
	}

	var classified []string

	for _, agg := range res {
		var losingDistance *float64
		if agg.LosingDistance != nil {
			d := float64(*agg.LosingDistance)
			losingDistance = &d
		}
		item.Schema.(map[string]interface{})[agg.Property] = models.MultipleRef{
			&models.SingleRef{
				Beacon: agg.Beacon,
				Meta: &models.ReferenceMeta{
					Classification: &models.ReferenceMetaClassification{
						WinningDistance: float64(agg.WinningDistance),
						LosingDistance:  losingDistance,
					},
				},
			},
		}

		// append list of actually classified (can differ from scope!) properties,
		// so we can build the object meta information
		classified = append(classified, agg.Property)
	}

	c.extendItemWithObjectMeta(&item, params, classified)
	err = c.store(item)
	if err != nil {
		return fmt.Errorf("store %s/%s: %v", item.ClassName, item.ID, err)
	}

	return nil
}
