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

package classification

import (
	"fmt"
	"time"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
)

func (c *Classifier) classifyItemUsingKNN(item search.Result, itemIndex int, kind kind.Kind,
	params models.Classification, filters filters) error {
	ctx, cancel := contextWithTimeout(2 * time.Second)
	defer cancel()

	// K is guaranteed to be set by now, no danger in dereferencing the pointer
	res, err := c.vectorRepo.AggregateNeighbors(ctx, item.Vector,
		kind, item.ClassName,
		params.ClassifyProperties, int(*params.K), filters.trainingSet)
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
				Classification: &models.ReferenceMetaClassification{
					WinningDistance: float64(agg.WinningDistance),
					LosingDistance:  losingDistance,
				},
				Meta: &models.ReferenceMeta{ // deprecated TODO: remove for v1.0.0
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
	// TODO: send back over channel for batched storage
	err = c.store(item)
	if err != nil {
		fmt.Printf("store %s/%s: %v", item.ClassName, item.ID, err)
	}

	return nil
}
