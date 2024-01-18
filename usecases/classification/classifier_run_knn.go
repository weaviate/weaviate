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

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
)

func (c *Classifier) classifyItemUsingKNN(item search.Result, itemIndex int,
	params models.Classification, filters Filters, writer Writer,
) error {
	ctx, cancel := contextWithTimeout(2 * time.Second)
	defer cancel()

	// this type assertion is safe to make, since we have passed the parsing stage
	settings := params.Settings.(*ParamsKNN)

	// K is guaranteed to be set by now, no danger in dereferencing the pointer
	res, err := c.vectorRepo.AggregateNeighbors(ctx, item.Vector,
		item.ClassName,
		params.ClassifyProperties, int(*settings.K), filters.TrainingSet())
	if err != nil {
		return fmt.Errorf("classify %s/%s: %v", item.ClassName, item.ID, err)
	}

	var classified []string

	for _, agg := range res {
		meta := agg.Meta()
		item.Schema.(map[string]interface{})[agg.Property] = models.MultipleRef{
			&models.SingleRef{
				Beacon:         agg.Beacon,
				Classification: meta,
			},
		}

		// append list of actually classified (can differ from scope!) properties,
		// so we can build the object meta information
		classified = append(classified, agg.Property)
	}

	c.extendItemWithObjectMeta(&item, params, classified)
	err = writer.Store(item)
	if err != nil {
		return fmt.Errorf("store %s/%s: %v", item.ClassName, item.ID, err)
	}

	return nil
}
