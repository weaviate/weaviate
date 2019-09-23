package classification

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
)

func (c *Classifier) run(params models.Classification) {
	unclassifiedItems, err := c.vectorRepo.GetUnclassified(context.Background(),
		params.Class, params.ClassifyProperties)
	if err != nil {
		// TODO: mark run as failed
	}

	if unclassifiedItems == nil {
		// no work to do
		return
	}

	for _, item := range *unclassifiedItems {
		err := c.classifyItem(item, params)
		if err != nil {
			// TODO: mark run as failed
		}

	}

}

func (c *Classifier) classifyItem(item search.Result, params models.Classification) error {
	// K is guaranteed to be set by now, no danger in dereferencing the pointer
	res, err := c.vectorRepo.AggregateNeighbors(context.Background(), item.Vector, item.ClassName,
		params.ClassifyProperties, int(*params.K))

	if err != nil {
		return fmt.Errorf("classify %s/%s: %v", item.ClassName, item.ID, err)
	}

	for _, agg := range res {
		item.Schema.(map[string]interface{})[agg.Property] = models.MultipleRef{
			&models.SingleRef{Beacon: agg.Beacon},
		}
	}

	err = c.store(item)
	if err != nil {
		return fmt.Errorf("store %s/%s: %v", item.ClassName, item.ID, err)
	}

	return nil

}

func (c *Classifier) store(item search.Result) error {
	switch item.Kind {
	case kind.Thing:
		return c.vectorRepo.PutThing(context.Background(), item.Thing(), item.Vector)
	case kind.Action:
		return c.vectorRepo.PutAction(context.Background(), item.Action(), item.Vector)
	default:
		return fmt.Errorf("impossible kind")
	}
}
