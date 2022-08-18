package scaling

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

type ScaleOutManager struct {
	// the scaleOutManager needs to read and updated the sharding state of a
	// class. It can access it through the schemaManager
	schemaManager SchemaManager
}

func NewScaleOutManager() *ScaleOutManager {
	return &ScaleOutManager{}
}

type SchemaManager interface{}

func (som *ScaleOutManager) SetSchemaManager(sm SchemaManager) {
	som.schemaManager = sm
}

func (som *ScaleOutManager) Scale(ctx context.Context, className string,
	old, updated sharding.Config,
) error {
	return errors.Errorf("not implemented yet")
}
