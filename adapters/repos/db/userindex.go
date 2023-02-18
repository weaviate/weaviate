package db

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func (db *DB) UserIndexStatus(ctx context.Context,
	className string,
) (*models.IndexStatusList, error) {
	ind := db.GetIndex(schema.ClassName(className))
	if ind == nil {
		return nil, fmt.Errorf("class %s not found", className)
	}

	out := ind.userIndexStatus.ToSwagger()
	out.ClassName = className

	return out, nil
}
