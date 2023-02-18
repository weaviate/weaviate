package db

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/userindex"
)

func (db *DB) UserIndexStatus(ctx context.Context,
	className string,
) ([]userindex.Index, error) {
	ind := db.GetIndex(schema.ClassName(className))
	if ind == nil {
		return nil, fmt.Errorf("class %s not found", className)
	}

	out := ind.userIndexStatus.List()

	return out, nil
}
