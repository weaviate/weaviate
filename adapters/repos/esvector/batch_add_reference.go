package esvector

import (
	"context"

	"github.com/semi-technologies/weaviate/usecases/kinds"
)

func (r *Repo) AddBatchReferences(ctx context.Context, list kinds.BatchReferences) (kinds.BatchReferences, error) {
	return list, nil
}
