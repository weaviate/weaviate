package db

import (
	"context"

	"github.com/semi-technologies/weaviate/usecases/kinds"
)

func (db *DB) BatchPutThings(ctx context.Context, things kinds.BatchThings) (kinds.BatchThings, error) {
	return nil, nil
}
func (db *DB) BatchPutActions(ctx context.Context, actions kinds.BatchActions) (kinds.BatchActions, error) {
	return nil, nil
}
func (db *DB) AddBatchReferences(ctx context.Context, references kinds.BatchReferences) (kinds.BatchReferences, error) {
	return nil, nil
}
