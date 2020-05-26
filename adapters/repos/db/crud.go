package db

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (d *DB) PutThing(ctx context.Context, object *models.Thing, vector []float32) error {
	return d.putObject(ctx, NewKindObjectFromThing(object, vector))
}

func (d *DB) PutAction(ctx context.Context, object *models.Action, vector []float32) error {
	return d.putObject(ctx, NewKindObjectFromAction(object, vector))
}

func (d *DB) putObject(ctx context.Context, object *KindObject) error {
	idx := d.GetIndex(object.Kind, object.Class())
	if idx == nil {
		return fmt.Errorf("tried to import into non-existing index for %s/%s", object.Kind, object.Class())
	}

	err := idx.putObject(ctx, object)
	if err != nil {
		return errors.Wrapf(err, "import into index %s", idx.ID())
	}

	return nil

}

func (d *DB) DeleteAction(ctx context.Context, className string, id strfmt.UUID) error {
	panic("not implemented") // TODO: Implement
}

func (d *DB) DeleteThing(ctx context.Context, className string, id strfmt.UUID) error {
	panic("not implemented") // TODO: Implement
}

func (d *DB) ThingByID(ctx context.Context, id strfmt.UUID, props traverser.SelectProperties, meta bool) (*search.Result, error) {
	return d.objectByID(ctx, kind.Thing, id, props, meta)
}

func (d *DB) ActionByID(ctx context.Context, id strfmt.UUID, props traverser.SelectProperties, meta bool) (*search.Result, error) {
	return d.objectByID(ctx, kind.Action, id, props, meta)
}

// objectByID checks every index of the particular kind for the ID
func (d *DB) objectByID(ctx context.Context, kind kind.Kind, id strfmt.UUID, props traverser.SelectProperties, meta bool) (*search.Result, error) {

	// TODO: Search in parallel, rather than sequentially or this will be
	// painfully slow on large schemas
	for _, index := range d.indices {
		if index.Config.Kind != kind {
			continue
		}

		res, err := index.objectByID(ctx, id, props, meta)
		if err != nil {
			return nil, errors.Wrapf(err, "search index %s", index.ID())
		}

		if res != nil {
			return res.SearchResult(), nil
		}
	}

	return nil, nil
}

func (d *DB) Exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	panic("not implemented") // TODO: Implement
}

func (d *DB) AddReference(ctx context.Context, kind kind.Kind, source strfmt.UUID, propName string, ref *models.SingleRef) error {
	panic("not implemented") // TODO: Implement
}

func (d *DB) Merge(ctx context.Context, merge kinds.MergeDocument) error {
	panic("not implemented") // TODO: Implement
}
