package db

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/filters"
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
	idx := d.GetIndex(object.Kind(), object.Class())
	if idx == nil {
		return fmt.Errorf("tried to import into non-existing index for %s/%s", object.kind, object.Class())
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
	panic("not implemented") // TODO: Implement
}

func (d *DB) ActionByID(ctx context.Context, id strfmt.UUID, props traverser.SelectProperties, meta bool) (*search.Result, error) {
	panic("not implemented") // TODO: Implement
}

func (d *DB) ThingSearch(ctx context.Context, limit int, filters *filters.LocalFilter, meta bool) (search.Results, error) {
	panic("not implemented") // TODO: Implement
}

func (d *DB) ActionSearch(ctx context.Context, limit int, filters *filters.LocalFilter, meta bool) (search.Results, error) {
	panic("not implemented") // TODO: Implement
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
