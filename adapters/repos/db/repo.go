package db

import (
	"context"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
)

type DB struct {
	logger logrus.FieldLogger
}

func (d *DB) SetSchemaGetter(sg schema.SchemaGetter) {}

func (d *DB) WaitForStartup(time.Duration) error {
	return nil
}

func (d *DB) PutThing(ctx context.Context, concept *models.Thing, vector []float32) error {
	panic("not implemented") // TODO: Implement
}

func (d *DB) PutAction(ctx context.Context, concept *models.Action, vector []float32) error {
	panic("not implemented") // TODO: Implement
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

func New(logger logrus.FieldLogger) *DB {
	return &DB{logger: logger}
}
