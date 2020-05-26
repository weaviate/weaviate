//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

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
	logger       logrus.FieldLogger
	schemaGetter schema.SchemaGetter
	config       Config
	indices      map[string]*Index
}

func (d *DB) SetSchemaGetter(sg schema.SchemaGetter) {
	d.schemaGetter = sg
}

func (d *DB) WaitForStartup(time.Duration) error {
	return d.init()
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

func New(logger logrus.FieldLogger, config Config) *DB {
	return &DB{
		logger:  logger,
		config:  config,
		indices: map[string]*Index{},
	}
}

type Config struct {
	RootPath string
}
