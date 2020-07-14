//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"time"

	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/sirupsen/logrus"
)

type DB struct {
	logger       logrus.FieldLogger
	schemaGetter schemaUC.SchemaGetter
	config       Config
	indices      map[string]*Index
}

func (d *DB) SetSchemaGetter(sg schemaUC.SchemaGetter) {
	d.schemaGetter = sg
}

func (d *DB) WaitForStartup(time.Duration) error {
	return d.init()
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

// GetIndex returns the index if it exists or nil if it doesn't
func (d *DB) GetIndex(kind kind.Kind, className schema.ClassName) *Index {
	id := indexID(kind, className)
	index, ok := d.indices[id]
	if !ok {
		return nil
	}

	return index
}
