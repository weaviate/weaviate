//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/schema"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/sirupsen/logrus"
)

type DB struct {
	logger       logrus.FieldLogger
	schemaGetter schemaUC.SchemaGetter
	config       Config
	indices      map[string]*Index
	remoteClient sharding.RemoteIndexClient
	nodeResolver nodeResolver
}

func (d *DB) SetSchemaGetter(sg schemaUC.SchemaGetter) {
	d.schemaGetter = sg
}

func (d *DB) WaitForStartup(ctx context.Context) error {
	return d.init(ctx)
}

func New(logger logrus.FieldLogger, config Config,
	remoteClient sharding.RemoteIndexClient, nodeResolver nodeResolver) *DB {
	return &DB{
		logger:       logger,
		config:       config,
		indices:      map[string]*Index{},
		remoteClient: remoteClient,
		nodeResolver: nodeResolver,
	}
}

type Config struct {
	RootPath            string
	QueryLimit          int64
	QueryMaximumResults int64
}

// GetIndex returns the index if it exists or nil if it doesn't
func (d *DB) GetIndex(className schema.ClassName) *Index {
	id := indexID(className)
	index, ok := d.indices[id]
	if !ok {
		return nil
	}

	return index
}

// GetIndexForIncoming returns the index if it exists or nil if it doesn't
func (d *DB) GetIndexForIncoming(className schema.ClassName) sharding.RemoteIndexIncomingRepo {
	id := indexID(className)
	index, ok := d.indices[id]
	if !ok {
		return nil
	}

	return index
}

// DeleteIndex deletes the index
func (d *DB) DeleteIndex(className schema.ClassName) error {
	id := indexID(className)
	index, ok := d.indices[id]
	if !ok {
		return errors.Errorf("exist index %s", id)
	}
	err := index.drop()
	if err != nil {
		return errors.Wrapf(err, "drop index %s", id)
	}
	delete(d.indices, id)
	return nil
}

func (d *DB) Shutdown(ctx context.Context) error {
	for id, index := range d.indices {
		if err := index.Shutdown(ctx); err != nil {
			return errors.Wrapf(err, "shutdown index %q", id)
		}
	}

	return nil
}
