//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
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
	promMetrics  *monitoring.PrometheusMetrics
	shutdown     chan struct{}
	appConfig    config.Config

	indexLock sync.Mutex
}

func (d *DB) SetSchemaGetter(sg schemaUC.SchemaGetter) {
	d.schemaGetter = sg
}

func (d *DB) WaitForStartup(ctx context.Context) error {
	err := d.init(ctx)
	if err != nil {
		return err
	}

	d.scanDiskUse()

	return nil
}

func New(logger logrus.FieldLogger, config Config,
	remoteClient sharding.RemoteIndexClient, nodeResolver nodeResolver,
	promMetrics *monitoring.PrometheusMetrics, appConfig config.Config,
) *DB {
	return &DB{
		logger:       logger,
		config:       config,
		indices:      map[string]*Index{},
		remoteClient: remoteClient,
		nodeResolver: nodeResolver,
		promMetrics:  promMetrics,
		shutdown:     make(chan struct{}),
		appConfig:    appConfig,
	}
}

type Config struct {
	RootPath                  string
	QueryLimit                int64
	QueryMaximumResults       int64
	DiskUseWarningPercentage  uint64
	DiskUseReadOnlyPercentage uint64
	MaxImportGoroutinesFactor float64
	NodeName                  string
}

// GetIndex returns the index if it exists or nil if it doesn't
func (d *DB) GetIndex(className schema.ClassName) *Index {
	d.indexLock.Lock()
	defer d.indexLock.Unlock()

	id := indexID(className)
	index, ok := d.indices[id]
	if !ok {
		return nil
	}

	return index
}

// GetIndexForIncoming returns the index if it exists or nil if it doesn't
func (d *DB) GetIndexForIncoming(className schema.ClassName) sharding.RemoteIndexIncomingRepo {
	d.indexLock.Lock()
	defer d.indexLock.Unlock()

	id := indexID(className)
	index, ok := d.indices[id]
	if !ok {
		return nil
	}

	return index
}

// DeleteIndex deletes the index
func (d *DB) DeleteIndex(className schema.ClassName) error {
	d.indexLock.Lock()
	defer d.indexLock.Unlock()

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
	d.shutdown <- struct{}{}

	for id, index := range d.indices {
		if err := index.Shutdown(ctx); err != nil {
			return errors.Wrapf(err, "shutdown index %q", id)
		}
	}

	return nil
}
