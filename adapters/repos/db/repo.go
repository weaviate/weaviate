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
	"github.com/semi-technologies/weaviate/usecases/replica"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/sirupsen/logrus"
)

type DB struct {
	logger        logrus.FieldLogger
	schemaGetter  schemaUC.SchemaGetter
	config        Config
	indices       map[string]*Index
	remoteIndex   sharding.RemoteIndexClient
	replicaClient replica.ReplicationClient
	nodeResolver  nodeResolver
	remoteNode    *sharding.RemoteNode
	promMetrics   *monitoring.PrometheusMetrics
	shutdown      chan struct{}

	// indexLock is an RWMutex which allows concurrent access to various indexes,
	// but only one modifaction at a time. R/W can be a bit confusing here,
	// because it does not refer to write or read requests from a user's
	// perspetive, but rather:
	//
	// - Read -> The array containing all indexes is read-only. In other words
	// there will never be a race condition from doing something like index :=
	// indexes[0]. What you do with the Index after retrieving it from the array
	// does not matter. Assuming that it is thread-safe (it is) you can
	// read/write from the index itself. Therefore from a user's perspective
	// something like a parallel import batch and a read-query can happen without
	// any problems.
	//
	// - Write -> The index array is being modified, for example, because a new
	// index is added. This is mutually exclusive with the other case (but
	// hopefully very short).
	//
	//
	// See also: https://github.com/semi-technologies/weaviate/issues/2351
	indexLock sync.RWMutex
}

func (d *DB) SetSchemaGetter(sg schemaUC.SchemaGetter) {
	d.schemaGetter = sg
}

func (d *DB) WaitForStartup(ctx context.Context) error {
	err := d.init(ctx)
	if err != nil {
		return err
	}

	d.scanResourceUsage()

	return nil
}

func New(logger logrus.FieldLogger, config Config,
	remoteIndex sharding.RemoteIndexClient, nodeResolver nodeResolver,
	remoteNodesClient sharding.RemoteNodeClient, replicaClient replica.ReplicationClient,
	promMetrics *monitoring.PrometheusMetrics,
) *DB {
	return &DB{
		logger:        logger,
		config:        config,
		indices:       map[string]*Index{},
		remoteIndex:   remoteIndex,
		nodeResolver:  nodeResolver,
		remoteNode:    sharding.NewRemoteNode(nodeResolver, remoteNodesClient),
		replicaClient: replicaClient,
		promMetrics:   promMetrics,
		shutdown:      make(chan struct{}),
	}
}

type Config struct {
	RootPath                         string
	QueryLimit                       int64
	QueryMaximumResults              int64
	ResourceUsage                    config.ResourceUsage
	MaxImportGoroutinesFactor        float64
	FlushIdleAfter                   int
	TrackVectorDimensions            bool
	ReindexVectorDimensionsAtStartup bool
	ServerVersion                    string
	GitHash                          string
}

// GetIndex returns the index if it exists or nil if it doesn't
func (d *DB) GetIndex(className schema.ClassName) *Index {
	d.indexLock.RLock()
	defer d.indexLock.RUnlock()

	id := indexID(className)
	index, ok := d.indices[id]
	if !ok {
		return nil
	}

	return index
}

// GetIndexForIncoming returns the index if it exists or nil if it doesn't
func (d *DB) GetIndexForIncoming(className schema.ClassName) sharding.RemoteIndexIncomingRepo {
	d.indexLock.RLock()
	defer d.indexLock.RUnlock()

	id := indexID(className)
	index, ok := d.indices[id]
	if !ok {
		return nil
	}

	return index
}

func (d *DB) GetReplicatedIndex(className schema.ClassName) Replicator {
	d.indexLock.RLock()
	defer d.indexLock.RUnlock()

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

	d.indexLock.Lock()
	defer d.indexLock.Unlock()
	for id, index := range d.indices {
		if err := index.Shutdown(ctx); err != nil {
			return errors.Wrapf(err, "shutdown index %q", id)
		}
	}

	return nil
}
