//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/weaviate/weaviate/entities/storobj"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type DB struct {
	logger          logrus.FieldLogger
	schemaGetter    schemaUC.SchemaGetter
	config          Config
	indices         map[string]*Index
	remoteIndex     sharding.RemoteIndexClient
	replicaClient   replica.Client
	nodeResolver    nodeResolver
	remoteNode      *sharding.RemoteNode
	promMetrics     *monitoring.PrometheusMetrics
	shutdown        chan struct{}
	startupComplete atomic.Bool

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
	// See also: https://github.com/weaviate/weaviate/issues/2351
	indexLock sync.RWMutex

	jobQueueCh          chan job
	shutDownWg          sync.WaitGroup
	maxNumberGoroutines int
}

func (d *DB) SetSchemaGetter(sg schemaUC.SchemaGetter) {
	d.schemaGetter = sg
}

func (d *DB) WaitForStartup(ctx context.Context) error {
	err := d.init(ctx)
	if err != nil {
		return err
	}

	d.startupComplete.Store(true)
	d.scanResourceUsage()

	return nil
}

func (d *DB) StartupComplete() bool { return d.startupComplete.Load() }

func New(logger logrus.FieldLogger, config Config,
	remoteIndex sharding.RemoteIndexClient, nodeResolver nodeResolver,
	remoteNodesClient sharding.RemoteNodeClient, replicaClient replica.Client,
	promMetrics *monitoring.PrometheusMetrics,
) (*DB, error) {
	db := &DB{
		logger:              logger,
		config:              config,
		indices:             map[string]*Index{},
		remoteIndex:         remoteIndex,
		nodeResolver:        nodeResolver,
		remoteNode:          sharding.NewRemoteNode(nodeResolver, remoteNodesClient),
		replicaClient:       replicaClient,
		promMetrics:         promMetrics,
		shutdown:            make(chan struct{}),
		jobQueueCh:          make(chan job, 100000),
		maxNumberGoroutines: int(math.Round(config.MaxImportGoroutinesFactor * float64(runtime.GOMAXPROCS(0)))),
	}
	if db.maxNumberGoroutines == 0 {
		return db, errors.New("no workers to add batch-jobs configured.")
	}
	db.shutDownWg.Add(db.maxNumberGoroutines)
	for i := 0; i < db.maxNumberGoroutines; i++ {
		go db.worker()
	}

	return db, nil
}

type Config struct {
	RootPath                  string
	QueryLimit                int64
	QueryMaximumResults       int64
	ResourceUsage             config.ResourceUsage
	MaxImportGoroutinesFactor float64
	MemtablesFlushIdleAfter   int
	MemtablesInitialSizeMB    int
	MemtablesMaxSizeMB        int
	MemtablesMinActiveSeconds int
	MemtablesMaxActiveSeconds int
	TrackVectorDimensions     bool
	ServerVersion             string
	GitHash                   string
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

	// shut down the workers that add objects to
	for i := 0; i < d.maxNumberGoroutines; i++ {
		d.jobQueueCh <- job{
			index: -1,
		}
	}

	d.indexLock.Lock()
	defer d.indexLock.Unlock()
	for id, index := range d.indices {
		if err := index.Shutdown(ctx); err != nil {
			return errors.Wrapf(err, "shutdown index %q", id)
		}
	}

	d.shutDownWg.Wait() // wait until job queue shutdown is completed

	return nil
}

func (d *DB) worker() {
	for jobToAdd := range d.jobQueueCh {
		if jobToAdd.index < 0 {
			d.shutDownWg.Done()
			return
		}
		jobToAdd.batcher.storeSingleObjectInAdditionalStorage(jobToAdd.ctx, jobToAdd.object, jobToAdd.status, jobToAdd.index)
		jobToAdd.batcher.wg.Done()
	}
}

type job struct {
	object  *storobj.Object
	status  objectInsertStatus
	index   int
	ctx     context.Context
	batcher *objectsBatcher
}
