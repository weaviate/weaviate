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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/weaviate/weaviate/entities/autocut"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/aggregator"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/sorter"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	"golang.org/x/sync/errgroup"
)

// shardMap is a syn.Map which specialized in storing shards
type shardMap sync.Map

// Range calls f sequentially for each key and value present in the map.
// If f returns an error, range stops the iteration
func (m *shardMap) Range(f func(name string, shard *Shard) error) (err error) {
	(*sync.Map)(m).Range(func(key, value any) bool {
		err = f(key.(string), value.(*Shard))
		return err == nil
	})
	return err
}

// Load returns the shard or nil if no shard is present.
func (m *shardMap) Load(name string) *Shard {
	v, ok := (*sync.Map)(m).Load(name)
	if !ok {
		return nil
	}
	return v.(*Shard)
}

// Store sets a shard giving its name and value
func (m *shardMap) Store(name string, shard *Shard) {
	(*sync.Map)(m).Store(name, shard)
}

// Index is the logical unit which contains all the data for one particular
// class. An index can be further broken up into self-contained units, called
// Shards, to allow for easy distribution across Nodes
type Index struct {
	classSearcher         inverted.ClassSearcher // to allow for nested by-references searches
	shards                shardMap
	Config                IndexConfig
	vectorIndexUserConfig schema.VectorIndexConfig
	getSchema             schemaUC.SchemaGetter
	logger                logrus.FieldLogger
	remote                *sharding.RemoteIndex
	stopwords             *stopwords.Detector
	replicator            *replica.Replicator

	backupState     BackupState
	backupStateLock sync.RWMutex

	invertedIndexConfig     schema.InvertedIndexConfig
	invertedIndexConfigLock sync.Mutex

	tenantKey string

	// This lock should be used together with the db indexLock.
	//
	// The db indexlock locks the map that contains all indices against changes and should be used while iterating.
	// This lock protects this specific index form being deleted while in use. Use Rlock to signale that it is in use.
	// This way many goroutines can use a specific index in parallel. The delete-routine will try to acquire a RWlock.
	//
	// Usage:
	// Lock the whole db using db.indexLock
	// pick the indices you want and Rlock them
	// unlock db.indexLock
	// Use the indices
	// RUnlock all picked indices
	dropIndex sync.RWMutex

	metrics         *Metrics
	centralJobQueue chan job
}

func (i *Index) ID() string {
	return indexID(i.Config.ClassName)
}

type nodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
}

// NewIndex creates an index with the specified amount of shards, using only
// the shards that are local to a node
func NewIndex(ctx context.Context, config IndexConfig,
	shardState *sharding.State, invertedIndexConfig schema.InvertedIndexConfig,
	vectorIndexUserConfig schema.VectorIndexConfig, sg schemaUC.SchemaGetter,
	cs inverted.ClassSearcher, logger logrus.FieldLogger,
	nodeResolver nodeResolver, remoteClient sharding.RemoteIndexClient,
	replicaClient replica.Client,
	promMetrics *monitoring.PrometheusMetrics, class *models.Class, jobQueueCh chan job,
) (*Index, error) {
	sd, err := stopwords.NewDetectorFromConfig(invertedIndexConfig.Stopwords)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new index")
	}

	repl := replica.NewReplicator(config.ClassName.String(),
		sg, nodeResolver, replicaClient, logger)

	index := &Index{
		Config:                config,
		getSchema:             sg,
		logger:                logger,
		classSearcher:         cs,
		vectorIndexUserConfig: vectorIndexUserConfig,
		invertedIndexConfig:   invertedIndexConfig,
		stopwords:             sd,
		replicator:            repl,
		remote: sharding.NewRemoteIndex(config.ClassName.String(), sg,
			nodeResolver, remoteClient),
		metrics:         NewMetrics(logger, promMetrics, config.ClassName.String(), "n/a"),
		centralJobQueue: jobQueueCh,
	}
	if class != nil && class.MultiTenancyConfig != nil {
		index.tenantKey = class.MultiTenancyConfig.TenantKey
	}

	if err := index.checkSingleShardMigration(shardState); err != nil {
		return nil, errors.Wrap(err, "migrating sharding state from previous version")
	}

	for _, shardName := range shardState.AllPhysicalShards() {

		if !shardState.IsShardLocal(shardName) {
			// do not create non-local shards
			continue
		}

		shard, err := NewShard(ctx, promMetrics, shardName, index, class, jobQueueCh)
		if err != nil {
			return nil, errors.Wrapf(err, "init shard %s of index %s", shardName, index.ID())
		}

		index.shards.Store(shardName, shard)
	}

	return index, nil
}

// Iterate over all objects in the index, applying the callback function to each one.  Adding or removing objects during iteration is not supported.
func (i *Index) IterateObjects(ctx context.Context, cb func(index *Index, shard *Shard, object *storobj.Object) error) (err error) {
	return i.ForEachShard(func(_ string, shard *Shard) error {
		wrapper := func(object *storobj.Object) error {
			return cb(i, shard, object)
		}
		bucket := shard.store.Bucket(helpers.ObjectsBucketLSM)
		return bucket.IterateObjects(ctx, wrapper)
	})
}

func (i *Index) ForEachShard(f func(name string, shard *Shard) error) error {
	return i.shards.Range(f)
}

// Iterate over all objects in the shard, applying the callback function to each one.  Adding or removing objects during iteration is not supported.
func (i *Index) IterateShards(ctx context.Context, cb func(index *Index, shard *Shard) error) (err error) {
	return i.ForEachShard(func(key string, shard *Shard) error {
		return cb(i, shard)
	})
}

func (i *Index) addProperty(ctx context.Context, prop *models.Property) error {
	eg := &errgroup.Group{}

	i.ForEachShard(func(key string, shard *Shard) error {
		shard.createPropertyIndex(ctx, prop, eg)
		return nil
	})
	if err := eg.Wait(); err != nil {
		return errors.Wrapf(err, "extend idx '%s' with property '%s", i.ID(), prop.Name)
	}
	return nil
}

func (i *Index) addUUIDProperty(ctx context.Context) error {
	return i.ForEachShard(func(name string, shard *Shard) error {
		err := shard.addIDProperty(ctx)
		if err != nil {
			return errors.Wrapf(err, "add id property to shard %q", name)
		}
		return nil
	})
}

func (i *Index) addDimensionsProperty(ctx context.Context) error {
	return i.ForEachShard(func(name string, shard *Shard) error {
		if err := shard.addDimensionsProperty(ctx); err != nil {
			return errors.Wrapf(err, "add dimensions property to shard %q", name)
		}
		return nil
	})
}

func (i *Index) addTimestampProperties(ctx context.Context) error {
	return i.ForEachShard(func(name string, shard *Shard) error {
		if err := shard.addTimestampProperties(ctx); err != nil {
			return errors.Wrapf(err, "add timestamp properties to shard %q", name)
		}
		return nil
	})
}

func (i *Index) updateVectorIndexConfig(ctx context.Context,
	updated schema.VectorIndexConfig,
) error {
	// an updated is not specific to one shard, but rather all
	return i.ForEachShard(func(name string, shard *Shard) error {
		// At the moment, we don't do anything in an update that could fail, but
		// technically this should be part of some sort of a two-phase commit  or
		// have another way to rollback if we have updates that could potentially
		// fail in the future. For now that's not a realistic risk.
		if err := shard.updateVectorIndexConfig(ctx, updated); err != nil {
			return errors.Wrapf(err, "shard %s", name)
		}
		return nil
	})
}

func (i *Index) getInvertedIndexConfig() schema.InvertedIndexConfig {
	i.invertedIndexConfigLock.Lock()
	defer i.invertedIndexConfigLock.Unlock()

	return i.invertedIndexConfig
}

func (i *Index) updateInvertedIndexConfig(ctx context.Context,
	updated schema.InvertedIndexConfig,
) error {
	i.invertedIndexConfigLock.Lock()
	defer i.invertedIndexConfigLock.Unlock()

	i.invertedIndexConfig = updated

	return nil
}

type IndexConfig struct {
	RootPath                  string
	ClassName                 schema.ClassName
	QueryMaximumResults       int64
	ResourceUsage             config.ResourceUsage
	MemtablesFlushIdleAfter   int
	MemtablesInitialSizeMB    int
	MemtablesMaxSizeMB        int
	MemtablesMinActiveSeconds int
	MemtablesMaxActiveSeconds int
	ReplicationFactor         int64

	TrackVectorDimensions bool
}

func indexID(class schema.ClassName) string {
	return strings.ToLower(string(class))
}

func (i *Index) shardFromUUID(in strfmt.UUID) (string, error) {
	uuid, err := uuid.Parse(in.String())
	if err != nil {
		return "", errors.Wrap(err, "parse id as uuid")
	}

	uuidBytes, _ := uuid.MarshalBinary() // cannot error

	return i.getSchema.ShardingState(i.Config.ClassName.String()).
		PhysicalShard(uuidBytes), nil
}

func (i *Index) determineObjectShard(id strfmt.UUID, tenantKey string) (string, error) {
	if tenantKey != "" {
		return i.shardFromTenantKey(tenantKey, id)
	}
	return i.shardFromUUID(id)
}

func (i *Index) shardFromTenantKey(tenantKey string, id strfmt.UUID) (string, error) {
	ss := i.getSchema.ShardingState(i.Config.ClassName.String())
	if name := ss.Shard(tenantKey, id.String()); name != "" {
		return name, nil
	}
	return "", fmt.Errorf("no tenant found with key: %q", tenantKey)
}

func (i *Index) putObject(ctx context.Context, object *storobj.Object,
	replProps *additional.ReplicationProperties, tenantKey string,
) error {
	if err := i.validateMultiTenancy(tenantKey); err != nil {
		return err
	}

	if i.Config.ClassName != object.Class() {
		return fmt.Errorf("cannot import object of class %s into index of class %s",
			object.Class(), i.Config.ClassName)
	}

	i.backupStateLock.RLock()
	defer i.backupStateLock.RUnlock()
	shardName, err := i.determineObjectShard(object.ID(), tenantKey)
	if err != nil {
		return err
	}

	if i.replicationEnabled() {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		err = i.replicator.PutObject(ctx, shardName, object, replica.ConsistencyLevel(replProps.ConsistencyLevel))
		if err != nil {
			return fmt.Errorf("failed to relay object put across replicas: %w", err)
		}
	} else if i.isLocalShard(shardName) {
		shard := i.shards.Load(shardName)
		if shard == nil {
			return fmt.Errorf("cannot find local shard %q", shardName)
		}
		if err := shard.putObject(ctx, object); err != nil {
			return errors.Wrapf(err, "shard %s", shard.ID())
		}
	} else {
		// this must be a remote shard, try sending it remotely
		if err := i.remote.PutObject(ctx, shardName, object); err != nil {
			return errors.Wrap(err, "send to remote shard")
		}
	}

	return nil
}

func (i *Index) IncomingPutObject(ctx context.Context, shardName string,
	object *storobj.Object,
) error {
	i.backupStateLock.RLock()
	defer i.backupStateLock.RUnlock()
	localShard := i.shards.Load(shardName)
	if localShard == nil {
		return errors.Errorf("shard %q does not exist locally", shardName)
	}

	// This is a bit hacky, the problem here is that storobj.Parse() currently
	// misses date fields as it has no way of knowing that a date-formatted
	// string was actually a date type. However, adding this functionality to
	// Parse() would break a lot of code, because it currently
	// schema-independent. To find out if a field is a date or date[], we need to
	// involve the schema, thus why we are doing it here. This was discovered as
	// part of https://github.com/weaviate/weaviate/issues/1775
	if err := i.parseDateFieldsInProps(object.Object.Properties); err != nil {
		return errors.Wrapf(err, "shard %s", localShard.ID())
	}

	if err := localShard.putObject(ctx, object); err != nil {
		return errors.Wrapf(err, "shard %s", localShard.ID())
	}

	return nil
}

func (i *Index) replicationEnabled() bool {
	return i.Config.ReplicationFactor > 1
}

// parseDateFieldsInProps checks the schema for the current class for which
// fields are date fields, then - if they are set - parses them accordingly.
// Works for both date and date[].
func (i *Index) parseDateFieldsInProps(props interface{}) error {
	if props == nil {
		return nil
	}

	propMap, ok := props.(map[string]interface{})
	if !ok {
		// don't know what to do with this
		return nil
	}

	schemaModel := i.getSchema.GetSchemaSkipAuth().Objects
	c, err := schema.GetClassByName(schemaModel, i.Config.ClassName.String())
	if err != nil {
		return err
	}

	for _, prop := range c.Properties {
		if prop.DataType[0] == string(schema.DataTypeDate) {
			raw, ok := propMap[prop.Name]
			if !ok {
				// prop is not set, nothing to do
				continue
			}

			parsed, err := parseAsStringToTime(raw)
			if err != nil {
				return errors.Wrapf(err, "time prop %q", prop.Name)
			}

			propMap[prop.Name] = parsed
		}

		if prop.DataType[0] == string(schema.DataTypeDateArray) {
			raw, ok := propMap[prop.Name]
			if !ok {
				// prop is not set, nothing to do
				continue
			}

			asSlice, ok := raw.([]string)
			if !ok {
				return errors.Errorf("parse as time array, expected []interface{} got %T",
					raw)
			}
			parsedSlice := make([]interface{}, len(asSlice))
			for j := range asSlice {
				parsed, err := parseAsStringToTime(interface{}(asSlice[j]))
				if err != nil {
					return errors.Wrapf(err, "time array prop %q at pos %d", prop.Name, j)
				}

				parsedSlice[j] = parsed
			}
			propMap[prop.Name] = parsedSlice

		}
	}

	return nil
}

func parseAsStringToTime(in interface{}) (time.Time, error) {
	var parsed time.Time
	var err error

	asString, ok := in.(string)
	if !ok {
		return parsed, errors.Errorf("parse as time, expected string got %T", in)
	}

	parsed, err = time.Parse(time.RFC3339, asString)
	if err != nil {
		return parsed, err
	}

	return parsed, nil
}

// return value []error gives the error for the index with the positions
// matching the inputs
func (i *Index) putObjectBatch(ctx context.Context, objects []*storobj.Object,
	replProps *additional.ReplicationProperties,
) []error {
	i.backupStateLock.RLock()
	defer i.backupStateLock.RUnlock()
	type objsAndPos struct {
		objects []*storobj.Object
		pos     []int
	}

	byShard := map[string]objsAndPos{}
	out := make([]error, len(objects))

	for pos, obj := range objects {
		shardName, err := i.shardFromUUID(obj.ID())
		if err != nil {
			out[pos] = err
			continue
		}

		group := byShard[shardName]
		group.objects = append(group.objects, obj)
		group.pos = append(group.pos, pos)
		byShard[shardName] = group
	}

	wg := &sync.WaitGroup{}

	for shardName, group := range byShard {
		wg.Add(1)
		go func(shardName string, group objsAndPos) {
			defer wg.Done()
			var errs []error
			if i.replicationEnabled() {
				if replProps == nil {
					replProps = defaultConsistency()
				}
				errs = i.replicator.PutObjects(ctx, shardName, group.objects,
					replica.ConsistencyLevel(replProps.ConsistencyLevel))
			} else if !i.isLocalShard(shardName) {
				errs = i.remote.BatchPutObjects(ctx, shardName, group.objects)
			} else {
				shard := i.shards.Load(shardName)
				errs = shard.putObjectBatch(ctx, group.objects)
			}
			for i, err := range errs {
				desiredPos := group.pos[i]
				out[desiredPos] = err
			}
		}(shardName, group)
	}

	wg.Wait()

	return out
}

func duplicateErr(in error, count int) []error {
	out := make([]error, count)
	for i := range out {
		out[i] = in
	}

	return out
}

func (i *Index) IncomingBatchPutObjects(ctx context.Context, shardName string,
	objects []*storobj.Object,
) []error {
	i.backupStateLock.RLock()
	defer i.backupStateLock.RUnlock()
	localShard := i.shards.Load(shardName)
	if localShard == nil {
		return duplicateErr(errors.Errorf("shard %q does not exist locally",
			shardName), len(objects))
	}

	// This is a bit hacky, the problem here is that storobj.Parse() currently
	// misses date fields as it has no way of knowing that a date-formatted
	// string was actually a date type. However, adding this functionality to
	// Parse() would break a lot of code, because it currently
	// schema-independent. To find out if a field is a date or date[], we need to
	// involve the schema, thus why we are doing it here. This was discovered as
	// part of https://github.com/weaviate/weaviate/issues/1775
	for j := range objects {
		if err := i.parseDateFieldsInProps(objects[j].Object.Properties); err != nil {
			return duplicateErr(errors.Wrapf(err, "shard %s", localShard.ID()),
				len(objects))
		}
	}

	return localShard.putObjectBatch(ctx, objects)
}

// return value map[int]error gives the error for the index as it received it
func (i *Index) addReferencesBatch(ctx context.Context, refs objects.BatchReferences,
	replProps *additional.ReplicationProperties,
) []error {
	i.backupStateLock.RLock()
	defer i.backupStateLock.RUnlock()
	type refsAndPos struct {
		refs objects.BatchReferences
		pos  []int
	}

	byShard := map[string]refsAndPos{}
	out := make([]error, len(refs))

	for pos, ref := range refs {
		shardName, err := i.shardFromUUID(ref.From.TargetID)
		if err != nil {
			out[pos] = err
			continue
		}

		group := byShard[shardName]
		group.refs = append(group.refs, ref)
		group.pos = append(group.pos, pos)
		byShard[shardName] = group
	}

	for shardName, group := range byShard {
		var errs []error
		if i.replicationEnabled() {
			if replProps == nil {
				replProps = defaultConsistency()
			}
			errs = i.replicator.AddReferences(ctx, shardName, group.refs,
				replica.ConsistencyLevel(replProps.ConsistencyLevel))
		} else if i.isLocalShard(shardName) {
			shard := i.shards.Load(shardName)
			errs = shard.addReferencesBatch(ctx, group.refs)
		} else {
			errs = i.remote.BatchAddReferences(ctx, shardName, group.refs)
		}
		for i, err := range errs {
			desiredPos := group.pos[i]
			out[desiredPos] = err
		}
	}

	return out
}

func (i *Index) IncomingBatchAddReferences(ctx context.Context, shardName string,
	refs objects.BatchReferences,
) []error {
	i.backupStateLock.RLock()
	defer i.backupStateLock.RUnlock()
	localShard := i.shards.Load(shardName)
	if localShard == nil {
		return duplicateErr(errors.Errorf("shard %q does not exist locally",
			shardName), len(refs))
	}

	return localShard.addReferencesBatch(ctx, refs)
}

func (i *Index) objectByID(ctx context.Context, id strfmt.UUID,
	props search.SelectProperties, addl additional.Properties,
	replProps *additional.ReplicationProperties, tenantKey string,
) (*storobj.Object, error) {
	if err := i.validateMultiTenancy(tenantKey); err != nil {
		return nil, err
	}

	shardName, err := i.determineObjectShard(id, tenantKey)
	if err != nil {
		return nil, fmt.Errorf("failed to determine shard for object %q: %w", id, err)
	}

	var obj *storobj.Object

	if i.replicationEnabled() {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		if replProps.NodeName != "" {
			obj, err = i.replicator.NodeObject(ctx, replProps.NodeName, shardName, id, props, addl)
		} else {
			obj, err = i.replicator.GetOne(ctx,
				replica.ConsistencyLevel(replProps.ConsistencyLevel), shardName, id, props, addl)
		}
	} else if i.isLocalShard(shardName) {
		if shard := i.shards.Load(shardName); shard != nil {
			obj, err = shard.objectByID(ctx, id, props, addl)
			if err != nil {
				err = fmt.Errorf("shard %s: %w", shard.ID(), err)
			}
		} else {
			err = fmt.Errorf("cannot find local shard %q", shardName)
		}
	} else {
		obj, err = i.remote.GetObject(ctx, shardName, id, props, addl)
		if err != nil {
			err = fmt.Errorf("get object from remote index: %w", err)
		}
	}

	return obj, err
}

func (i *Index) IncomingGetObject(ctx context.Context, shardName string,
	id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
	shard := i.shards.Load(shardName)
	if shard == nil {
		return nil, errors.Errorf("shard %q does not exist locally", shardName)
	}

	obj, err := shard.objectByID(ctx, id, props, additional)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return obj, nil
}

func (i *Index) IncomingMultiGetObjects(ctx context.Context, shardName string,
	ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	shard := i.shards.Load(shardName)
	if shard == nil {
		return nil, errors.Errorf("shard %q does not exist locally", shardName)
	}

	objs, err := shard.multiObjectByID(ctx, wrapIDsInMulti(ids))
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return objs, nil
}

func (i *Index) multiObjectByID(ctx context.Context,
	query []multi.Identifier,
) ([]*storobj.Object, error) {
	type idsAndPos struct {
		ids []multi.Identifier
		pos []int
	}

	byShard := map[string]idsAndPos{}

	for pos, id := range query {
		shardName, err := i.shardFromUUID(strfmt.UUID(id.ID))
		if err != nil {
			return nil, err
		}

		group := byShard[shardName]
		group.ids = append(group.ids, id)
		group.pos = append(group.pos, pos)
		byShard[shardName] = group
	}

	out := make([]*storobj.Object, len(query))

	for shardName, group := range byShard {
		local := i.getSchema.
			ShardingState(i.Config.ClassName.String()).
			IsShardLocal(shardName)

		var objects []*storobj.Object
		var err error

		if local {
			shard := i.shards.Load(shardName)
			if shard == nil {
				return nil, fmt.Errorf("cannot find local shard %q", shardName)
			}
			objects, err = shard.multiObjectByID(ctx, group.ids)
			if err != nil {
				return nil, errors.Wrapf(err, "shard %s", shard.ID())
			}
		} else {
			objects, err = i.remote.MultiGetObjects(ctx, shardName, extractIDsFromMulti(group.ids))
			if err != nil {
				return nil, errors.Wrapf(err, "remote shard %s", shardName)
			}
		}

		for i, obj := range objects {
			desiredPos := group.pos[i]
			out[desiredPos] = obj
		}
	}

	return out, nil
}

func extractIDsFromMulti(in []multi.Identifier) []strfmt.UUID {
	out := make([]strfmt.UUID, len(in))

	for i, id := range in {
		out[i] = strfmt.UUID(id.ID)
	}

	return out
}

func wrapIDsInMulti(in []strfmt.UUID) []multi.Identifier {
	out := make([]multi.Identifier, len(in))

	for i, id := range in {
		out[i] = multi.Identifier{ID: string(id)}
	}

	return out
}

func (i *Index) exists(ctx context.Context, id strfmt.UUID,
	replProps *additional.ReplicationProperties, tenantKey string,
) (bool, error) {
	if err := i.validateMultiTenancy(tenantKey); err != nil {
		return false, err
	}

	shardName, err := i.determineObjectShard(id, tenantKey)
	if err != nil {
		return false, err
	}

	var exists bool
	if i.replicationEnabled() {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		exists, err = i.replicator.Exists(ctx,
			replica.ConsistencyLevel(replProps.ConsistencyLevel), shardName, id)
	} else if i.isLocalShard(shardName) {
		shard := i.shards.Load(shardName)
		exists, err = shard.exists(ctx, id)
	} else {
		exists, err = i.remote.Exists(ctx, shardName, id)
	}

	if err != nil {
		return false, errors.Wrapf(err, "shard %s", shardName)
	}

	return exists, nil
}

func (i *Index) IncomingExists(ctx context.Context, shardName string,
	id strfmt.UUID,
) (bool, error) {
	shard := i.shards.Load(shardName)
	if shard == nil {
		return false, errors.Errorf("shard %q does not exist locally", shardName)
	}

	ok, err := shard.exists(ctx, id)
	if err != nil {
		return ok, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return ok, nil
}

func (i *Index) objectSearch(ctx context.Context, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor,
	addlProps additional.Properties, replProps *additional.ReplicationProperties,
) ([]*storobj.Object, []float32, error) {
	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()

	// If the request is a BM25F with no properties selected, use all possible properties
	if keywordRanking != nil && keywordRanking.Type == "bm25" && len(keywordRanking.Properties) == 0 {

		cl, err := schema.GetClassByName(
			i.getSchema.GetSchemaSkipAuth().Objects,
			i.Config.ClassName.String())
		if err != nil {
			return nil, nil, err
		}

		propHash := cl.Properties
		// Get keys of hash
		for _, v := range propHash {
			if inverted.PropertyHasSearchableIndex(i.getSchema.GetSchemaSkipAuth().Objects,
				i.Config.ClassName.String(), v.Name) {

				keywordRanking.Properties = append(keywordRanking.Properties, v.Name)
			}
		}

		// WEAVIATE-471 - error if we can't find a property to search
		if len(keywordRanking.Properties) == 0 {
			return nil, []float32{}, errors.New(
				"No properties provided, and no indexed properties found in class")
		}
	}

	outObjects, outScores, err := i.objectSearchByShard(ctx, limit,
		filters, keywordRanking, sort, cursor, addlProps, shardNames)
	if err != nil {
		return nil, nil, err
	}

	if len(outObjects) == len(outScores) {
		if keywordRanking != nil && keywordRanking.Type == "bm25" {
			for ii := range outObjects {
				oo := outObjects[ii]
				os := outScores[ii]

				if oo.AdditionalProperties() == nil {
					oo.Object.Additional = make(map[string]interface{})
				}
				oo.Object.Additional["score"] = os

				// Collect all keys starting with "BM25F" and add them to the Additional
				if keywordRanking.AdditionalExplanations {
					explainScore := ""
					for k, v := range oo.Object.Additional {
						if strings.HasPrefix(k, "BM25F") {

							explainScore = fmt.Sprintf("%v, %v:%v", explainScore, k, v)
							delete(oo.Object.Additional, k)
						}
					}
					oo.Object.Additional["explainScore"] = explainScore
				}
			}
		}
	}

	if len(sort) > 0 {
		if len(shardNames) > 1 {
			var err error
			outObjects, outScores, err = i.sort(outObjects, outScores, sort, limit)
			if err != nil {
				return nil, nil, errors.Wrap(err, "sort")
			}
		}
	} else if keywordRanking != nil {
		outObjects, outScores = i.sortKeywordRanking(outObjects, outScores)
	} else if len(shardNames) > 1 && !addlProps.ReferenceQuery {
		// sort only for multiple shards (already sorted for single)
		// and for not reference nested query (sort is applied for root query)
		outObjects, outScores = i.sortByID(outObjects, outScores)
	}

	if keywordRanking != nil && keywordRanking.AutoCut > 0 {
		cutOff := autocut.Autocut(outScores, keywordRanking.AutoCut)
		outObjects = outObjects[:cutOff]
		outScores = outScores[:cutOff]
	}

	// if this search was caused by a reference property
	// search, we should not limit the number of results.
	// for example, if the query contains a where filter
	// whose operator is `And`, and one of the operands
	// contains a path to a reference prop, the ClassSearch
	// caused by such a ref prop being limited can cause
	// the `And` to return no results where results would
	// be expected. we won't know that unless we search
	// and return all referenced object properties.
	if !addlProps.ReferenceQuery && len(outObjects) > limit {
		if len(outObjects) == len(outScores) {
			outScores = outScores[:limit]
		}
		outObjects = outObjects[:limit]
	}

	if i.replicationEnabled() {
		if replProps == nil {
			replProps = defaultConsistency(replica.One)
		}
		l := replica.ConsistencyLevel(replProps.ConsistencyLevel)
		err = i.replicator.CheckConsistency(ctx, l, outObjects)
		if err != nil {
			i.logger.WithField("action", "object_search").
				Errorf("failed to check consistency of search results: %v", err)
		}
	}

	return outObjects, outScores, nil
}

func (i *Index) objectSearchByShard(ctx context.Context, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor,
	addlProps additional.Properties, shards []string,
) ([]*storobj.Object, []float32, error) {
	resultObjects, resultScores := objectSearchPreallocate(limit, shards)

	eg := errgroup.Group{}
	shardResultLock := sync.Mutex{}
	for _, shardName := range shards {
		shardName := shardName

		eg.Go(func() error {
			var objs []*storobj.Object
			var scores []float32
			var err error

			if i.isLocalShard(shardName) {
				shard := i.shards.Load(shardName)
				objs, scores, err = shard.objectSearch(ctx, limit, filters, keywordRanking, sort, cursor, addlProps)
				if err != nil {
					return fmt.Errorf(
						"local shard object search %s: %w", shard.ID(), err)
				}
				if i.replicationEnabled() {
					storobj.AddOwnership(objs, i.getSchema.NodeName(), shardName)
				}
			} else {
				objs, scores, err = i.remote.SearchShard(
					ctx, shardName, nil, limit, filters, keywordRanking,
					sort, cursor, nil, addlProps, i.replicationEnabled())
				if err != nil {
					return fmt.Errorf(
						"remote shard object serach %s: %w", shardName, err)
				}
			}

			shardResultLock.Lock()
			resultObjects = append(resultObjects, objs...)
			resultScores = append(resultScores, scores...)
			shardResultLock.Unlock()

			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	return resultObjects, resultScores, nil
}

func (i *Index) sortByID(objects []*storobj.Object, scores []float32,
) ([]*storobj.Object, []float32) {
	return newIDSorter().sort(objects, scores)
}

func (i *Index) sortKeywordRanking(objects []*storobj.Object,
	scores []float32,
) ([]*storobj.Object, []float32) {
	return newScoresSorter().sort(objects, scores)
}

func (i *Index) sort(objects []*storobj.Object, scores []float32,
	sort []filters.Sort, limit int,
) ([]*storobj.Object, []float32, error) {
	return sorter.NewObjectsSorter(i.getSchema.GetSchemaSkipAuth()).
		Sort(objects, scores, limit, sort)
}

func (i *Index) mergeGroups(objects []*storobj.Object, dists []float32,
	groupBy *searchparams.GroupBy, limit, shardCount int,
) ([]*storobj.Object, []float32, error) {
	return newGroupMerger(objects, dists, groupBy).Do()
}

func (i *Index) singleLocalShardObjectVectorSearch(ctx context.Context, searchVector []float32,
	dist float32, limit int, filters *filters.LocalFilter,
	sort []filters.Sort, groupBy *searchparams.GroupBy, additional additional.Properties,
	shardName string,
) ([]*storobj.Object, []float32, error) {
	shard := i.shards.Load(shardName)
	res, resDists, err := shard.objectVectorSearch(
		ctx, searchVector, dist, limit, filters, sort, groupBy, additional)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return res, resDists, nil
}

func (i *Index) objectVectorSearch(ctx context.Context, searchVector []float32,
	dist float32, limit int, filters *filters.LocalFilter,
	sort []filters.Sort, groupBy *searchparams.GroupBy,
	additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	shardingState := i.getSchema.ShardingState(i.Config.ClassName.String())
	shardNames := shardingState.AllPhysicalShards()

	if len(shardNames) == 1 && shardingState.IsShardLocal(shardNames[0]) {
		return i.singleLocalShardObjectVectorSearch(ctx, searchVector, dist, limit, filters,
			sort, groupBy, additional, shardNames[0])
	}

	// a limit of -1 is used to signal a search by distance. if that is
	// the case we have to adjust how we calculate the output capacity
	var shardCap int
	if limit < 0 {
		shardCap = len(shardNames) * hnsw.DefaultSearchByDistInitialLimit
	} else {
		shardCap = len(shardNames) * limit
	}

	errgrp := &errgroup.Group{}
	m := &sync.Mutex{}

	out := make([]*storobj.Object, 0, shardCap)
	dists := make([]float32, 0, shardCap)
	for _, shardName := range shardNames {
		shardName := shardName
		errgrp.Go(func() error {
			local := shardingState.IsShardLocal(shardName)

			var res []*storobj.Object
			var resDists []float32
			var err error

			if local {
				shard := i.shards.Load(shardName)
				res, resDists, err = shard.objectVectorSearch(
					ctx, searchVector, dist, limit, filters, sort, groupBy, additional)
				if err != nil {
					return errors.Wrapf(err, "shard %s", shard.ID())
				}
			} else {
				res, resDists, err = i.remote.SearchShard(ctx,
					shardName, searchVector, limit, filters,
					nil, sort, nil, groupBy, additional, i.replicationEnabled())
				if err != nil {
					return errors.Wrapf(err, "remote shard %s", shardName)
				}
			}

			m.Lock()
			out = append(out, res...)
			dists = append(dists, resDists...)
			m.Unlock()

			return nil
		})
	}

	if err := errgrp.Wait(); err != nil {
		return nil, nil, err
	}

	if len(shardNames) == 1 {
		return out, dists, nil
	}

	if len(shardNames) > 1 && groupBy != nil {
		return i.mergeGroups(out, dists, groupBy, limit, len(shardNames))
	}

	if len(shardNames) > 1 && len(sort) > 0 {
		return i.sort(out, dists, sort, limit)
	}

	out, dists = newDistancesSorter().sort(out, dists)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
		dists = dists[:limit]
	}

	return out, dists, nil
}

func (i *Index) IncomingSearch(ctx context.Context, shardName string,
	searchVector []float32, distance float32, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort,
	cursor *filters.Cursor, groupBy *searchparams.GroupBy,
	additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	shard := i.shards.Load(shardName)
	if shard == nil {
		return nil, nil, errors.Errorf("shard %q does not exist locally", shardName)
	}

	if searchVector == nil {
		res, scores, err := shard.objectSearch(ctx, limit, filters, keywordRanking, sort, cursor, additional)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "shard %s", shard.ID())
		}

		return res, scores, nil
	}

	res, resDists, err := shard.objectVectorSearch(
		ctx, searchVector, distance, limit, filters, sort, groupBy, additional)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return res, resDists, nil
}

func (i *Index) deleteObject(ctx context.Context, id strfmt.UUID,
	replProps *additional.ReplicationProperties, tenantKey string,
) error {
	i.backupStateLock.RLock()
	defer i.backupStateLock.RUnlock()
	shardName, err := i.determineObjectShard(id, tenantKey)
	if err != nil {
		return err
	}

	if i.replicationEnabled() {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		err = i.replicator.DeleteObject(ctx, shardName, id,
			replica.ConsistencyLevel(replProps.ConsistencyLevel))
		if err != nil {
			return fmt.Errorf("failed to relay object delete across replicas: %w", err)
		}
	} else if i.isLocalShard(shardName) {
		shard := i.shards.Load(shardName)
		if err := shard.deleteObject(ctx, id); err != nil {
			return fmt.Errorf("delete object: %w", err)
		}
	} else {
		err = i.remote.DeleteObject(ctx, shardName, id)
		if err != nil {
			return errors.Wrapf(err, "send to remote shard %s", shardName)
		}
	}

	return nil
}

func (i *Index) IncomingDeleteObject(ctx context.Context, shardName string,
	id strfmt.UUID,
) error {
	i.backupStateLock.RLock()
	defer i.backupStateLock.RUnlock()
	shard := i.shards.Load(shardName)
	if shard == nil {
		return errors.Errorf("shard %q does not exist locally", shardName)
	}

	err := shard.deleteObject(ctx, id)
	if err != nil {
		return errors.Wrapf(err, "shard %s", shard.ID())
	}

	return nil
}

func (i *Index) isLocalShard(shard string) bool {
	return i.getSchema.ShardingState(i.Config.ClassName.String()).IsShardLocal(shard)
}

func (i *Index) mergeObject(ctx context.Context, merge objects.MergeDocument,
	replProps *additional.ReplicationProperties,
) error {
	i.backupStateLock.RLock()
	defer i.backupStateLock.RUnlock()
	shardName, err := i.shardFromUUID(merge.ID)
	if err != nil {
		return err
	}

	if i.replicationEnabled() {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		err = i.replicator.MergeObject(ctx, shardName, &merge, replica.ConsistencyLevel(replProps.ConsistencyLevel))
		if err != nil {
			return fmt.Errorf("failed to relay object patch across replicas: %w", err)
		}
	} else if i.isLocalShard(shardName) {
		shard := i.shards.Load(shardName)
		err = shard.mergeObject(ctx, merge)
	} else {
		err = i.remote.MergeObject(ctx, shardName, merge)
	}
	if err != nil {
		return errors.Wrapf(err, "shard %s", shardName)
	}

	return nil
}

func (i *Index) IncomingMergeObject(ctx context.Context, shardName string,
	mergeDoc objects.MergeDocument,
) error {
	i.backupStateLock.RLock()
	defer i.backupStateLock.RUnlock()
	shard := i.shards.Load(shardName)
	if shard == nil {
		return errors.Errorf("shard %q does not exist locally", shardName)
	}

	err := shard.mergeObject(ctx, mergeDoc)
	if err != nil {
		return errors.Wrapf(err, "shard %s", shard.ID())
	}

	return nil
}

func (i *Index) aggregate(ctx context.Context,
	params aggregation.Params,
) (*aggregation.Result, error) {
	shardState := i.getSchema.ShardingState(i.Config.ClassName.String())
	shardNames := shardState.AllPhysicalShards()

	results := make([]*aggregation.Result, len(shardNames))
	for j, shardName := range shardNames {
		local := shardState.IsShardLocal(shardName)

		var err error
		var res *aggregation.Result
		if !local {
			res, err = i.remote.Aggregate(ctx, shardName, params)
		} else {
			shard := i.shards.Load(shardName)
			res, err = shard.aggregate(ctx, params)
		}
		if err != nil {
			return nil, errors.Wrapf(err, "shard %s", shardName)
		}

		results[j] = res
	}

	return aggregator.NewShardCombiner().Do(results), nil
}

func (i *Index) IncomingAggregate(ctx context.Context, shardName string,
	params aggregation.Params,
) (*aggregation.Result, error) {
	shard := i.shards.Load(shardName)
	if shard == nil {
		return nil, errors.Errorf("shard %q does not exist locally", shardName)
	}

	res, err := shard.aggregate(ctx, params)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return res, nil
}

func (i *Index) drop() error {
	i.backupStateLock.RLock()
	defer i.backupStateLock.RUnlock()
	for _, name := range i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards() {
		shard := i.shards.Load(name)
		if shard == nil {
			// skip non-local, but do delete everything that exists - even if it
			// shouldn't
			continue
		}
		err := shard.drop()
		if err != nil {
			return errors.Wrapf(err, "delete shard %s", shard.ID())
		}
	}

	return nil
}

func (i *Index) Shutdown(ctx context.Context) error {
	i.backupStateLock.RLock()
	defer i.backupStateLock.RUnlock()
	return i.ForEachShard(func(name string, shard *Shard) error {
		if err := shard.shutdown(ctx); err != nil {
			return errors.Wrapf(err, "shutdown shard %q", name)
		}
		return nil
	})
}

func (i *Index) getShardsStatus(ctx context.Context) (map[string]string, error) {
	shardsStatus := make(map[string]string)

	shardState := i.getSchema.ShardingState(i.Config.ClassName.String())
	shardNames := shardState.AllPhysicalShards()

	for _, shardName := range shardNames {
		local := shardState.IsShardLocal(shardName)

		var err error
		var status string
		if !local {
			status, err = i.remote.GetShardStatus(ctx, shardName)
		} else {
			shard := i.shards.Load(shardName)
			if shard == nil {
				err = errors.Errorf("shard %s does not exist", shardName)
			} else {
				status = shard.getStatus().String()
			}
		}
		if err != nil {
			return nil, errors.Wrapf(err, "shard %s", shardName)
		}

		shardsStatus[shardName] = status
	}

	return shardsStatus, nil
}

func (i *Index) IncomingGetShardStatus(ctx context.Context, shardName string) (string, error) {
	shard := i.shards.Load(shardName)
	if shard == nil {
		return "", errors.Errorf("shard %q does not exist", shardName)
	}
	return shard.getStatus().String(), nil
}

func (i *Index) updateShardStatus(ctx context.Context, shardName, targetStatus string) error {
	shardState := i.getSchema.ShardingState(i.Config.ClassName.String())

	var err error
	local := shardState.IsShardLocal(shardName)
	if !local {
		err = i.remote.UpdateShardStatus(ctx, shardName, targetStatus)
	} else {
		shard := i.shards.Load(shardName)
		if shard == nil {
			err = errors.Errorf("shard %s does not exist", shardName)
		} else {
			err = shard.updateStatus(targetStatus)
		}
	}
	if err != nil {
		return errors.Wrapf(err, "shard %s", shardName)
	}

	return nil
}

func (i *Index) IncomingUpdateShardStatus(ctx context.Context, shardName, targetStatus string) error {
	shard := i.shards.Load(shardName)
	if shard == nil {
		return errors.Errorf("shard %s does not exist", shardName)
	}
	return shard.updateStatus(targetStatus)
}

func (i *Index) notifyReady() {
	i.ForEachShard(func(name string, shard *Shard) error {
		shard.notifyReady()
		return nil
	})
}

func (i *Index) findDocIDs(ctx context.Context,
	filters *filters.LocalFilter,
) (map[string][]uint64, error) {
	before := time.Now()
	defer i.metrics.BatchDelete(before, "filter_total")

	shardState := i.getSchema.ShardingState(i.Config.ClassName.String())
	shardNames := shardState.AllPhysicalShards()

	results := make(map[string][]uint64)
	for _, shardName := range shardNames {
		local := shardState.IsShardLocal(shardName)

		var err error
		var res []uint64
		if !local {
			res, err = i.remote.FindDocIDs(ctx, shardName, filters)
		} else {
			shard := i.shards.Load(shardName)
			res, err = shard.findDocIDs(ctx, filters)
		}
		if err != nil {
			return nil, errors.Wrapf(err, "shard %s", shardName)
		}

		results[shardName] = res
	}

	return results, nil
}

func (i *Index) IncomingFindDocIDs(ctx context.Context, shardName string,
	filters *filters.LocalFilter,
) ([]uint64, error) {
	shard := i.shards.Load(shardName)
	if shard == nil {
		return nil, errors.Errorf("shard %q does not exist locally", shardName)
	}

	docIDs, err := shard.findDocIDs(ctx, filters)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return docIDs, nil
}

func (i *Index) batchDeleteObjects(ctx context.Context, shardDocIDs map[string][]uint64,
	dryRun bool, replProps *additional.ReplicationProperties,
) (objects.BatchSimpleObjects, error) {
	i.backupStateLock.RLock()
	defer i.backupStateLock.RUnlock()
	before := time.Now()
	defer i.metrics.BatchDelete(before, "delete_from_shards_total")

	type result struct {
		objs objects.BatchSimpleObjects
	}

	wg := &sync.WaitGroup{}
	ch := make(chan result, len(shardDocIDs))
	for shardName, docIDs := range shardDocIDs {
		wg.Add(1)
		go func(shardName string, docIDs []uint64) {
			defer wg.Done()

			var objs objects.BatchSimpleObjects
			if i.replicationEnabled() {
				if replProps == nil {
					replProps = defaultConsistency()
				}
				objs = i.replicator.DeleteObjects(ctx, shardName, docIDs,
					dryRun, replica.ConsistencyLevel(replProps.ConsistencyLevel))
			} else if i.isLocalShard(shardName) {
				shard := i.shards.Load(shardName)
				objs = shard.deleteObjectBatch(ctx, docIDs, dryRun)
			} else {
				objs = i.remote.DeleteObjectBatch(ctx, shardName, docIDs, dryRun)
			}
			ch <- result{objs}
		}(shardName, docIDs)
	}

	wg.Wait()
	close(ch)

	var out objects.BatchSimpleObjects
	for res := range ch {
		out = append(out, res.objs...)
	}

	return out, nil
}

func (i *Index) IncomingDeleteObjectBatch(ctx context.Context, shardName string,
	docIDs []uint64, dryRun bool,
) objects.BatchSimpleObjects {
	i.backupStateLock.RLock()
	defer i.backupStateLock.RUnlock()
	shard := i.shards.Load(shardName)
	if shard == nil {
		return objects.BatchSimpleObjects{
			objects.BatchSimpleObject{Err: errors.Errorf("shard %q does not exist locally", shardName)},
		}
	}

	return shard.deleteObjectBatch(ctx, docIDs, dryRun)
}

func defaultConsistency(l ...replica.ConsistencyLevel) *additional.ReplicationProperties {
	rp := &additional.ReplicationProperties{}
	if len(l) != 0 {
		rp.ConsistencyLevel = string(l[0])
	} else {
		rp.ConsistencyLevel = string(replica.Quorum)
	}
	return rp
}

func objectSearchPreallocate(limit int, shards []string) ([]*storobj.Object, []float32) {
	perShardLimit := config.DefaultQueryMaximumResults
	if perShardLimit > int64(limit) {
		perShardLimit = int64(limit)
	}
	capacity := perShardLimit * int64(len(shards))
	objects := make([]*storobj.Object, 0, capacity)
	scores := make([]float32, 0, capacity)

	return objects, scores
}

func (i *Index) addNewShard(ctx context.Context,
	class *models.Class, shardName string,
) error {
	if shard := i.shards.Load(shardName); shard != nil {
		return fmt.Errorf("shard %q exists already", shardName)
	}

	// TODO: metrics
	s, err := NewShard(ctx, nil, shardName, i, class, i.centralJobQueue)
	if err != nil {
		return err
	}

	i.shards.Store(shardName, s)

	return nil
}

func (i *Index) validateMultiTenancy(tenantKey string) error {
	if i.tenantKey != "" && tenantKey == "" {
		return fmt.Errorf("class %q has multi-tenancy enabled, tenant_key %q required",
			i.Config.ClassName, i.tenantKey)
	}
	return nil
}
