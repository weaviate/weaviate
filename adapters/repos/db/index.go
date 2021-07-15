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
	"sort"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/multi"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/objects"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
)

// Index is the logical unit which contains all the data for one particular
// class. An index can be further broken up into self-contained units, called
// Shards, to allow for easy distribution across Nodes
type Index struct {
	classSearcher         inverted.ClassSearcher // to allow for nested by-references searches
	Shards                map[string]*Shard
	Config                IndexConfig
	vectorIndexUserConfig schema.VectorIndexConfig
	invertedIndexConfig   *models.InvertedIndexConfig
	getSchema             schemaUC.SchemaGetter
	logger                logrus.FieldLogger
}

func (i Index) ID() string {
	return indexID(i.Config.ClassName)
}

// NewIndex - for now - always creates a single-shard index
func NewIndex(ctx context.Context, config IndexConfig,
	shardState *sharding.State, invertedIndexConfig *models.InvertedIndexConfig,
	vectorIndexUserConfig schema.VectorIndexConfig, sg schemaUC.SchemaGetter,
	cs inverted.ClassSearcher, logger logrus.FieldLogger) (*Index, error) {
	index := &Index{
		Config:                config,
		Shards:                map[string]*Shard{},
		getSchema:             sg,
		logger:                logger,
		classSearcher:         cs,
		vectorIndexUserConfig: vectorIndexUserConfig,
		invertedIndexConfig:   invertedIndexConfig,
	}

	for _, shardName := range shardState.AllPhysicalShards() {
		shard, err := NewShard(ctx, shardName, index)
		if err != nil {
			return nil, errors.Wrapf(err, "init shard %s of index %s", shardName, index.ID())
		}

		index.Shards[shardName] = shard
	}

	return index, nil
}

func (i *Index) addProperty(ctx context.Context, prop *models.Property) error {
	for name, shard := range i.Shards {
		if err := shard.addProperty(ctx, prop); err != nil {
			return errors.Wrapf(err, "add property to shard %q", name)
		}
	}

	return nil
}

func (i *Index) addUUIDProperty(ctx context.Context) error {
	for name, shard := range i.Shards {
		if err := shard.addIDProperty(ctx); err != nil {
			return errors.Wrapf(err, "add id property to shard %q", name)
		}
	}

	return nil
}

func (i *Index) updateVectorIndexConfig(ctx context.Context,
	updated schema.VectorIndexConfig) error {
	// an updated is not specific to one shard, but rather all
	for name, shard := range i.Shards {
		// At the moment, we don't do anything in an update that could fail, but
		// technically this should be part of some sort of a two-phase commit  or
		// have another way to rollback if we have updates that could potentially
		// fail in the future. For now that's not a realistic risk.
		if err := shard.updateVectorIndexConfig(ctx, updated); err != nil {
			return errors.Wrapf(err, "shard %s", name)
		}
	}

	return nil
}

type IndexConfig struct {
	RootPath  string
	ClassName schema.ClassName
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

func (i *Index) putObject(ctx context.Context, object *storobj.Object) error {
	if i.Config.ClassName != object.Class() {
		return errors.Errorf("cannot import object of class %s into index of class %s",
			object.Class(), i.Config.ClassName)
	}

	shardName, err := i.shardFromUUID(object.ID())
	if err != nil {
		return err
	}

	shard := i.Shards[shardName]
	err = shard.putObject(ctx, object)
	if err != nil {
		return errors.Wrapf(err, "shard %s", shard.ID())
	}

	return nil
}

// return value map[int]error gives the error for the index as it received it
func (i *Index) putObjectBatch(ctx context.Context,
	objects []*storobj.Object) map[int]error {
	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()

	// TODO: pick the right shard(s) instead of using the first shard
	shard := i.Shards[shardNames[0]]
	return shard.putObjectBatch(ctx, objects)
}

// return value map[int]error gives the error for the index as it received it
func (i *Index) addReferencesBatch(ctx context.Context,
	refs objects.BatchReferences) map[int]error {
	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()

	// TODO: pick the right shard(s) instead of using the first shard
	shard := i.Shards[shardNames[0]]
	return shard.addReferencesBatch(ctx, refs)
}

func (i *Index) objectByID(ctx context.Context, id strfmt.UUID,
	props traverser.SelectProperties, additional traverser.AdditionalProperties) (*storobj.Object, error) {
	shardName, err := i.shardFromUUID(id)
	if err != nil {
		return nil, err
	}

	shard := i.Shards[shardName]
	obj, err := shard.objectByID(ctx, id, props, additional)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return obj, nil
}

func (i *Index) multiObjectByID(ctx context.Context,
	query []multi.Identifier) ([]*storobj.Object, error) {
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
		shard := i.Shards[shardName]
		objects, err := shard.multiObjectByID(ctx, group.ids)
		if err != nil {
			return nil, errors.Wrapf(err, "shard %s", shard.ID())
		}

		for i, obj := range objects {
			desiredPos := group.pos[i]
			out[desiredPos] = obj
		}
	}

	return out, nil
}

func (i *Index) exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	shardName, err := i.shardFromUUID(id)
	if err != nil {
		return false, err
	}

	shard := i.Shards[shardName]
	ok, err := shard.exists(ctx, id)
	if err != nil {
		return false, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return ok, nil
}

func (i *Index) objectSearch(ctx context.Context, limit int,
	filters *filters.LocalFilter,
	additional traverser.AdditionalProperties) ([]*storobj.Object, error) {
	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()

	out := make([]*storobj.Object, 0, len(shardNames)*limit)
	for _, shardName := range shardNames {
		shard := i.Shards[shardName]
		res, err := shard.objectSearch(ctx, limit, filters, additional)
		if err != nil {
			return nil, errors.Wrapf(err, "shard %s", shard.ID())
		}

		out = append(out, res...)
	}

	if len(out) > limit {
		out = out[:limit]
	}

	return out, nil
}

func (i *Index) objectVectorSearch(ctx context.Context, searchVector []float32,
	limit int, filters *filters.LocalFilter, additional traverser.AdditionalProperties) ([]*storobj.Object, error) {
	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()

	out := make([]*storobj.Object, 0, len(shardNames)*limit)
	dists := make([]float32, 0, len(shardNames)*limit)
	for _, shardName := range shardNames {
		shard := i.Shards[shardName]
		res, resDists, err := shard.objectVectorSearch(ctx, searchVector, limit, filters, additional)
		if err != nil {
			return nil, errors.Wrapf(err, "shard %s", shard.ID())
		}

		out = append(out, res...)
		dists = append(dists, resDists...)
	}

	if len(shardNames) == 1 {
		return out, nil
	}

	sbd := sortObjsByDist{out, dists}
	sort.Sort(sbd)
	if len(sbd.objects) > limit {
		sbd.objects = sbd.objects[:limit]
	}

	return sbd.objects, nil
}

func (i *Index) deleteObject(ctx context.Context, id strfmt.UUID) error {
	shardName, err := i.shardFromUUID(id)
	if err != nil {
		return err
	}

	shard := i.Shards[shardName]
	if err := shard.deleteObject(ctx, id); err != nil {
		return errors.Wrapf(err, "shard %s", shard.ID())
	}

	return nil
}

func (i *Index) mergeObject(ctx context.Context, merge objects.MergeDocument) error {
	shardName, err := i.shardFromUUID(merge.ID)
	if err != nil {
		return err
	}

	shard := i.Shards[shardName]
	if err := shard.mergeObject(ctx, merge); err != nil {
		return errors.Wrapf(err, "shard %s", shard.ID())
	}

	return nil
}

func (i *Index) aggregate(ctx context.Context,
	params traverser.AggregateParams) (*aggregation.Result, error) {
	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()
	// TODO: search across all shards, rather than hard-coded first shard

	shard := i.Shards[shardNames[0]]
	obj, err := shard.aggregate(ctx, params)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return obj, nil
}

func (i *Index) drop() error {
	for _, name := range i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards() {
		shard := i.Shards[name]
		err := shard.drop()
		if err != nil {
			return errors.Wrapf(err, "delete shard %s", shard.ID())
		}
	}

	return nil
}

func (i *Index) Shutdown(ctx context.Context) error {
	for id, shard := range i.Shards {
		if err := shard.shutdown(ctx); err != nil {
			return errors.Wrapf(err, "shutdown shard %q", id)
		}
	}

	return nil
}
