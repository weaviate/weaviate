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
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
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

	if len(shardState.AllPhysicalShards()) > 1 {
		return nil, errors.Errorf("multi-shard indices not supported yet")
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

func (i *Index) putObject(ctx context.Context, object *storobj.Object) error {
	if i.Config.ClassName != object.Class() {
		return fmt.Errorf("cannot import object of class %s into index of class %s",
			object.Class(), i.Config.ClassName)
	}

	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()

	// TODO: pick the right shard instead of using the first shard
	shard := i.Shards[shardNames[0]]
	err := shard.putObject(ctx, object)
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
	// TODO: don't ignore meta

	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()
	// TODO: search across all shards, rather than hard-coded first shard

	shard := i.Shards[shardNames[0]]
	obj, err := shard.objectByID(ctx, id, props, additional)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return obj, nil
}

func (i *Index) multiObjectByID(ctx context.Context,
	query []multi.Identifier) ([]*storobj.Object, error) {
	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()
	// TODO: search across all shards, rather than hard-coded first shard

	shard := i.Shards[shardNames[0]]
	objects, err := shard.multiObjectByID(ctx, query)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return objects, nil
}

func (i *Index) exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()
	// TODO: use correct shard, rather than hard-coded first shard

	shard := i.Shards[shardNames[0]]
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
	// TODO: search across all shards, rather than hard-coded first shard

	shard := i.Shards[shardNames[0]]
	res, err := shard.objectSearch(ctx, limit, filters, additional)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return res, nil
}

func (i *Index) objectVectorSearch(ctx context.Context, searchVector []float32,
	limit int, filters *filters.LocalFilter, additional traverser.AdditionalProperties) ([]*storobj.Object, error) {
	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()
	// TODO: search across all shards, rather than hard-coded first shard

	shard := i.Shards[shardNames[0]]
	res, err := shard.objectVectorSearch(ctx, searchVector, limit, filters, additional)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return res, nil
}

func (i *Index) deleteObject(ctx context.Context, id strfmt.UUID) error {
	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()
	// TODO: search across all shards, rather than hard-coded first shard

	shard := i.Shards[shardNames[0]]
	if err := shard.deleteObject(ctx, id); err != nil {
		return errors.Wrapf(err, "shard %s", shard.ID())
	}

	return nil
}

func (i *Index) mergeObject(ctx context.Context, merge objects.MergeDocument) error {
	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()
	// TODO: search across all shards, rather than hard-coded first shard

	shard := i.Shards[shardNames[0]]
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
