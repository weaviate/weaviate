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
	"context"
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/multi"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
)

// Index is the logical unit which contains all the data for one particular
// class. An index can be further broken up into self-contained units, called
// Shards, to allow for easy distribution across Nodes
type Index struct {
	Shards    map[string]*Shard
	Config    IndexConfig
	getSchema schemaUC.SchemaGetter
	logger    logrus.FieldLogger
}

func (i Index) ID() string {
	return indexID(i.Config.Kind, i.Config.ClassName)
}

// NewIndex - for now - always creates a single-shard index
func NewIndex(config IndexConfig, sg schemaUC.SchemaGetter,
	logger logrus.FieldLogger) (*Index, error) {
	index := &Index{
		Config:    config,
		Shards:    map[string]*Shard{},
		getSchema: sg,
		logger:    logger,
	}

	// use explicit shard name "single" to indicate it's currently the only
	// supported config
	singleShard, err := NewShard("single", index)
	if err != nil {
		return nil, errors.Wrapf(err, "init index %s", index.ID())
	}

	index.Shards["single"] = singleShard
	return index, nil
}

func (i *Index) addProperty(ctx context.Context, prop *models.Property) error {
	// TODO: pick the right shard instead of using the "single" shard
	shard := i.Shards["single"]

	return shard.addProperty(ctx, prop)
}

type IndexConfig struct {
	RootPath  string
	Kind      kind.Kind
	ClassName schema.ClassName
}

func indexID(kind kind.Kind, class schema.ClassName) string {
	return strings.ToLower(fmt.Sprintf("%s_%s", kind, class))
}

func (i *Index) putObject(ctx context.Context, object *storobj.Object) error {
	if i.Config.Kind != object.Kind {
		return fmt.Errorf("cannot import object of kind %s into index of kind %s",
			object.Kind, i.Config.Kind)
	}

	if i.Config.ClassName != object.Class() {
		return fmt.Errorf("cannot import object of class %s into index of class %s",
			object.Class(), i.Config.ClassName)
	}

	// TODO: pick the right shard instead of using the "single" shard
	shard := i.Shards["single"]
	err := shard.putObject(ctx, object)
	if err != nil {
		return errors.Wrapf(err, "shard %s", shard.ID())
	}

	return nil
}

// return value map[int]error gives the error for the index as it received it
func (i *Index) putObjectBatch(ctx context.Context,
	objects []*storobj.Object) map[int]error {
	// TODO: pick the right shard(s) instead of using the "single" shard
	shard := i.Shards["single"]
	return shard.putObjectBatch(ctx, objects)
}

// return value map[int]error gives the error for the index as it received it
func (i *Index) addReferencesBatch(ctx context.Context,
	refs kinds.BatchReferences) map[int]error {
	// TODO: pick the right shard(s) instead of using the "single" shard
	shard := i.Shards["single"]
	return shard.addReferencesBatch(ctx, refs)
}

func (i *Index) objectByID(ctx context.Context, id strfmt.UUID,
	props traverser.SelectProperties, meta bool) (*storobj.Object, error) {
	// TODO: don't ignore meta

	// TODO: search across all shards, rather than hard-coded "single" shard
	// TODO: can we improve this by hashing so we know the target shard?

	shard := i.Shards["single"]
	obj, err := shard.objectByID(ctx, id, props, meta)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return obj, nil
}

func (i *Index) multiObjectByID(ctx context.Context,
	query []multi.Identifier) ([]*storobj.Object, error) {
	// TODO: search across all shards, rather than hard-coded "single" shard
	// TODO: can we improve this by hashing so we know the target shard?

	shard := i.Shards["single"]
	objects, err := shard.multiObjectByID(ctx, query)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return objects, nil
}

func (i *Index) exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	// TODO: search across all shards, rather than hard-coded "single" shard
	// TODO: can we improve this by hashing so we know the target shard?

	shard := i.Shards["single"]
	ok, err := shard.exists(ctx, id)
	if err != nil {
		return false, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return ok, nil
}

func (i *Index) objectSearch(ctx context.Context, limit int,
	filters *filters.LocalFilter,
	meta bool) ([]*storobj.Object, error) {
	// TODO: don't ignore meta
	// TODO: search across all shards, rather than hard-coded "single" shard

	shard := i.Shards["single"]
	res, err := shard.objectSearch(ctx, limit, filters, meta)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return res, nil
}

func (i *Index) objectVectorSearch(ctx context.Context, searchVector []float32,
	limit int, filters *filters.LocalFilter, meta bool) ([]*storobj.Object, error) {
	// TODO: don't ignore meta
	// TODO: search across all shards, rather than hard-coded "single" shard

	shard := i.Shards["single"]
	res, err := shard.objectVectorSearch(ctx, searchVector, limit, filters, meta)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return res, nil
}

func (i *Index) deleteObject(ctx context.Context, id strfmt.UUID) error {
	// TODO: search across all shards, rather than hard-coded "single" shard

	shard := i.Shards["single"]
	if err := shard.deleteObject(ctx, id); err != nil {
		return errors.Wrapf(err, "shard %s", shard.ID())
	}

	return nil
}

func (i *Index) mergeObject(ctx context.Context, merge kinds.MergeDocument) error {
	// TODO: search across all shards, rather than hard-coded "single" shard

	shard := i.Shards["single"]
	if err := shard.mergeObject(ctx, merge); err != nil {
		return errors.Wrapf(err, "shard %s", shard.ID())
	}

	return nil
}
