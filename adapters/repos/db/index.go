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
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// Index is the logical unit which contains all the data for one particular
// class. An index can be further broken up into self-contained units, called
// Shards, to allow for easy distribution across Nodes
type Index struct {
	Shards    map[string]*Shard
	Config    IndexConfig
	getSchema schemaUC.SchemaGetter
}

func (i Index) ID() string {
	return indexID(i.Config.Kind, i.Config.ClassName)
}

// NewIndex - for now - always creates a single-shard index
func NewIndex(config IndexConfig, sg schemaUC.SchemaGetter) (*Index, error) {
	index := &Index{
		Config:    config,
		Shards:    map[string]*Shard{},
		getSchema: sg,
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

func (i *Index) putObject(ctx context.Context, object *KindObject) error {
	if i.Config.Kind != object.Kind {
		return fmt.Errorf("cannot import object of kind %s into index of kind %s", object.Kind, i.Config.Kind)
	}

	if i.Config.ClassName != object.Class() {
		return fmt.Errorf("cannot import object of class %s into index of class %s", object.Class(), i.Config.ClassName)
	}

	// TODO: pick the right shard instead of using the "single" shard
	shard := i.Shards["single"]
	err := shard.putObject(ctx, object)
	if err != nil {
		errors.Wrapf(err, "shard %s", shard.ID())
	}

	return nil
}

func (i *Index) objectByID(ctx context.Context, id strfmt.UUID, props traverser.SelectProperties, meta bool) (*KindObject, error) {
	// TODO: don't ignore meta and props

	// TODO: search across all shards, rather than hard-coded "single" shard
	// TODO: can we improve this by hashing so we know the target shard?

	shard := i.Shards["single"]
	obj, err := shard.objectByID(ctx, id, props, meta)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return obj, nil
}

func (i *Index) objectSearch(ctx context.Context, limit int, filters *filters.LocalFilter,
	meta bool) ([]*KindObject, error) {
	// TODO: don't ignore meta and filters
	// TODO: search across all shards, rather than hard-coded "single" shard

	shard := i.Shards["single"]
	res, err := shard.objectSearch(ctx, limit, filters, meta)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return res, nil
}
