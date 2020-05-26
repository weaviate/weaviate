package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// Index is the logical unit which contains all the data for one particular
// class. An index can be further broken up into self-contained units, called
// Shards, to allow for easy distribution across Nodes
type Index struct {
	Shards map[string]*Shard
	Config IndexConfig
}

func (i Index) ID() string {
	return indexID(i.Config.Kind, i.Config.ClassName)
}

// NewIndex - for now - always creates a single-shard index
func NewIndex(config IndexConfig) (*Index, error) {
	index := &Index{
		Config: config,
		Shards: map[string]*Shard{},
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

type IndexConfig struct {
	RootPath  string
	Kind      kind.Kind
	ClassName schema.ClassName
}

func indexID(kind kind.Kind, class schema.ClassName) string {
	return strings.ToLower(fmt.Sprintf("%s_%s", kind, class))
}

func (i *Index) putObject(ctx context.Context, object *KindObject) error {
	if i.Config.Kind != object.Kind() {
		return fmt.Errorf("cannot import object of kind %s into index of kind %s", object.Kind(), i.Config.Kind)
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
