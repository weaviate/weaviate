package db

import (
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
	return strings.ToLower(fmt.Sprintf("%s_%s", i.Config.Kind, i.Config.ClassName))
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
