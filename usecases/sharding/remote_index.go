package sharding

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type RemoteIndex struct {
	class       string
	stateGetter shardingStateGetter
}

type shardingStateGetter interface {
	ShardingState(class string) *State
}

func NewRemoteIndex(className string,
	stateGetter shardingStateGetter) *RemoteIndex {
	return &RemoteIndex{
		class:       className,
		stateGetter: stateGetter,
	}
}

// TODO: a UC cannot depend on an adapter sub-package, we might need to move storobj

func (ri *RemoteIndex) PutObject(ctx context.Context, shardName string,
	obj *storobj.Object) error {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	fmt.Printf("would now contact %s/%s/%s\n", shard.BelongsToNode, ri.class, shardName)
	return nil
}
