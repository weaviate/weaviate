package sharding

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type RemoteIndex struct {
	class        string
	stateGetter  shardingStateGetter
	client       RemoteIndexClient
	nodeResolver nodeResolver
}

type shardingStateGetter interface {
	ShardingState(class string) *State
}

func NewRemoteIndex(className string,
	stateGetter shardingStateGetter, nodeResolver nodeResolver,
	client RemoteIndexClient) *RemoteIndex {
	return &RemoteIndex{
		class:        className,
		stateGetter:  stateGetter,
		client:       client,
		nodeResolver: nodeResolver,
	}
}

type nodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
}

type RemoteIndexClient interface {
	PutObject(ctx context.Context, hostName, indexName, shardName string,
		obj *storobj.Object) error
}

func (ri *RemoteIndex) PutObject(ctx context.Context, shardName string,
	obj *storobj.Object) error {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode)
	if !ok {
		return errors.Errorf("resolve node name %q to host", shard.BelongsToNode)
	}

	fmt.Printf("will now contact %s: %s/%s/%s\n", shard.BelongsToNode, host, ri.class, shardName)
	return ri.client.PutObject(ctx, host, ri.class, shardName, obj)
}
