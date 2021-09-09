package sharding

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
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
	BatchPutObjects(ctx context.Context, hostName, indexName, shardName string,
		objs []*storobj.Object) []error
	BatchAddReferences(ctx context.Context, hostName, indexName, shardName string,
		refs objects.BatchReferences) []error
	GetObject(ctx context.Context, hostname, indexName, shardName string,
		id strfmt.UUID, props search.SelectProperties,
		additional additional.Properties) (*storobj.Object, error)
	Exists(ctx context.Context, hostname, indexName, shardName string,
		id strfmt.UUID) (bool, error)
	MultiGetObjects(ctx context.Context, hostname, indexName, shardName string,
		ids []strfmt.UUID) ([]*storobj.Object, error)
	SearchShard(ctx context.Context, hostname, indexName, shardName string,
		searchVector []float32, limit int, filters *filters.LocalFilter,
		additional additional.Properties) ([]*storobj.Object, []float32, error)
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

	return ri.client.PutObject(ctx, host, ri.class, shardName, obj)
}

// helper for single errors that affect the entire batch, assign the error to
// every single item in the batch
func duplicateErr(in error, count int) []error {
	out := make([]error, count)
	for i := range out {
		out[i] = in
	}
	return out
}

func (ri *RemoteIndex) BatchPutObjects(ctx context.Context, shardName string,
	objs []*storobj.Object) []error {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return duplicateErr(errors.Errorf("class %s has no physical shard %q",
			ri.class, shardName), len(objs))
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode)
	if !ok {
		return duplicateErr(errors.Errorf("resolve node name %q to host",
			shard.BelongsToNode), len(objs))
	}

	return ri.client.BatchPutObjects(ctx, host, ri.class, shardName, objs)
}

func (ri *RemoteIndex) BatchAddReferences(ctx context.Context, shardName string,
	refs objects.BatchReferences) []error {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return duplicateErr(errors.Errorf("class %s has no physical shard %q",
			ri.class, shardName), len(refs))
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode)
	if !ok {
		return duplicateErr(errors.Errorf("resolve node name %q to host",
			shard.BelongsToNode), len(refs))
	}

	return ri.client.BatchAddReferences(ctx, host, ri.class, shardName, refs)
}

func (ri *RemoteIndex) Exists(ctx context.Context, shardName string,
	id strfmt.UUID) (bool, error) {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return false, errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode)
	if !ok {
		return false, errors.Errorf("resolve node name %q to host", shard.BelongsToNode)
	}

	return ri.client.Exists(ctx, host, ri.class, shardName, id)
}

func (ri *RemoteIndex) GetObject(ctx context.Context, shardName string,
	id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties) (*storobj.Object, error) {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return nil, errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode)
	if !ok {
		return nil, errors.Errorf("resolve node name %q to host", shard.BelongsToNode)
	}

	return ri.client.GetObject(ctx, host, ri.class, shardName, id, props, additional)
}

func (ri *RemoteIndex) MultiGetObjects(ctx context.Context, shardName string,
	ids []strfmt.UUID) ([]*storobj.Object, error) {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return nil, errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode)
	if !ok {
		return nil, errors.Errorf("resolve node name %q to host", shard.BelongsToNode)
	}

	return ri.client.MultiGetObjects(ctx, host, ri.class, shardName, ids)
}

func (ri *RemoteIndex) SearchShard(ctx context.Context, shardName string,
	searchVector []float32, limit int, filters *filters.LocalFilter,
	additional additional.Properties) ([]*storobj.Object, []float32, error) {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return nil, nil, errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode)
	if !ok {
		return nil, nil, errors.Errorf("resolve node name %q to host", shard.BelongsToNode)
	}

	return ri.client.SearchShard(ctx, host, ri.class, shardName, searchVector, limit,
		filters, additional)
}
