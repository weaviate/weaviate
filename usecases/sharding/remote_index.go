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

package sharding

import (
	"context"
	"io"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
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
	client RemoteIndexClient,
) *RemoteIndex {
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
		objs []*storobj.Object, repl *additional.ReplicationProperties) []error
	BatchAddReferences(ctx context.Context, hostName, indexName, shardName string,
		refs objects.BatchReferences) []error
	GetObject(ctx context.Context, hostname, indexName, shardName string,
		id strfmt.UUID, props search.SelectProperties,
		additional additional.Properties) (*storobj.Object, error)
	Exists(ctx context.Context, hostname, indexName, shardName string,
		id strfmt.UUID) (bool, error)
	DeleteObject(ctx context.Context, hostname, indexName, shardName string,
		id strfmt.UUID) error
	MergeObject(ctx context.Context, hostname, indexName, shardName string,
		mergeDoc objects.MergeDocument) error
	MultiGetObjects(ctx context.Context, hostname, indexName, shardName string,
		ids []strfmt.UUID) ([]*storobj.Object, error)
	SearchShard(ctx context.Context, hostname, indexName, shardName string,
		searchVector []float32, limit int, filters *filters.LocalFilter,
		keywordRanking *searchparams.KeywordRanking, sort []filters.Sort,
		cursor *filters.Cursor, groupBy *searchparams.GroupBy,
		additional additional.Properties,
	) ([]*storobj.Object, []float32, error)
	Aggregate(ctx context.Context, hostname, indexName, shardName string,
		params aggregation.Params) (*aggregation.Result, error)
	FindDocIDs(ctx context.Context, hostName, indexName, shardName string,
		filters *filters.LocalFilter) ([]uint64, error)
	DeleteObjectBatch(ctx context.Context, hostName, indexName, shardName string,
		docIDs []uint64, dryRun bool) objects.BatchSimpleObjects
	GetShardStatus(ctx context.Context, hostName, indexName, shardName string) (string, error)
	UpdateShardStatus(ctx context.Context, hostName, indexName, shardName,
		targetStatus string) error

	PutFile(ctx context.Context, hostName, indexName, shardName, fileName string,
		payload io.ReadSeekCloser) error
}

func (ri *RemoteIndex) PutObject(ctx context.Context, shardName string,
	obj *storobj.Object,
) error {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode())
	if !ok {
		return errors.Errorf("resolve node name %q to host", shard.BelongsToNode())
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
	objs []*storobj.Object,
) []error {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return duplicateErr(errors.Errorf("class %s has no physical shard %q",
			ri.class, shardName), len(objs))
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode())
	if !ok {
		return duplicateErr(errors.Errorf("resolve node name %q to host",
			shard.BelongsToNode()), len(objs))
	}

	return ri.client.BatchPutObjects(ctx, host, ri.class, shardName, objs, nil)
}

func (ri *RemoteIndex) BatchAddReferences(ctx context.Context, shardName string,
	refs objects.BatchReferences,
) []error {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return duplicateErr(errors.Errorf("class %s has no physical shard %q",
			ri.class, shardName), len(refs))
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode())
	if !ok {
		return duplicateErr(errors.Errorf("resolve node name %q to host",
			shard.BelongsToNode()), len(refs))
	}

	return ri.client.BatchAddReferences(ctx, host, ri.class, shardName, refs)
}

func (ri *RemoteIndex) Exists(ctx context.Context, shardName string,
	id strfmt.UUID,
) (bool, error) {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return false, errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode())
	if !ok {
		return false, errors.Errorf("resolve node name %q to host", shard.BelongsToNode())
	}

	return ri.client.Exists(ctx, host, ri.class, shardName, id)
}

func (ri *RemoteIndex) DeleteObject(ctx context.Context, shardName string,
	id strfmt.UUID,
) error {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode())
	if !ok {
		return errors.Errorf("resolve node name %q to host", shard.BelongsToNode())
	}

	return ri.client.DeleteObject(ctx, host, ri.class, shardName, id)
}

func (ri *RemoteIndex) MergeObject(ctx context.Context, shardName string,
	mergeDoc objects.MergeDocument,
) error {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode())
	if !ok {
		return errors.Errorf("resolve node name %q to host", shard.BelongsToNode())
	}

	return ri.client.MergeObject(ctx, host, ri.class, shardName, mergeDoc)
}

func (ri *RemoteIndex) GetObject(ctx context.Context, shardName string,
	id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return nil, errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode())
	if !ok {
		return nil, errors.Errorf("resolve node name %q to host", shard.BelongsToNode())
	}

	return ri.client.GetObject(ctx, host, ri.class, shardName, id, props, additional)
}

func (ri *RemoteIndex) MultiGetObjects(ctx context.Context, shardName string,
	ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return nil, errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode())
	if !ok {
		return nil, errors.Errorf("resolve node name %q to host", shard.BelongsToNode())
	}

	return ri.client.MultiGetObjects(ctx, host, ri.class, shardName, ids)
}

func (ri *RemoteIndex) SearchShard(ctx context.Context, shardName string,
	searchVector []float32, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort,
	cursor *filters.Cursor, groupBy *searchparams.GroupBy,
	additional additional.Properties, replEnabled bool,
) ([]*storobj.Object, []float32, error) {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return nil, nil, errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode())
	if !ok {
		return nil, nil, errors.Errorf("resolve node name %q to host", shard.BelongsToNode())
	}

	objs, scores, err := ri.client.SearchShard(ctx, host, ri.class, shardName, searchVector, limit,
		filters, keywordRanking, sort, cursor, groupBy, additional)
	if replEnabled {
		storobj.AddOwnership(objs, shard.BelongsToNode(), shard.Name)
	}
	return objs, scores, err
}

func (ri *RemoteIndex) Aggregate(ctx context.Context, shardName string,
	params aggregation.Params,
) (*aggregation.Result, error) {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return nil, errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode())
	if !ok {
		return nil, errors.Errorf("resolve node name %q to host", shard.BelongsToNode())
	}

	return ri.client.Aggregate(ctx, host, ri.class, shardName, params)
}

func (ri *RemoteIndex) FindDocIDs(ctx context.Context, shardName string,
	filters *filters.LocalFilter,
) ([]uint64, error) {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return nil, errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode())
	if !ok {
		return nil, errors.Errorf("resolve node name %q to host", shard.BelongsToNode())
	}

	return ri.client.FindDocIDs(ctx, host, ri.class, shardName, filters)
}

func (ri *RemoteIndex) DeleteObjectBatch(ctx context.Context, shardName string,
	docIDs []uint64, dryRun bool,
) objects.BatchSimpleObjects {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		err := errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode())
	if !ok {
		err := errors.Errorf("resolve node name %q to host", shard.BelongsToNode())
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	return ri.client.DeleteObjectBatch(ctx, host, ri.class, shardName, docIDs, dryRun)
}

func (ri *RemoteIndex) GetShardStatus(ctx context.Context, shardName string) (string, error) {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return "", errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode())
	if !ok {
		return "", errors.Errorf("resolve node name %q to host", shard.BelongsToNode())
	}

	return ri.client.GetShardStatus(ctx, host, ri.class, shardName)
}

func (ri *RemoteIndex) UpdateShardStatus(ctx context.Context, shardName, targetStatus string) error {
	shard, ok := ri.stateGetter.ShardingState(ri.class).Physical[shardName]
	if !ok {
		return errors.Errorf("class %s has no physical shard %q", ri.class, shardName)
	}

	host, ok := ri.nodeResolver.NodeHostname(shard.BelongsToNode())
	if !ok {
		return errors.Errorf("resolve node name %q to host", shard.BelongsToNode())
	}

	return ri.client.UpdateShardStatus(ctx, host, ri.class, shardName, targetStatus)
}
