//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package replica

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/stretchr/testify/mock"
)

type fakeClient struct {
	mock.Mock
}

func (f *fakeClient) PutObject(ctx context.Context, host, index, shard, requestID string,
	obj *storobj.Object,
) (SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, obj)
	return args.Get(0).(SimpleResponse), args.Error(1)
}

func (f *fakeClient) DeleteObject(ctx context.Context, host, index, shard, requestID string,
	id strfmt.UUID,
) (SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, id)
	return args.Get(0).(SimpleResponse), args.Error(1)
}

func (f *fakeClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
	doc *objects.MergeDocument,
) (SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, doc)
	return args.Get(0).(SimpleResponse), args.Error(1)
}

func (f *fakeClient) PutObjects(ctx context.Context, host, index, shard, requestID string,
	objs []*storobj.Object,
) (SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, objs)
	return args.Get(0).(SimpleResponse), args.Error(1)
}

func (f *fakeClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	docIDs []uint64, dryRun bool,
) (SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, docIDs, dryRun)
	return args.Get(0).(SimpleResponse), args.Error(1)
}

func (f *fakeClient) AddReferences(ctx context.Context, host, index, shard, requestID string,
	refs objects.BatchReferences,
) (SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, refs)
	return args.Get(0).(SimpleResponse), args.Error(1)
}

func (f *fakeClient) Commit(ctx context.Context, host string, requestID string, resp interface{}) error {
	args := f.Called(ctx, host, requestID, resp)
	return args.Error(0)
}

func (f *fakeClient) Abort(ctx context.Context, host string, requestID string) error {
	args := f.Called(ctx, host, requestID)
	return args.Error(0)
}

// Replica finder
type fakeShardingState struct {
	ShardToReplicas map[string][]string
}

func newFakeShardingState(shardToReplicas map[string][]string) *fakeShardingState {
	return &fakeShardingState{ShardToReplicas: shardToReplicas}
}

func (f *fakeShardingState) ShardingState(class string) *sharding.State {
	state := sharding.State{}
	state.Physical = make(map[string]sharding.Physical)
	for shard, nodes := range f.ShardToReplicas {
		state.Physical[shard] = sharding.Physical{BelongsToNodes: nodes}
	}
	return &state
}

// node resolver
type fakeNodeResolver struct {
	hosts map[string]string
}

func (r *fakeNodeResolver) NodeHostname(nodeName string) (string, bool) {
	return r.hosts[nodeName], true
}

func newFakeNodeResolver(nodes []string) *fakeNodeResolver {
	hosts := make(map[string]string)
	for _, node := range nodes {
		hosts[node] = node
	}
	return &fakeNodeResolver{hosts: hosts}
}
