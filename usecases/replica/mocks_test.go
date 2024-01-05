//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

type fakeRClient struct {
	mock.Mock
}

func (f *fakeRClient) FetchObject(ctx context.Context, host, index, shard string,
	id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties,
) (objects.Replica, error) {
	args := f.Called(ctx, host, index, shard, id, props, additional)
	return args.Get(0).(objects.Replica), args.Error(1)
}

func (f *fakeRClient) FetchObjects(ctx context.Context, host, index,
	shard string, ids []strfmt.UUID,
) ([]objects.Replica, error) {
	args := f.Called(ctx, host, index, shard, ids)
	return args.Get(0).([]objects.Replica), args.Error(1)
}

func (f *fakeRClient) OverwriteObjects(ctx context.Context, host, index, shard string,
	xs []*objects.VObject,
) ([]RepairResponse, error) {
	args := f.Called(ctx, host, index, shard, xs)
	return args.Get(0).([]RepairResponse), args.Error(1)
}

func (f *fakeRClient) DigestObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID,
) ([]RepairResponse, error) {
	args := f.Called(ctx, host, index, shard, ids)
	return args.Get(0).([]RepairResponse), args.Error(1)
}

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
	uuids []strfmt.UUID, dryRun bool,
) (SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, uuids, dryRun)
	return args.Get(0).(SimpleResponse), args.Error(1)
}

func (f *fakeClient) AddReferences(ctx context.Context, host, index, shard, requestID string,
	refs []objects.BatchReference,
) (SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, refs)
	return args.Get(0).(SimpleResponse), args.Error(1)
}

func (f *fakeClient) Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error {
	args := f.Called(ctx, host, index, shard, requestID, resp)
	return args.Error(0)
}

func (f *fakeClient) Abort(ctx context.Context, host, index, shard, requestID string) (SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID)
	return args.Get(0).(SimpleResponse), args.Error(1)
}

// Replica finder
type fakeShardingState struct {
	thisNode        string
	ShardToReplicas map[string][]string
	nodeResolver    *fakeNodeResolver
}

func newFakeShardingState(thisNode string, shardToReplicas map[string][]string, resolver *fakeNodeResolver) *fakeShardingState {
	return &fakeShardingState{
		thisNode:        thisNode,
		ShardToReplicas: shardToReplicas,
		nodeResolver:    resolver,
	}
}

func (f *fakeShardingState) NodeName() string {
	return f.thisNode
}

func (f *fakeShardingState) ResolveParentNodes(_ string, shard string) (map[string]string, error) {
	replicas, ok := f.ShardToReplicas[shard]
	if !ok {
		return nil, fmt.Errorf("sharding state not found")
	}

	m := make(map[string]string)
	for _, name := range replicas {
		addr, _ := f.nodeResolver.NodeHostname(name)
		m[name] = addr

	}

	return m, nil
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
