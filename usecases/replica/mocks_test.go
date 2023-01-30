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

func (f *fakeRClient) FindObject(ctx context.Context, host, index, shard string,
	id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
	args := f.Called(ctx, host, index, shard, id, props, additional)
	return args.Get(0).(*storobj.Object), args.Error(1)
}

func (f *fakeRClient) Exists(ctx context.Context, host, index, shard string, id strfmt.UUID) (bool, error) {
	args := f.Called(ctx, host, index, shard, id)
	return args.Get(0).(bool), args.Error(1)
}

func (f *fakeRClient) MultiGetObjects(ctx context.Context, host, index,
	shard string, ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	args := f.Called(ctx, host, index, shard, ids)
	return args.Get(0).([]*storobj.Object), args.Error(1)
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
	docIDs []uint64, dryRun bool,
) (SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, docIDs, dryRun)
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
	ShardToReplicas map[string][]string
	nodeResolver    *fakeNodeResolver
}

func newFakeShardingState(shardToReplicas map[string][]string, resolver *fakeNodeResolver) *fakeShardingState {
	return &fakeShardingState{ShardToReplicas: shardToReplicas, nodeResolver: resolver}
}

func (f *fakeShardingState) NodeName() string {
	return "Coordinator"
}

func (f *fakeShardingState) ResolveParentNodes(_ string, shard string,
) ([]string, []string, error) {
	reps, ok := f.ShardToReplicas[shard]
	if !ok {
		return nil, nil, fmt.Errorf("sharding state not found")
	}

	var resolved []string
	var unresolved []string

	for _, rep := range reps {
		host, found := f.nodeResolver.NodeHostname(rep)
		if found && host != "" {
			resolved = append(resolved, host)
		} else {
			unresolved = append(unresolved, rep)
		}
	}

	return resolved, unresolved, nil
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
