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

package replica_test

import (
	"context"
	"time"

	"github.com/weaviate/weaviate/usecases/replica"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

type fakeRClient struct {
	mock.Mock
}

func (f *fakeRClient) FetchObject(ctx context.Context, host, index, shard string,
	id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties, numRetries int,
) (replica.Replica, error) {
	args := f.Called(ctx, host, index, shard, id, props, additional)
	return args.Get(0).(replica.Replica), args.Error(1)
}

func (f *fakeRClient) FetchObjects(ctx context.Context, host, index,
	shard string, ids []strfmt.UUID,
) ([]replica.Replica, error) {
	args := f.Called(ctx, host, index, shard, ids)
	return args.Get(0).([]replica.Replica), args.Error(1)
}

func (f *fakeRClient) OverwriteObjects(ctx context.Context, host, index, shard string,
	xs []*objects.VObject,
) ([]types.RepairResponse, error) {
	args := f.Called(ctx, host, index, shard, xs)
	return args.Get(0).([]types.RepairResponse), args.Error(1)
}

func (f *fakeRClient) DigestObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID, numRetries int,
) ([]types.RepairResponse, error) {
	args := f.Called(ctx, host, index, shard, ids)
	return args.Get(0).([]types.RepairResponse), args.Error(1)
}

func (f *fakeRClient) FindUUIDs(ctx context.Context, host, index, shard string,
	filters *filters.LocalFilter,
) ([]strfmt.UUID, error) {
	args := f.Called(ctx, host, index, shard, filters)
	return args.Get(0).([]strfmt.UUID), args.Error(1)
}

func (f *fakeRClient) DigestObjectsInRange(ctx context.Context, host, index, shard string,
	initialUUID, finalUUID strfmt.UUID, limit int,
) ([]types.RepairResponse, error) {
	args := f.Called(ctx, host, index, shard, initialUUID, finalUUID, limit)
	return args.Get(0).([]types.RepairResponse), args.Error(1)
}

func (f *fakeRClient) HashTreeLevel(ctx context.Context,
	host, index, shard string, level int, discriminant *hashtree.Bitset,
) (digests []hashtree.Digest, err error) {
	args := f.Called(ctx, host, index, shard, level, discriminant)
	return args.Get(0).([]hashtree.Digest), args.Error(1)
}

type fakeClient struct {
	mock.Mock
}

func (f *fakeClient) PutObject(ctx context.Context, host, index, shard, requestID string,
	obj *storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, obj, schemaVersion)
	return args.Get(0).(replica.SimpleResponse), args.Error(1)
}

func (f *fakeClient) DeleteObject(ctx context.Context, host, index, shard, requestID string,
	id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, id, deletionTime, schemaVersion)
	return args.Get(0).(replica.SimpleResponse), args.Error(1)
}

func (f *fakeClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
	doc *objects.MergeDocument, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, doc, schemaVersion)
	return args.Get(0).(replica.SimpleResponse), args.Error(1)
}

func (f *fakeClient) PutObjects(ctx context.Context, host, index, shard, requestID string,
	objs []*storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, objs, schemaVersion)
	return args.Get(0).(replica.SimpleResponse), args.Error(1)
}

func (f *fakeClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, uuids, deletionTime, dryRun, schemaVersion)
	return args.Get(0).(replica.SimpleResponse), args.Error(1)
}

func (f *fakeClient) AddReferences(ctx context.Context, host, index, shard, requestID string,
	refs []objects.BatchReference, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID, refs, schemaVersion)
	return args.Get(0).(replica.SimpleResponse), args.Error(1)
}

func (f *fakeClient) Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error {
	args := f.Called(ctx, host, index, shard, requestID, resp)
	return args.Error(0)
}

func (f *fakeClient) Abort(ctx context.Context, host, index, shard, requestID string) (replica.SimpleResponse, error) {
	args := f.Called(ctx, host, index, shard, requestID)
	return args.Get(0).(replica.SimpleResponse), args.Error(1)
}
