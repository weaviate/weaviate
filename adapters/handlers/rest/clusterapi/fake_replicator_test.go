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

package clusterapi_test

import (
	"context"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// fakeReplicator is a mock implementation of the replicator interface for testing
type fakeReplicator struct {
	commitBlock chan struct{}
}

// newFakeReplicator creates a new controllable fake replicator
func newFakeReplicator() *fakeReplicator {
	return &fakeReplicator{
		commitBlock: make(chan struct{}),
	}
}

func (f *fakeReplicator) ReplicateObject(ctx context.Context, indexName, shardName, requestID string, object *storobj.Object, schemaVersion uint64) replica.SimpleResponse {
	return replica.SimpleResponse{}
}

func (f *fakeReplicator) ReplicateObjects(ctx context.Context, indexName, shardName, requestID string, objects []*storobj.Object, schemaVersion uint64) replica.SimpleResponse {
	return replica.SimpleResponse{}
}

func (f *fakeReplicator) ReplicateUpdate(ctx context.Context, indexName, shardName, requestID string, mergeDoc *objects.MergeDocument, schemaVersion uint64) replica.SimpleResponse {
	return replica.SimpleResponse{}
}

func (f *fakeReplicator) ReplicateDeletion(ctx context.Context, indexName, shardName, requestID string, uuid strfmt.UUID, deletionTime time.Time, schemaVersion uint64) replica.SimpleResponse {
	return replica.SimpleResponse{}
}

func (f *fakeReplicator) ReplicateDeletions(ctx context.Context, indexName, shardName, requestID string, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64) replica.SimpleResponse {
	return replica.SimpleResponse{}
}

func (f *fakeReplicator) ReplicateReferences(ctx context.Context, indexName, shardName, requestID string, refs []objects.BatchReference, schemaVersion uint64) replica.SimpleResponse {
	return replica.SimpleResponse{}
}

func (f *fakeReplicator) CommitReplication(indexName, shardName, requestID string) interface{} {
	<-f.commitBlock
	return map[string]string{"status": "committed"}
}

func (f *fakeReplicator) AbortReplication(indexName, shardName, requestID string) interface{} {
	return map[string]string{"status": "aborted"}
}

func (f *fakeReplicator) OverwriteObjects(ctx context.Context, index, shard string, vobjects []*objects.VObject) ([]replica.RepairResponse, error) {
	return []replica.RepairResponse{}, nil
}

func (f *fakeReplicator) FetchObject(ctx context.Context, indexName, shardName string, id strfmt.UUID) (objects.Replica, error) {
	return objects.Replica{}, nil
}

func (f *fakeReplicator) FetchObjects(ctx context.Context, class, shardName string, ids []strfmt.UUID) ([]objects.Replica, error) {
	return []objects.Replica{}, nil
}

func (f *fakeReplicator) DigestObjects(ctx context.Context, class, shardName string, ids []strfmt.UUID) (result []replica.RepairResponse, err error) {
	return []replica.RepairResponse{}, nil
}

func (f *fakeReplicator) DigestObjectsInRange(ctx context.Context, class, shardName string, initialUUID, finalUUID strfmt.UUID, limit int) (result []replica.RepairResponse, err error) {
	return []replica.RepairResponse{}, nil
}

func (f *fakeReplicator) HashTreeLevel(ctx context.Context, index, shard string, level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error) {
	return []hashtree.Digest{}, nil
}
