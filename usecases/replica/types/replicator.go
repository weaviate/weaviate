//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package types

import (
	"context"
	"time"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

type Replicator interface {
	// Write endpoints
	ReplicateObject(ctx context.Context, className, shardName, requestID string, object *storobj.Object, schemaVersion uint64) replica.SimpleResponse
	ReplicateObjects(ctx context.Context, className, shardName, requestID string, objects []*storobj.Object, schemaVersion uint64) replica.SimpleResponse
	ReplicateUpdate(ctx context.Context, className, shardName, requestID string, mergeDoc *objects.MergeDocument, schemaVersion uint64) replica.SimpleResponse
	ReplicateDeletion(ctx context.Context, className, shardName, requestID string, uuid strfmt.UUID, deletionTime time.Time, schemaVersion uint64) replica.SimpleResponse
	ReplicateDeletions(ctx context.Context, className, shardName, requestID string, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64) replica.SimpleResponse
	ReplicateReferences(ctx context.Context, className, shardName, requestID string, refs []objects.BatchReference, schemaVersion uint64) replica.SimpleResponse
	CommitReplication(className, shardName, requestID string) interface{}
	AbortReplication(className, shardName, requestID string) interface{}
	OverwriteObjects(ctx context.Context, className, shard string, vobjects []*objects.VObject) ([]types.RepairResponse, error)
	// Read endpoints
	FetchObject(ctx context.Context, className, shardName string, id strfmt.UUID) (replica.Replica, error)
	FetchObjects(ctx context.Context, className, shardName string, ids []strfmt.UUID) ([]replica.Replica, error)
	DigestObjects(ctx context.Context, className, shardName string, ids []strfmt.UUID) (result []types.RepairResponse, err error)
	DigestObjectsInRange(ctx context.Context, className, shardName string,
		initialUUID, finalUUID strfmt.UUID, limit int) (result []types.RepairResponse, err error)
	HashTreeLevel(ctx context.Context, className, shardName string,
		level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error)
}
