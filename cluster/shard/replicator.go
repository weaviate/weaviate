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

package shard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-openapi/strfmt"
	"github.com/klauspost/compress/s2"
	"github.com/sirupsen/logrus"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrNoLeaderFound is returned when no leader can be found for a shard.
	ErrNoLeaderFound = errors.New("no leader found for shard")

	// ErrForwardingRequired is returned when the request needs to be forwarded to another node.
	ErrForwardingRequired = errors.New("request must be forwarded to leader")
)

type Replicator interface {
	AddReferences(ctx context.Context, shard string, refs []objects.BatchReference, l routerTypes.ConsistencyLevel, schemaVersion uint64) []error
	CheckConsistency(ctx context.Context, l routerTypes.ConsistencyLevel, xs []*storobj.Object) error
	CollectShardDifferences(ctx context.Context, shardName string, ht hashtree.AggregatedHashTree, diffTimeoutPerNode time.Duration, targetNodeOverrides []additional.AsyncReplicationTargetNodeOverride) (diffReader *replica.ShardDifferenceReader, err error)
	DeleteObject(ctx context.Context, shard string, id strfmt.UUID, deletionTime time.Time, l routerTypes.ConsistencyLevel, schemaVersion uint64) error
	DeleteObjects(ctx context.Context, shard string, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, l routerTypes.ConsistencyLevel, schemaVersion uint64) []objects.BatchSimpleObject
	DigestObjectsInRange(ctx context.Context, shardName string, host string, initialUUID strfmt.UUID, finalUUID strfmt.UUID, limit int) (ds []routerTypes.RepairResponse, err error)
	Exists(ctx context.Context, l routerTypes.ConsistencyLevel, shard string, id strfmt.UUID) (bool, error)
	FindUUIDs(ctx context.Context, className string, shard string, filters *filters.LocalFilter, l routerTypes.ConsistencyLevel, limit int) (uuids []strfmt.UUID, err error)
	GetOne(ctx context.Context, l routerTypes.ConsistencyLevel, shard string, id strfmt.UUID, props search.SelectProperties, adds additional.Properties) (*storobj.Object, error)
	LocalNodeName() string
	MergeObject(ctx context.Context, shard string, doc *objects.MergeDocument, l routerTypes.ConsistencyLevel, schemaVersion uint64) error
	NodeObject(ctx context.Context, nodeName string, shard string, id strfmt.UUID, props search.SelectProperties, adds additional.Properties) (*storobj.Object, error)
	Overwrite(ctx context.Context, host string, index string, shard string, xs []*objects.VObject) ([]routerTypes.RepairResponse, error)
	PutObject(ctx context.Context, shard string, obj *storobj.Object, l routerTypes.ConsistencyLevel, schemaVersion uint64) error
	PutObjects(ctx context.Context, shard string, objs []*storobj.Object, l routerTypes.ConsistencyLevel, schemaVersion uint64) []error
}

// RouterConfig holds configuration for the Router.
type RouterConfig struct {
	// NodeID is the local node's identifier.
	NodeID string
	// Logger is the logger to use.
	Logger logrus.FieldLogger
	// Raft is the per-index Raft manager that manages shard Stores.
	Raft *Raft
	// ClassName is the name of the class this router is responsible for.
	ClassName string
	// BackingReplicator is the underlying replicator to delegate non-RAFT methods to.
	// This allows for gradual migration where only some methods use RAFT while others
	// continue to use the existing 2PC/async replication.
	BackingReplicator Replicator
	// Client is the forwarding client used to send requests to leaders when this node is not the leader.
	RpcClientMaker rpcClientMaker
}

// Router routes operations to the correct shard RAFT leader.
// If this node is the leader, the operation is applied locally.
// If this node is not the leader, the operation is forwarded to the leader.
// For methods not yet migrated to RAFT, it delegates to the backing replicator.
type replicator struct {
	Replicator     // Embedded for delegation to backing replicator
	config         RouterConfig
	log            logrus.FieldLogger
	raft           *Raft
	class          string
	rpcClientMaker rpcClientMaker
}

// NewRouter creates a new router for shard RAFT operations.
// If a backing replicator is provided, non-RAFT methods will be delegated to it.
func Newreplicator(config RouterConfig) *replicator {
	return &replicator{
		Replicator:     config.BackingReplicator, // Delegate non-RAFT methods
		config:         config,
		log:            config.Logger.WithField("component", "shard_raft_router"),
		raft:           config.Raft,
		rpcClientMaker: config.RpcClientMaker,
		class:          config.ClassName,
	}
}

func (r *replicator) apply(ctx context.Context, req *shardproto.ApplyRequest) (*shardproto.ApplyResponse, error) {
	store := r.raft.GetStore(req.Shard)
	if store == nil {
		return nil, fmt.Errorf("raft store not found for %s/%s", req.Class, req.Shard)
	}

	if r.IsLeader(req.Shard) {
		v, err := store.Apply(ctx, req)
		if err != nil {
			return nil, err
		}
		return &shardproto.ApplyResponse{Version: v}, nil
	}

	// We're not the leader, need to forward
	return r.forwardToLeader(ctx, store, req)
}

// forwardToLeader forwards a PutObject request to the current leader with retry logic.
func (r *replicator) forwardToLeader(
	ctx context.Context,
	store *Store,
	req *shardproto.ApplyRequest,
) (*shardproto.ApplyResponse, error) {
	// Create exponential backoff for retries
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 100 * time.Millisecond
	bo.MaxInterval = 2 * time.Second
	bo.MaxElapsedTime = 10 * time.Second
	bo.Reset()

	// Wrap with context
	ctxBackoff := backoff.WithContext(bo, ctx)

	var resp *shardproto.ApplyResponse
	var lastErr error
	operation := func() error {
		// Get the current leader (may change between retries)
		leaderID := store.LeaderID()
		if leaderID == "" {
			r.log.WithFields(logrus.Fields{
				"class": req.Class,
				"shard": req.Shard,
			}).Debug("no leader found, will retry")
			return fmt.Errorf("no leader found")
		}

		r.log.WithFields(logrus.Fields{
			"class":    req.Class,
			"shard":    req.Shard,
			"leaderID": leaderID,
		}).Debug("forwarding PutObject to leader")

		client, err := r.rpcClientMaker(ctx, leaderID)
		if err != nil {
			return fmt.Errorf("create RPC client for leader %s: %w", leaderID, err)
		}

		// Forward to the leader
		resp, err = client.Apply(ctx, req)
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if error indicates leader change - should retry
		if errors.Is(err, ErrNotLeader) || strings.Contains(err.Error(), "not leader") {
			r.log.WithFields(logrus.Fields{
				"class":    req.Class,
				"shard":    req.Shard,
				"leaderID": leaderID,
			}).Debug("leader changed, will retry")
			return err // Retry
		}

		// Non-retryable error
		return backoff.Permanent(err)
	}

	if err := backoff.Retry(operation, ctxBackoff); err != nil {
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, err
	}

	return resp, nil
}

// PutObject routes a PutObject operation to the appropriate shard leader.
// If this node is the leader, the operation is applied locally via RAFT.
// If this node is not the leader and a forwarding client is configured,
// the operation is forwarded to the leader.
func (r *replicator) PutObject(ctx context.Context, shard string, obj *storobj.Object, l routerTypes.ConsistencyLevel, schemaVersion uint64) error {
	// Build the PutObject sub-command
	objBytes, err := obj.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal object: %w", err)
	}

	putReq := &shardproto.PutObjectRequest{
		Object:        objBytes,
		SchemaVersion: schemaVersion,
	}
	subCmd, err := proto.Marshal(putReq)
	if err != nil {
		return fmt.Errorf("marshal put request: %w", err)
	}

	compressed := s2.Encode(nil, subCmd)

	req := &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_PUT_OBJECT,
		Class:      r.class,
		Shard:      shard,
		SubCommand: compressed,
		Compressed: true,
	}
	_, err = r.apply(ctx, req)
	return err
}

// DeleteObject routes a DeleteObject operation to the appropriate shard leader.
func (r *replicator) DeleteObject(ctx context.Context, shard string, id strfmt.UUID, deletionTime time.Time, l routerTypes.ConsistencyLevel, schemaVersion uint64) error {
	deleteReq := &shardproto.DeleteObjectRequest{
		Id:               string(id),
		DeletionTimeUnix: deletionTime.UnixNano(),
		SchemaVersion:    schemaVersion,
	}
	subCmd, err := proto.Marshal(deleteReq)
	if err != nil {
		return fmt.Errorf("marshal delete request: %w", err)
	}

	compressed := s2.Encode(nil, subCmd)

	req := &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_DELETE_OBJECT,
		Class:      r.class,
		Shard:      shard,
		SubCommand: compressed,
		Compressed: true,
	}
	_, err = r.apply(ctx, req)
	return err
}

// MergeObject routes a MergeObject operation to the appropriate shard leader.
func (r *replicator) MergeObject(ctx context.Context, shard string, doc *objects.MergeDocument, l routerTypes.ConsistencyLevel, schemaVersion uint64) error {
	docJSON, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal merge document: %w", err)
	}

	mergeReq := &shardproto.MergeObjectRequest{
		MergeDocumentJson: docJSON,
		SchemaVersion:     schemaVersion,
	}
	subCmd, err := proto.Marshal(mergeReq)
	if err != nil {
		return fmt.Errorf("marshal merge request: %w", err)
	}

	compressed := s2.Encode(nil, subCmd)

	req := &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_MERGE_OBJECT,
		Class:      r.class,
		Shard:      shard,
		SubCommand: compressed,
		Compressed: true,
	}
	_, err = r.apply(ctx, req)
	return err
}

// PutObjects routes a batch PutObjects operation to the appropriate shard leader.
// Objects are serialized, chunked by size, and each chunk is applied as a separate
// RAFT log entry via the existing unary Apply path.
func (r *replicator) PutObjects(ctx context.Context, shard string, objs []*storobj.Object, l routerTypes.ConsistencyLevel, schemaVersion uint64) []error {
	if len(objs) == 0 {
		return nil
	}

	// Serialize all objects
	objBytes := make([][]byte, len(objs))
	for i, obj := range objs {
		b, err := obj.MarshalBinary()
		if err != nil {
			return duplicateError(fmt.Errorf("marshal object %d: %w", i, err), len(objs))
		}
		objBytes[i] = b
	}

	// Chunk by size and apply each chunk
	chunks := ChunkObjectBytes(objBytes, defaultMaxBatchChunkBytes)
	for _, chunk := range chunks {
		batchReq := &shardproto.PutObjectsBatchRequest{
			Objects:       chunk,
			SchemaVersion: schemaVersion,
		}
		subCmd, err := proto.Marshal(batchReq)
		if err != nil {
			return duplicateError(fmt.Errorf("marshal batch request: %w", err), len(objs))
		}

		compressed := s2.Encode(nil, subCmd)

		req := &shardproto.ApplyRequest{
			Type:       shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH,
			Class:      r.class,
			Shard:      shard,
			SubCommand: compressed,
			Compressed: true,
		}
		if _, err := r.apply(ctx, req); err != nil {
			return duplicateError(err, len(objs))
		}
	}

	return make([]error, len(objs))
}

// DeleteObjects routes a batch DeleteObjects operation to the appropriate shard leader.
// When dryRun is true, the operation is read-only and delegates to the backing replicator.
func (r *replicator) DeleteObjects(ctx context.Context, shard string, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, l routerTypes.ConsistencyLevel, schemaVersion uint64) []objects.BatchSimpleObject {
	// Dry-run is read-only, skip RAFT and delegate to backing replicator
	if dryRun {
		return r.Replicator.DeleteObjects(ctx, shard, uuids, deletionTime, dryRun, l, schemaVersion)
	}

	uuidStrs := make([]string, len(uuids))
	for i, id := range uuids {
		uuidStrs[i] = string(id)
	}

	deleteReq := &shardproto.DeleteObjectsBatchRequest{
		Uuids:            uuidStrs,
		DeletionTimeUnix: deletionTime.UnixNano(),
		DryRun:           false,
		SchemaVersion:    schemaVersion,
	}
	subCmd, err := proto.Marshal(deleteReq)
	if err != nil {
		return duplicateBatchSimpleError(fmt.Errorf("marshal batch delete request: %w", err), uuids)
	}

	compressed := s2.Encode(nil, subCmd)

	req := &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_DELETE_OBJECTS_BATCH,
		Class:      r.class,
		Shard:      shard,
		SubCommand: compressed,
		Compressed: true,
	}
	if _, err := r.apply(ctx, req); err != nil {
		return duplicateBatchSimpleError(err, uuids)
	}

	results := make([]objects.BatchSimpleObject, len(uuids))
	for i, id := range uuids {
		results[i] = objects.BatchSimpleObject{UUID: id}
	}
	return results
}

// AddReferences routes a batch AddReferences operation to the appropriate shard leader.
func (r *replicator) AddReferences(ctx context.Context, shard string, refs []objects.BatchReference, l routerTypes.ConsistencyLevel, schemaVersion uint64) []error {
	refsJSON, err := json.Marshal(refs)
	if err != nil {
		return duplicateError(fmt.Errorf("marshal references: %w", err), len(refs))
	}

	addReq := &shardproto.AddReferencesRequest{
		ReferencesJson: refsJSON,
		SchemaVersion:  schemaVersion,
	}
	subCmd, err := proto.Marshal(addReq)
	if err != nil {
		return duplicateError(fmt.Errorf("marshal add references request: %w", err), len(refs))
	}

	compressed := s2.Encode(nil, subCmd)

	req := &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_ADD_REFERENCES,
		Class:      r.class,
		Shard:      shard,
		SubCommand: compressed,
		Compressed: true,
	}
	if _, err := r.apply(ctx, req); err != nil {
		return duplicateError(err, len(refs))
	}

	return make([]error, len(refs))
}

const defaultMaxBatchChunkBytes = 2 * 1024 * 1024 // 2MB per chunk

// ChunkObjectBytes splits serialized objects into chunks where each chunk's
// total size stays under maxBytes. Single objects larger than maxBytes get
// their own chunk (at least one object per chunk).
func ChunkObjectBytes(objectBytes [][]byte, maxBytes int) [][][]byte {
	if len(objectBytes) == 0 {
		return nil
	}

	var chunks [][][]byte
	var current [][]byte
	currentSize := 0

	for _, b := range objectBytes {
		if len(current) > 0 && currentSize+len(b) > maxBytes {
			chunks = append(chunks, current)
			current = nil
			currentSize = 0
		}
		current = append(current, b)
		currentSize += len(b)
	}

	if len(current) > 0 {
		chunks = append(chunks, current)
	}

	return chunks
}

// duplicateError returns a slice of n identical errors.
func duplicateError(err error, n int) []error {
	errs := make([]error, n)
	for i := range errs {
		errs[i] = err
	}
	return errs
}

// duplicateBatchSimpleError returns a BatchSimpleObjects slice with the same
// error for each UUID.
func duplicateBatchSimpleError(err error, uuids []strfmt.UUID) []objects.BatchSimpleObject {
	results := make([]objects.BatchSimpleObject, len(uuids))
	for i, id := range uuids {
		results[i] = objects.BatchSimpleObject{UUID: id, Err: err}
	}
	return results
}

// IsLeader returns true if this node is the leader for the specified shard.
// Note: className is not needed as the Router is already bound to a specific class.
func (r *replicator) IsLeader(shardName string) bool {
	return r.raft.IsLeader(shardName)
}

// VerifyLeaderForRead verifies this node is the leader (for linearizable reads).
func (r *replicator) VerifyLeaderForRead(ctx context.Context, shardName string) error {
	return r.raft.VerifyLeaderForRead(ctx, shardName)
}

// LeaderAddress returns the current leader's address for the specified shard.
func (r *replicator) LeaderAddress(shardName string) string {
	return r.raft.LeaderAddress(shardName)
}
