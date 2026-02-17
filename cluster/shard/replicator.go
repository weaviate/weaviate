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

// ErrNoLeaderFound is returned when no leader can be found for a shard.
var ErrNoLeaderFound = errors.New("no leader found for shard")

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
	EnsureReadConsistency(ctx context.Context, shardName string, cl routerTypes.ConsistencyLevel) (localReady bool, err error)
	PutObject(ctx context.Context, shard string, obj *storobj.Object, l routerTypes.ConsistencyLevel, schemaVersion uint64) error
	PutObjects(ctx context.Context, shard string, objs []*storobj.Object, l routerTypes.ConsistencyLevel, schemaVersion uint64) []error
}

// ShardReader provides read access to a local shard.
// Implemented by adapters/repos/db.ShardLike.
type ShardReader interface {
	ObjectByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, adds additional.Properties) (*storobj.Object, error)
	Exists(ctx context.Context, id strfmt.UUID) (bool, error)
	FindUUIDs(ctx context.Context, filters *filters.LocalFilter, limit int) ([]strfmt.UUID, error)
}

// ShardReaderProvider resolves a shard name to a local ShardReader.
// Returns the reader and a release function. Returns nil reader if the
// shard is not locally available.
type ShardReaderProvider func(shardName string) (ShardReader, func(), error)

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
	// LocalShardReader resolves a shard name to a local reader for direct reads.
	LocalShardReader ShardReaderProvider
	// Registry is the shard RAFT registry (for ReadIndex protocol).
	Registry *Registry
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

// GetOne overrides the backing replicator for RAFT-backed shards.
func (r *replicator) GetOne(ctx context.Context, l routerTypes.ConsistencyLevel, shard string, id strfmt.UUID, props search.SelectProperties, adds additional.Properties) (*storobj.Object, error) {
	isRaftShard := r.raft.GetStore(shard) != nil

	// Non-RAFT shard: delegate to backing replicator (mapping RAFT CLs to 2PC equivalents)
	if !isRaftShard {
		cl := l
		if l.IsRaft() {
			cl = l.MapTo2PC()
		}
		return r.Replicator.GetOne(ctx, cl, shard, id, props, adds)
	}

	// RAFT shard: reject 2PC CLs
	if l.Is2PC() {
		return nil, fmt.Errorf("consistency level %s is not supported for RAFT-backed shards; use EVENTUAL, STRONG, or DIRECT", l)
	}

	switch l {
	case routerTypes.ConsistencyLevelEventual:
		return r.readLocalObject(ctx, shard, id, props, adds)

	case routerTypes.ConsistencyLevelStrong:
		if err := r.ensureReadIndex(ctx, shard); err != nil {
			return nil, fmt.Errorf("strong read: %w", err)
		}
		return r.readLocalObject(ctx, shard, id, props, adds)

	case routerTypes.ConsistencyLevelDirect:
		return r.readFromLeader(ctx, shard, id, props, adds)

	default:
		return nil, fmt.Errorf("unsupported consistency level: %s", l)
	}
}

// Exists overrides the backing replicator for RAFT-backed shards.
func (r *replicator) Exists(ctx context.Context, l routerTypes.ConsistencyLevel, shard string, id strfmt.UUID) (bool, error) {
	isRaftShard := r.raft.GetStore(shard) != nil

	if !isRaftShard {
		cl := l
		if l.IsRaft() {
			cl = l.MapTo2PC()
		}
		return r.Replicator.Exists(ctx, cl, shard, id)
	}

	if l.Is2PC() {
		return false, fmt.Errorf("consistency level %s is not supported for RAFT-backed shards; use EVENTUAL, STRONG, or DIRECT", l)
	}

	switch l {
	case routerTypes.ConsistencyLevelEventual:
		return r.existsLocal(ctx, shard, id)

	case routerTypes.ConsistencyLevelStrong:
		if err := r.ensureReadIndex(ctx, shard); err != nil {
			return false, fmt.Errorf("strong read: %w", err)
		}
		return r.existsLocal(ctx, shard, id)

	case routerTypes.ConsistencyLevelDirect:
		return r.existsFromLeader(ctx, shard, id)

	default:
		return false, fmt.Errorf("unsupported consistency level: %s", l)
	}
}

// FindUUIDs overrides the backing replicator for RAFT-backed shards.
func (r *replicator) FindUUIDs(ctx context.Context, className string, shard string, f *filters.LocalFilter, l routerTypes.ConsistencyLevel, limit int) ([]strfmt.UUID, error) {
	isRaftShard := r.raft.GetStore(shard) != nil

	if !isRaftShard {
		cl := l
		if l.IsRaft() {
			cl = l.MapTo2PC()
		}
		return r.Replicator.FindUUIDs(ctx, className, shard, f, cl, limit)
	}

	if l.Is2PC() {
		return nil, fmt.Errorf("consistency level %s is not supported for RAFT-backed shards; use EVENTUAL, STRONG, or DIRECT", l)
	}

	switch l {
	case routerTypes.ConsistencyLevelEventual:
		return r.findUUIDsLocal(ctx, shard, f, limit)

	case routerTypes.ConsistencyLevelStrong:
		if err := r.ensureReadIndex(ctx, shard); err != nil {
			return nil, fmt.Errorf("strong read: %w", err)
		}
		return r.findUUIDsLocal(ctx, shard, f, limit)

	case routerTypes.ConsistencyLevelDirect:
		// For DIRECT FindUUIDs, ensure we're on leader or fall back to backing replicator
		store := r.raft.GetStore(shard)
		if store.IsLeader() {
			if err := store.VerifyLeader(); err != nil {
				return nil, fmt.Errorf("verify leader: %w", err)
			}
			return r.findUUIDsLocal(ctx, shard, f, limit)
		}
		// Forward via backing replicator mapped to ALL (leader-read)
		return r.Replicator.FindUUIDs(ctx, className, shard, f, routerTypes.ConsistencyLevelAll, limit)

	default:
		return nil, fmt.Errorf("unsupported consistency level: %s", l)
	}
}

// CheckConsistency overrides the backing replicator for RAFT CLs.
// For RAFT CLs, consistency is ensured at read time (ReadIndex or leader read),
// so skip the post-read digest check.
func (r *replicator) CheckConsistency(ctx context.Context, l routerTypes.ConsistencyLevel, objs []*storobj.Object) error {
	if l.IsRaft() {
		return nil // Consistency already ensured by ReadIndex/VerifyLeader
	}
	return r.Replicator.CheckConsistency(ctx, l, objs)
}

// readLocalObject reads from the local shard via shardReaderProvider.
func (r *replicator) readLocalObject(ctx context.Context, shard string, id strfmt.UUID, props search.SelectProperties, adds additional.Properties) (*storobj.Object, error) {
	reader, release, err := r.config.LocalShardReader(shard)
	if err != nil {
		return nil, fmt.Errorf("get local shard reader: %w", err)
	}
	if reader == nil {
		return nil, fmt.Errorf("shard %s not available locally", shard)
	}
	defer release()
	return reader.ObjectByID(ctx, id, props, adds)
}

// existsLocal checks existence via the local shard reader.
func (r *replicator) existsLocal(ctx context.Context, shard string, id strfmt.UUID) (bool, error) {
	reader, release, err := r.config.LocalShardReader(shard)
	if err != nil {
		return false, fmt.Errorf("get local shard reader: %w", err)
	}
	if reader == nil {
		return false, fmt.Errorf("shard %s not available locally", shard)
	}
	defer release()
	return reader.Exists(ctx, id)
}

// findUUIDsLocal finds UUIDs via the local shard reader.
func (r *replicator) findUUIDsLocal(ctx context.Context, shard string, f *filters.LocalFilter, limit int) ([]strfmt.UUID, error) {
	reader, release, err := r.config.LocalShardReader(shard)
	if err != nil {
		return nil, fmt.Errorf("get local shard reader: %w", err)
	}
	if reader == nil {
		return nil, fmt.Errorf("shard %s not available locally", shard)
	}
	defer release()
	return reader.FindUUIDs(ctx, f, limit)
}

// ensureReadIndex performs the ReadIndex protocol for STRONG consistency.
// Delegates to Registry.WaitForLinearizableRead which handles:
// - Leader: VerifyLeader (no RPC needed)
// - Follower: GetLastAppliedIndex RPC with VerifyLeader=true → wait for local FSM
func (r *replicator) ensureReadIndex(ctx context.Context, shardName string) error {
	return r.config.Registry.WaitForLinearizableRead(ctx, r.config.ClassName, shardName)
}

// readFromLeader reads from the leader for DIRECT consistency.
// If this node is the leader, reads locally after verifying leadership.
// If not, forwards the read to the leader node via the backing replicator's NodeObject.
func (r *replicator) readFromLeader(ctx context.Context, shard string, id strfmt.UUID, props search.SelectProperties, adds additional.Properties) (*storobj.Object, error) {
	store := r.raft.GetStore(shard)
	if store == nil {
		return nil, fmt.Errorf("raft store not found for shard %s", shard)
	}

	if store.IsLeader() {
		if err := store.VerifyLeader(); err != nil {
			return nil, fmt.Errorf("verify leader: %w", err)
		}
		return r.readLocalObject(ctx, shard, id, props, adds)
	}

	// Forward to leader using the backing replicator's NodeObject
	leaderID := store.LeaderID()
	if leaderID == "" {
		return nil, ErrNoLeaderFound
	}
	return r.NodeObject(ctx, leaderID, shard, id, props, adds)
}

// existsFromLeader checks existence from the leader for DIRECT consistency.
func (r *replicator) existsFromLeader(ctx context.Context, shard string, id strfmt.UUID) (bool, error) {
	store := r.raft.GetStore(shard)
	if store == nil {
		return false, fmt.Errorf("raft store not found for shard %s", shard)
	}

	if store.IsLeader() {
		if err := store.VerifyLeader(); err != nil {
			return false, fmt.Errorf("verify leader: %w", err)
		}
		return r.existsLocal(ctx, shard, id)
	}

	// For Exists, there's no direct NodeObject equivalent — fall back to backing replicator
	// with ALL consistency level which reads from all replicas (including leader).
	return r.Replicator.Exists(ctx, routerTypes.ConsistencyLevelAll, shard, id)
}

// EnsureReadConsistency ensures a shard is ready for a consistent read under RAFT CLs.
// For EVENTUAL: no-op, local shard is ready.
// For STRONG: performs ReadIndex protocol (via WaitForLinearizableRead), local shard is ready.
// For DIRECT: verifies leadership; returns false if this node isn't leader (caller should forward).
// Returns true if the local shard is ready for a consistent read.
func (r *replicator) EnsureReadConsistency(ctx context.Context, shardName string, cl routerTypes.ConsistencyLevel) (bool, error) {
	if !cl.IsRaft() || r.raft.GetStore(shardName) == nil {
		return true, nil // Not RAFT, proceed normally
	}

	switch cl {
	case routerTypes.ConsistencyLevelEventual:
		return true, nil

	case routerTypes.ConsistencyLevelStrong:
		if err := r.ensureReadIndex(ctx, shardName); err != nil {
			return false, err
		}
		return true, nil

	case routerTypes.ConsistencyLevelDirect:
		store := r.raft.GetStore(shardName)
		if store.IsLeader() {
			if err := store.VerifyLeader(); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, nil // Caller should forward search to leader

	default:
		return true, nil
	}
}
