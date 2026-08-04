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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/klauspost/compress/s2"
	"github.com/sirupsen/logrus"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ErrNoLeaderFound is returned when no leader can be found for a shard.
var ErrNoLeaderFound = errors.New("no leader found for shard")

// ErrNotLeaderForRead is returned when a DIRECT consistency read is attempted
// on a non-leader node. The caller should forward the read to the leader.
var ErrNotLeaderForRead = errors.New("not leader: forward read to leader")

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
	CountObjects(ctx context.Context, shard string, l routerTypes.ConsistencyLevel) (int, error)
	WaitForDrain(ctx context.Context, shard string) error
	BroadcastCreateAsyncCheckpoint(ctx context.Context, shardNames []string, cutoffMs int64, createdAt time.Time) (successes, failures int)
	BroadcastDeleteAsyncCheckpoint(ctx context.Context, shardNames []string) (successes, failures int)
	BroadcastGetAsyncCheckpointStatus(ctx context.Context, shardNames []string) (statuses map[string][]replica.AsyncCheckpointNodeStatus, successes, failures int)
	CompareDigests(ctx context.Context, shardName string, host string, digests []routerTypes.RepairResponse) ([]routerTypes.RepairResponse, error)
	PrefilterShardRoots(ctx context.Context, roots map[string]hashtree.Digest) (map[string]struct{}, replica.PrefilterStats)
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

	// ApplyRetryBudget bounds the total time apply spends absorbing
	// retryable leadership churn (elections, transfers, proposal
	// backpressure) before surfacing an error. Zero means the default:
	// max(10s, 3x ElectionTimeout) — one full election plus catch-up. The
	// caller's context still has the last word: a client deadline shorter
	// than an election surfaces errors regardless of this budget.
	ApplyRetryBudget time.Duration
	// ApplyAttemptTimeout bounds a single local Apply or forwarded RPC, so
	// one attempt wedged against a dying leader cannot consume the whole
	// retry budget. Zero means the default: max(5s, 2x ElectionTimeout).
	ApplyAttemptTimeout time.Duration
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

// applyRetryBudgetFloor / applyAttemptTimeoutFloor are the minimums for the
// retry knobs when the election-timeout-derived values are smaller. The
// budget must cover one full election plus leader catch-up; the attempt
// timeout must cover a healthy-but-loaded Apply (commit + local apply).
const (
	applyRetryBudgetFloor    = 10 * time.Second
	applyAttemptTimeoutFloor = 5 * time.Second
)

func (r *replicator) retryBudget() time.Duration {
	if r.config.ApplyRetryBudget > 0 {
		return r.config.ApplyRetryBudget
	}
	if b := 3 * r.raft.config.ElectionTimeout; b > applyRetryBudgetFloor {
		return b
	}
	return applyRetryBudgetFloor
}

func (r *replicator) attemptTimeout() time.Duration {
	if r.config.ApplyAttemptTimeout > 0 {
		return r.config.ApplyAttemptTimeout
	}
	if a := 2 * r.raft.config.ElectionTimeout; a > applyAttemptTimeoutFloor {
		return a
	}
	return applyAttemptTimeoutFloor
}

// isRetryableApplyErr is the single retry-classification table for the write
// path, local and forwarded. Retryable means: transient leadership or
// availability churn that a bounded server-side retry absorbs so the client
// never sees it — not-leader (reroute), leadership lost mid-apply, proposal
// backpressure (same-node retry), no leader known yet, and a timed-out
// attempt (the parent context separately gets the last word). Forwarded
// attempts are classified purely by gRPC status code; toRPCError on the
// serving side is the producing half of this contract.
func isRetryableApplyErr(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrNotLeader),
		errors.Is(err, ErrLeadershipLost),
		errors.Is(err, ErrProposalBackpressure),
		errors.Is(err, ErrNoLeaderFound),
		errors.Is(err, ErrLeaderElectionTimeout):
		return true
	case errors.Is(err, context.DeadlineExceeded):
		// A per-attempt sub-deadline fired (e.g. wedged against a dying
		// leader); the next attempt re-resolves. apply() checks the parent
		// context before retrying.
		return true
	case errors.Is(err, storagestate.ErrStatusReadOnly), errors.Is(err, ErrClassDropped):
		// Admission rejections from the local leader route (Store.Apply's
		// reject-fast): explicitly non-retryable — the reason must reach the
		// client intact. The forwarded route agrees: toRPCError maps both to
		// codes.FailedPrecondition, which is not in the retryable code set
		// below.
		return false
	}
	switch status.Code(err) {
	case NotLeaderRPCCode, codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// apply drives one command through the shard's RAFT group, absorbing
// transient leadership churn server-side: every attempt re-resolves the
// route (leadership may move onto or off this node between attempts), a
// per-attempt sub-deadline keeps one wedged attempt from eating the budget,
// and retries stop at the retry budget or the caller's context, whichever
// ends first. This is the ONLY retry site on the write path — Server.Apply
// on the remote side calls Store.Apply once and reports typed codes back.
func (r *replicator) apply(ctx context.Context, req *shardproto.ApplyRequest) (*shardproto.ApplyResponse, error) {
	store := r.raft.GetStore(req.Shard)
	if store == nil {
		return nil, fmt.Errorf("raft store not found for %s/%s", req.Class, req.Shard)
	}

	budget := r.retryBudget()
	deadline := time.Now().Add(budget)
	wait := r.raft.config.ElectionTimeout / 20
	if wait <= 0 {
		wait = 25 * time.Millisecond
	}
	maxWait := r.raft.config.ElectionTimeout
	if maxWait <= 0 {
		maxWait = time.Second
	}

	// giveUp shapes the terminal error: a run that ends while no leader was
	// ever found keeps the typed ErrLeaderElectionTimeout surface callers
	// match on (the pre-retry contract of the WaitForLeader-first path).
	giveUp := func(err error) error {
		if errors.Is(err, ErrNoLeaderFound) {
			err = fmt.Errorf("%w: %w", ErrLeaderElectionTimeout, err)
		}
		return err
	}

	for attempt := 1; ; attempt++ {
		resp, err := r.applyOnce(ctx, store, req)
		if err == nil {
			return resp, nil
		}
		if !isRetryableApplyErr(err) {
			return nil, err
		}
		if ctx.Err() != nil {
			// The caller's deadline has the last word.
			return nil, fmt.Errorf("%w (last apply error: %w)", ctx.Err(), giveUp(err))
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, fmt.Errorf("apply retry budget %v exhausted after %d attempts: %w", budget, attempt, giveUp(err))
		}
		r.log.WithFields(logrus.Fields{
			"class":   req.Class,
			"shard":   req.Shard,
			"attempt": attempt,
		}).Debugf("retrying apply after transient error: %v", err)

		sleep := wait
		if sleep > remaining {
			sleep = remaining
		}
		timer := time.NewTimer(sleep)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("%w (last apply error: %w)", ctx.Err(), giveUp(err))
		}
		if wait *= 2; wait > maxWait {
			wait = maxWait
		}
		// After leadership-shaped failures, wait for a leader to be known
		// before re-resolving rather than hot-spinning against a leaderless
		// window. Backpressure is a same-node condition — no wait needed.
		if !errors.Is(err, ErrProposalBackpressure) {
			waitCtx, cancel := context.WithDeadline(ctx, deadline)
			_ = store.WaitForLeader(waitCtx) // outcome re-checked by the next attempt
			cancel()
		}
	}
}

// applyOnce performs one attempt at the current best-known route: locally
// when this node leads the shard, otherwise forwarded to the current leader.
func (r *replicator) applyOnce(ctx context.Context, store *Store, req *shardproto.ApplyRequest) (*shardproto.ApplyResponse, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, r.attemptTimeout())
	defer cancel()

	if store.IsLeader() {
		v, err := store.Apply(attemptCtx, req)
		if err != nil {
			return nil, err
		}
		return &shardproto.ApplyResponse{Version: v}, nil
	}

	leaderID := store.LeaderID()
	if leaderID == "" {
		return nil, ErrNoLeaderFound
	}
	client, err := r.rpcClientMaker(ctx, leaderID)
	if err != nil {
		// Client construction fails while the leader address is stale or the
		// leader just died — same shape as "no reachable leader": retryable.
		return nil, fmt.Errorf("%w: create RPC client for leader %s: %w", ErrNoLeaderFound, leaderID, err)
	}
	return client.Apply(attemptCtx, req)
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
		store := r.raft.GetStore(shard)
		if store.IsLeader() {
			if err := store.VerifyLeader(ctx); err != nil {
				return nil, fmt.Errorf("verify leader: %w", err)
			}
			return r.findUUIDsLocal(ctx, shard, f, limit)
		}
		// No direct leader-forwarding RPC for FindUUIDs; signal caller to forward.
		return nil, ErrNotLeaderForRead

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

// verifyLeaderApplied drives the leader-side linearizable read barrier ahead
// of a forwarded DIRECT read: GetLastAppliedIndex with VerifyLeader invokes
// the leader's VerifyLeader, which quorum-confirms leadership and waits for
// the leader's FSM to apply at least its ReadState's commit index. Apply acks
// at quorum commit, so without this barrier even the true leader could serve
// a forwarded read from state that lacks an acknowledged write.
func (r *replicator) verifyLeaderApplied(ctx context.Context, shardName, leaderID string) error {
	client, err := r.rpcClientMaker(ctx, leaderID)
	if err != nil {
		return fmt.Errorf("%w: create RPC client for leader %s: %w", ErrNoLeaderFound, leaderID, err)
	}
	if _, err := client.GetLastAppliedIndex(ctx, &shardproto.GetLastAppliedIndexRequest{
		Class:        r.class,
		Shard:        shardName,
		VerifyLeader: true,
	}); err != nil {
		return fmt.Errorf("verify leader before forwarded read: %w", err)
	}
	return nil
}

// readFromLeader reads from the leader for DIRECT consistency.
// If this node is the leader, reads locally after the VerifyLeader barrier
// (leadership plus applied-wait). If not, drives the same barrier on the
// leader via RPC, then forwards the read via the backing replicator's
// NodeObject.
func (r *replicator) readFromLeader(ctx context.Context, shard string, id strfmt.UUID, props search.SelectProperties, adds additional.Properties) (*storobj.Object, error) {
	store := r.raft.GetStore(shard)
	if store == nil {
		return nil, fmt.Errorf("raft store not found for shard %s", shard)
	}

	if store.IsLeader() {
		if err := store.VerifyLeader(ctx); err != nil {
			return nil, fmt.Errorf("verify leader: %w", err)
		}
		return r.readLocalObject(ctx, shard, id, props, adds)
	}

	leaderID := store.LeaderID()
	if leaderID == "" {
		return nil, ErrNoLeaderFound
	}
	if err := r.verifyLeaderApplied(ctx, shard, leaderID); err != nil {
		return nil, err
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
		if err := store.VerifyLeader(ctx); err != nil {
			return false, fmt.Errorf("verify leader: %w", err)
		}
		return r.existsLocal(ctx, shard, id)
	}

	// Forward to leader via NodeObject — a nil result means the object doesn't
	// exist. The barrier mirrors readFromLeader: the leader must have applied
	// every acked write before it serves the forwarded read.
	leaderID := store.LeaderID()
	if leaderID == "" {
		return false, ErrNoLeaderFound
	}
	if err := r.verifyLeaderApplied(ctx, shard, leaderID); err != nil {
		return false, err
	}
	obj, err := r.NodeObject(ctx, leaderID, shard, id, search.SelectProperties{}, additional.Properties{})
	if err != nil {
		return false, err
	}
	return obj != nil, nil
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
			if err := store.VerifyLeader(ctx); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, nil // Caller checks localReady and forwards to leader

	default:
		return true, nil
	}
}
