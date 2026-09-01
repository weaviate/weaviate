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

package replica

import (
	"context"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

const (
	// RequestKey is used to marshalling request IDs
	RequestKey       = "request_id"
	SchemaVersionKey = "schema_version"
)

// Client is used to read and write objects on replicas
type Client interface {
	RClient
	WClient
}

type SimpleResponse struct {
	Errors []replicaerrors.Error `json:"errors,omitempty"`
}

func (r *SimpleResponse) FirstError() error {
	for i, err := range r.Errors {
		if !err.Empty() {
			return &r.Errors[i]
		}
	}
	return nil
}

// DeleteBatchResponse represents the response returned by DeleteObjects
type DeleteBatchResponse struct {
	Batch []UUID2Error `json:"batch,omitempty"`
}

type UUID2Error struct {
	UUID  string              `json:"uuid,omitempty"`
	Error replicaerrors.Error `json:"error,omitempty"`
}

// FirstError returns the first found error
func (r *DeleteBatchResponse) FirstError() error {
	for i, ue := range r.Batch {
		if !ue.Error.Empty() {
			return &r.Batch[i].Error
		}
	}
	return nil
}

type DigestObjectsInRangeReq struct {
	InitialUUID strfmt.UUID `json:"initialUUID,omitempty"`
	FinalUUID   strfmt.UUID `json:"finalUUID,omitempty"`
	Limit       int         `json:"limit,omitempty"`
}

type DigestObjectsInRangeResp struct {
	Digests []types.RepairResponse `json:"digests,omitempty"`
}

// CompareHashTreeRootsReq / Resp are the REST payloads for the batched root pre-filter; roots use raw [high,low] pairs since Digest's pointer-receiver JSON breaks for map values.
type CompareHashTreeRootsReq struct {
	Roots map[string][2]uint64 `json:"roots"`
}

type CompareHashTreeRootsResp struct {
	DivergingShards []string `json:"divergingShards,omitempty"`
}

// CompareHashTreeRootsMultiReq is the cross-class root pre-filter payload: class → shard → raw [high,low] root.
type CompareHashTreeRootsMultiReq struct {
	Classes map[string]map[string][2]uint64 `json:"classes"`
}

type CompareHashTreeRootsMultiResp struct {
	Classes map[string]CompareHashTreeRootsMultiClassResp `json:"classes"`
}

// CompareHashTreeRootsMultiClassResp: Error set ⇒ receiver could not compare this class, sender descends its shards.
type CompareHashTreeRootsMultiClassResp struct {
	DivergingShards []string `json:"divergingShards,omitempty"`
	Error           string   `json:"error,omitempty"`
}

// WClient is the client used to write to replicas
type WClient interface {
	PutObject(ctx context.Context, host, index, shard, requestID string,
		obj *storobj.Object, schemaVersion uint64) (SimpleResponse, error)
	DeleteObject(ctx context.Context, host, index, shard, requestID string,
		id strfmt.UUID, deletionTime time.Time, schemaVersion uint64) (SimpleResponse, error)
	PutObjects(ctx context.Context, host, index, shard, requestID string,
		objs []*storobj.Object, schemaVersion uint64) (SimpleResponse, error)
	MergeObject(ctx context.Context, host, index, shard, requestID string,
		mergeDoc *objects.MergeDocument, schemaVersion uint64) (SimpleResponse, error)
	DeleteObjects(ctx context.Context, host, index, shard, requestID string,
		uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64) (SimpleResponse, error)
	AddReferences(ctx context.Context, host, index, shard, requestID string,
		refs []objects.BatchReference, schemaVersion uint64) (SimpleResponse, error)
	Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error
	Abort(ctx context.Context, host, index, shard, requestID string) (SimpleResponse, error)
}

// RClient is the client used to read from remote replicas
type RClient interface {
	// FetchObject fetches one object
	FetchObject(_ context.Context, host, index, shard string,
		id strfmt.UUID, props search.SelectProperties,
		additional additional.Properties, numRetries int) (Replica, error)

	// FetchObjects fetches objects specified in ids list.
	FetchObjects(_ context.Context, host, index, shard string,
		ids []strfmt.UUID) ([]Replica, error)

	// OverwriteObjects conditionally updates existing objects.
	OverwriteObjects(_ context.Context, host, index, shard string,
		_ []*objects.VObject) ([]types.RepairResponse, error)

	// DigestObjects finds a list of objects and returns a compact representation
	// of a list of the objects. This is used by the replicator to optimize the
	// number of bytes transferred over the network when fetching a replicated
	// object
	DigestObjects(ctx context.Context, host, index, shard string,
		ids []strfmt.UUID, numRetries int) ([]types.RepairResponse, error)

	FindUUIDs(ctx context.Context, host, index, shard string,
		filters *filters.LocalFilter, limit int) ([]strfmt.UUID, error)

	DigestObjectsInRange(ctx context.Context, host, index, shard string,
		initialUUID, finalUUID strfmt.UUID, limit int) ([]types.RepairDigest, error)

	// CompareDigests sends the source's local digests to the target and returns
	// only the subset needing source-side action: objects missing on the target
	// (UpdateTime==0 — also how target-side tombstones surface; the source then
	// proposes an Overwrite and settles any deletion conflict per DeletionStrategy)
	// and objects the source holds a strictly newer version of. Equal-timestamp
	// objects are never returned (identical hashtree digests, hence already
	// invisible to the hashtree diff that drives this call).
	CompareDigests(ctx context.Context, host, index, shard string,
		digests []types.RepairDigest) ([]types.RepairDigest, error)

	HashTreeLevel(ctx context.Context, host, index, shard string, level int,
		discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error)

	// CompareHashTreeRoots batches the level-0 root compare of many shards, returning
	// the diverging subset. Returns ErrCompareHashTreeRootsUnsupported on too-old targets.
	CompareHashTreeRoots(ctx context.Context, host, index string,
		roots map[string]hashtree.Digest) (divergingShards []string, err error)

	CountObjects(ctx context.Context, host, index, shard string) (int, error)

	// Async-checkpoint RPCs: createdAt is the initiator's value, propagated unchanged.
	GetAsyncCheckpointStatus(ctx context.Context, host, index string, shardNames []string) (map[string]AsyncCheckpointShardStatus, error)
	CreateAsyncCheckpoint(ctx context.Context, host, index string, shardNames []string, cutoffMs int64, createdAt time.Time) error
	DeleteAsyncCheckpoint(ctx context.Context, host, index string, shardNames []string) error
}

// FinderClient extends RClient with consistency checks
type FinderClient struct {
	cl  RClient
	log logrus.FieldLogger
}

func NewFinderClient(cl RClient, log logrus.FieldLogger) FinderClient {
	return FinderClient{cl: cl, log: log}
}

// FullRead reads full object
func (fc FinderClient) FullRead(ctx context.Context,
	host, index, shard string,
	id strfmt.UUID,
	props search.SelectProperties,
	additional additional.Properties,
	numRetries int,
) (Replica, error) {
	return fc.cl.FetchObject(ctx, host, index, shard, id, props, additional, numRetries)
}

func (fc FinderClient) HashTreeLevel(ctx context.Context,
	host, index, shard string, level int, discriminant *hashtree.Bitset,
) (digests []hashtree.Digest, err error) {
	return fc.cl.HashTreeLevel(ctx, host, index, shard, level, discriminant)
}

// DigestReads reads digests of all specified objects
func (fc FinderClient) DigestReads(ctx context.Context,
	host, index, shard string,
	ids []strfmt.UUID, numRetries int,
) ([]types.RepairResponse, error) {
	n := len(ids)
	rs, err := fc.cl.DigestObjects(ctx, host, index, shard, ids, numRetries)
	if err == nil && len(rs) != n {
		err = fmt.Errorf("malformed digest read response: length expected %d got %d", n, len(rs))
	}
	return rs, err
}

func (fc FinderClient) DigestObjectsInRange(ctx context.Context,
	host, index, shard string,
	initialUUID, finalUUID strfmt.UUID, limit int,
) ([]types.RepairDigest, error) {
	return fc.cl.DigestObjectsInRange(ctx, host, index, shard, initialUUID, finalUUID, limit)
}

func (fc FinderClient) CompareDigests(ctx context.Context,
	host, index, shard string,
	digests []types.RepairDigest,
) ([]types.RepairDigest, error) {
	return fc.cl.CompareDigests(ctx, host, index, shard, digests)
}

func (fc FinderClient) CompareHashTreeRoots(ctx context.Context,
	host, index string, roots map[string]hashtree.Digest,
) ([]string, error) {
	return fc.cl.CompareHashTreeRoots(ctx, host, index, roots)
}

// MaxFullReadIDsPerRequest bounds ids per FetchObjects request. The REST
// transport base64-encodes them into the URL query string at ~53 bytes per id,
// so 256 ids is ~14 KB: well inside the receiving server's 1 MiB header cap
// (MaxHeaderBytes is unset) and the 60 KiB a service-mesh sidecar (Envoy
// default) allows on node-to-node traffic. An unbounded list would overflow
// those caps with a 414 that is not retried. The chunk also caps how many
// whole objects one response holds in memory.
const MaxFullReadIDsPerRequest = 256

// MaxConcurrentFullReadRequests bounds how many chunked FetchObjects requests
// are in flight at once against the single winning host, capping peak response
// memory at MaxConcurrentFullReadRequests * MaxFullReadIDsPerRequest whole
// objects per FullReads call. The repairer runs one FullReads per winning
// replica concurrently, so the ceiling per repaired batch is that product
// times the number of winning replicas (at most the replication factor).
const MaxConcurrentFullReadRequests = 16

// FullReads reads the current version of each id from host, one entry per
// requested id in request order. Ids are fetched in bounded chunks, chunks
// concurrently; any failed chunk fails the whole read. A response that does
// not line up with its request is rejected rather than returned: callers
// index the result positionally, so a mispair would repair the wrong object.
func (fc FinderClient) FullReads(ctx context.Context,
	host, index, shard string,
	ids []strfmt.UUID,
) ([]Replica, error) {
	if len(ids) <= MaxFullReadIDsPerRequest {
		return fc.fullReadChunk(ctx, host, index, shard, ids, 0)
	}

	rs := make([]Replica, len(ids))
	gr, ctx := enterrors.NewErrorGroupWithContextWrapper(fc.log, ctx)
	gr.SetLimit(MaxConcurrentFullReadRequests)
	for start := 0; start < len(ids); start += MaxFullReadIDsPerRequest {
		start, end := start, min(start+MaxFullReadIDsPerRequest, len(ids))
		gr.Go(func() error {
			part, err := fc.fullReadChunk(ctx, host, index, shard, ids[start:end], start)
			if err != nil {
				return err
			}
			copy(rs[start:end], part)
			return nil
		})
	}
	if err := gr.Wait(); err != nil {
		return nil, err
	}
	return rs, nil
}

// fullReadChunk performs one FetchObjects request and validates that the
// response carries exactly the requested ids in request order. offset is the
// chunk's position in the whole id list, so errors name absolute indices.
func (fc FinderClient) fullReadChunk(ctx context.Context,
	host, index, shard string,
	chunk []strfmt.UUID, offset int,
) ([]Replica, error) {
	part, err := fc.cl.FetchObjects(ctx, host, index, shard, chunk)
	if err != nil {
		return nil, err
	}
	if len(part) != len(chunk) {
		return nil, fmt.Errorf("malformed full read response: length expected %d got %d",
			len(chunk), len(part))
	}
	for i := range part {
		if part[i].ID != chunk[i] {
			return nil, fmt.Errorf("malformed full read response: object %d is %q, expected %q",
				offset+i, part[i].ID, chunk[i])
		}
	}
	return part, nil
}

// Overwrite specified object with most recent contents
func (fc FinderClient) Overwrite(ctx context.Context,
	host, index, shard string,
	xs []*objects.VObject,
) ([]types.RepairResponse, error) {
	return fc.cl.OverwriteObjects(ctx, host, index, shard, xs)
}

func (fc FinderClient) FindUUIDs(ctx context.Context,
	host, class, shard string, filters *filters.LocalFilter, limit int,
) ([]strfmt.UUID, error) {
	return fc.cl.FindUUIDs(ctx, host, class, shard, filters, limit)
}
