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

package db

import (
	"context"
	"io"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/cluster/replication/changelog"
	"github.com/weaviate/weaviate/entities/additional"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

// ErrReadOnlyFollower is returned by every mutation entry point of a
// ReadOnlyShard. It is the API-boundary equivalent of the storage-layer
// ErrImmutable: a follower never mutates its copy.
var ErrReadOnlyFollower = errors.New("shard is a read-only follower: writes are not permitted")

// ReadOnlyShard wraps an inner *Shard that was opened read-only (Shard.readOnly)
// and rejects every mutation at the shard boundary. It embeds the inner shard so
// all ~100 read/search/aggregate methods are served directly by the inner shard
// with no duplication (the same composition LazyLoadShard uses for its
// delegations, minus the lazy init); only the write surface is overridden here.
//
// Write rejection is defense-in-depth. The API layer (OperationalMode=READ_ONLY)
// already blocks writes before they reach a shard, and the inner shard's storage
// is itself read-only (immutable LSM buckets, read-only vector FS) so any write
// that slipped through would still fail at the disk boundary. This wrapper makes
// the rejection explicit and early, with a clear read-only-follower error.
type ReadOnlyShard struct {
	*Shard
}

var (
	_ ShardLike                  = (*ReadOnlyShard)(nil)
	_ asyncReplicationController = (*ReadOnlyShard)(nil)
)

// NewReadOnlyShard wraps an inner shard (which must have been constructed with
// Shard.readOnly set) so that it rejects writes at the shard boundary.
func NewReadOnlyShard(inner *Shard) *ReadOnlyShard {
	return &ReadOnlyShard{Shard: inner}
}

// Unwrap returns the inner read-only shard. Mirrors LazyLoadShard's accessor for
// the rare read paths that need the concrete handle.
func (s *ReadOnlyShard) Unwrap() *Shard {
	return s.Shard
}

func readOnlyReplicaResponse() replica.SimpleResponse {
	return replica.SimpleResponse{
		Errors: []replicaerrors.Error{
			*replicaerrors.NewError(replicaerrors.StatusReadOnly, ErrReadOnlyFollower.Error()),
		},
	}
}

// --- object mutations ---

func (s *ReadOnlyShard) PutObject(context.Context, *storobj.Object) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) PutObjectBatch(_ context.Context, objs []*storobj.Object) []error {
	errs := make([]error, len(objs))
	for i := range errs {
		errs[i] = ErrReadOnlyFollower
	}
	return errs
}

func (s *ReadOnlyShard) MergeObject(context.Context, objects.MergeDocument) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) DeleteObject(context.Context, strfmt.UUID, time.Time) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) batchDeleteObject(context.Context, strfmt.UUID, time.Time) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) DeleteObjectBatch(_ context.Context, ids []strfmt.UUID, _ time.Time, _ bool) objects.BatchSimpleObjects {
	out := make(objects.BatchSimpleObjects, len(ids))
	for i, id := range ids {
		out[i] = objects.BatchSimpleObject{UUID: id, Err: ErrReadOnlyFollower}
	}
	return out
}

func (s *ReadOnlyShard) AddReferencesBatch(_ context.Context, refs objects.BatchReferences) []error {
	errs := make([]error, len(refs))
	for i := range errs {
		errs[i] = ErrReadOnlyFollower
	}
	return errs
}

// --- vector index config / lifecycle mutations ---

func (s *ReadOnlyShard) UpdateVectorIndexConfig(context.Context, schemaConfig.VectorIndexConfig) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) UpdateVectorIndexConfigs(context.Context, map[string]schemaConfig.VectorIndexConfig) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) DropVectorIndex(context.Context, string) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) ConvertQueue(string) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) FillQueue(string, uint64) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) SetPropertyLengths([]inverted.Property) error {
	return ErrReadOnlyFollower
}

// --- replica write coordination (defense-in-depth; API layer blocks first) ---

func (s *ReadOnlyShard) preparePutObject(context.Context, string, *storobj.Object) replica.SimpleResponse {
	return readOnlyReplicaResponse()
}

func (s *ReadOnlyShard) preparePutObjects(context.Context, string, []*storobj.Object) replica.SimpleResponse {
	return readOnlyReplicaResponse()
}

func (s *ReadOnlyShard) prepareMergeObject(context.Context, string, *objects.MergeDocument) replica.SimpleResponse {
	return readOnlyReplicaResponse()
}

func (s *ReadOnlyShard) prepareDeleteObject(context.Context, string, strfmt.UUID, time.Time) replica.SimpleResponse {
	return readOnlyReplicaResponse()
}

func (s *ReadOnlyShard) prepareDeleteObjects(context.Context, string, []strfmt.UUID, time.Time, bool) replica.SimpleResponse {
	return readOnlyReplicaResponse()
}

func (s *ReadOnlyShard) prepareAddReferences(context.Context, string, []objects.BatchReference) replica.SimpleResponse {
	return readOnlyReplicaResponse()
}

func (s *ReadOnlyShard) commitReplication(context.Context, string) any {
	return readOnlyReplicaResponse()
}

func (s *ReadOnlyShard) abortReplication(context.Context, string) replica.SimpleResponse {
	return readOnlyReplicaResponse()
}

func (s *ReadOnlyShard) filePutter(context.Context, string) (io.WriteCloser, error) {
	return nil, ErrReadOnlyFollower
}

// --- async replication / change capture / checkpoints (write-side only) ---

func (s *ReadOnlyShard) enableAsyncReplication(context.Context, AsyncReplicationConfig) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) disableAsyncReplication(context.Context) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) addTargetNodeOverride(context.Context, additional.AsyncReplicationTargetNodeOverride) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) removeTargetNodeOverride(context.Context, additional.AsyncReplicationTargetNodeOverride) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) removeAllTargetNodeOverrides(context.Context) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) ActivateChangeLog(context.Context, string) (*changelog.ChangeLog, error) {
	return nil, ErrReadOnlyFollower
}

func (s *ReadOnlyShard) FinalizeChangeLog(context.Context, string) (uint64, error) {
	return 0, ErrReadOnlyFollower
}

func (s *ReadOnlyShard) StopChangeCapture(context.Context, string) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) CreateAsyncCheckpoint(context.Context, int64, time.Time) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) DeleteAsyncCheckpoint(context.Context) error {
	return ErrReadOnlyFollower
}

// --- index maintenance / transfer / backup writes ---

func (s *ReadOnlyShard) HaltForTransfer(context.Context, bool, time.Duration) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) DebugResetVectorIndex(context.Context, string) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) RepairIndex(context.Context, string) error {
	return ErrReadOnlyFollower
}

func (s *ReadOnlyShard) RequantizeIndex(context.Context, string) error {
	return ErrReadOnlyFollower
}
