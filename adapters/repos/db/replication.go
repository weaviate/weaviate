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
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/cluster/replication/changelog"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

func (db *DB) ReplicateObject(ctx context.Context, class,
	shard, requestID string, object *storobj.Object,
	schemaVersion uint64,
) replica.SimpleResponse {
	if resp := db.waitForSchemaVersionForIndexWrite(ctx, schemaVersion); resp != nil {
		return *resp
	}

	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateObject(ctx, shard, requestID, object, schemaVersion)
}

func (db *DB) ReplicateObjects(ctx context.Context, class,
	shard, requestID string, objects []*storobj.Object, schemaVersion uint64,
) replica.SimpleResponse {
	if resp := db.waitForSchemaVersionForIndexWrite(ctx, schemaVersion); resp != nil {
		return *resp
	}

	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateObjects(ctx, shard, requestID, objects, schemaVersion)
}

func (db *DB) ReplicateUpdate(ctx context.Context, class,
	shard, requestID string, mergeDoc *objects.MergeDocument,
	schemaVersion uint64,
) replica.SimpleResponse {
	if resp := db.waitForSchemaVersionForIndexWrite(ctx, schemaVersion); resp != nil {
		return *resp
	}

	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateUpdate(ctx, shard, requestID, mergeDoc, schemaVersion)
}

func (db *DB) ReplicateDeletion(ctx context.Context, class,
	shard, requestID string, uuid strfmt.UUID, deletionTime time.Time,
	schemaVersion uint64,
) replica.SimpleResponse {
	if resp := db.waitForSchemaVersionForIndexWrite(ctx, schemaVersion); resp != nil {
		return *resp
	}

	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateDeletion(ctx, shard, requestID, uuid, deletionTime, schemaVersion)
}

func (db *DB) ReplicateDeletions(ctx context.Context, class,
	shard, requestID string, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) replica.SimpleResponse {
	if resp := db.waitForSchemaVersionForIndexWrite(ctx, schemaVersion); resp != nil {
		return *resp
	}

	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateDeletions(ctx, shard, requestID, uuids, deletionTime, dryRun, schemaVersion)
}

func (db *DB) ReplicateReferences(ctx context.Context, class,
	shard, requestID string, refs []objects.BatchReference,
	schemaVersion uint64,
) replica.SimpleResponse {
	if resp := db.waitForSchemaVersionForIndexWrite(ctx, schemaVersion); resp != nil {
		return *resp
	}

	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateReferences(ctx, shard, requestID, refs, schemaVersion)
}

func (db *DB) OverwriteObjects(ctx context.Context, className, shard string, vobjects []*objects.VObject) ([]types.RepairResponse, error) {
	index, pr := db.replicatedIndex(className)
	if pr != nil {
		return nil, pr.FirstError()
	}
	return index.OverwriteObjects(ctx, shard, vobjects)
}

func (db *DB) FetchObject(ctx context.Context, className, shardName string, id strfmt.UUID) (replica.Replica, error) {
	index, pr := db.replicatedIndex(className)
	if pr != nil {
		return replica.Replica{}, pr.FirstError()
	}
	return index.FetchObject(ctx, shardName, id)
}

func (db *DB) FetchObjects(ctx context.Context, className, shardName string, ids []strfmt.UUID) ([]replica.Replica, error) {
	index, pr := db.replicatedIndex(className)
	if pr != nil {
		return nil, pr.FirstError()
	}
	return index.FetchObjects(ctx, shardName, ids)
}

func (db *DB) DigestObjects(ctx context.Context, className, shardName string, ids []strfmt.UUID) (result []types.RepairResponse, err error) {
	index, pr := db.replicatedIndex(className)
	if pr != nil {
		return nil, pr.FirstError()
	}
	return index.DigestObjects(ctx, shardName, ids)
}

func (db *DB) DigestObjectsInRange(ctx context.Context, className, shardName string, initialUUID, finalUUID strfmt.UUID, limit int) (result []types.RepairResponse, err error) {
	index, pr := db.replicatedIndex(className)
	if pr != nil {
		return nil, pr.FirstError()
	}
	return index.DigestObjectsInRange(ctx, shardName, initialUUID, finalUUID, limit)
}

func (db *DB) CompareDigests(ctx context.Context, className, shardName string, digests []types.RepairResponse) ([]types.RepairResponse, error) {
	index, pr := db.replicatedIndex(className)
	if pr != nil {
		return nil, pr.FirstError()
	}
	return index.CompareDigests(ctx, shardName, digests)
}

func (db *DB) HashTreeLevel(ctx context.Context, className, shardName string, level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error) {
	index, pr := db.replicatedIndex(className)
	if pr != nil {
		return nil, pr.FirstError()
	}
	return index.HashTreeLevel(ctx, shardName, level, discriminant)
}

func (db *DB) CountObjects(ctx context.Context, indexName string, shardName string) (int, error) {
	index, pr := db.replicatedIndex(indexName)
	if pr != nil {
		return 0, pr.FirstError()
	}
	return index.CountObjects(ctx, shardName)
}

func (db *DB) FindUUIDs(ctx context.Context, indexName, shardName string,
	f *filters.LocalFilter, limit int,
) ([]strfmt.UUID, error) {
	index, pr := db.replicatedIndex(indexName)
	if pr != nil {
		return nil, pr.FirstError()
	}
	return index.IncomingFindUUIDs(ctx, shardName, f, limit)
}

func (db *DB) CreateAsyncCheckpoint(ctx context.Context, className string, shardNames []string, cutoffMs int64, createdAt time.Time) error {
	index, pr := db.replicatedIndex(className)
	if pr != nil {
		return pr.FirstError()
	}
	return index.createAsyncCheckpointShards(ctx, index.resolveShardNames(shardNames), cutoffMs, createdAt)
}

func (db *DB) DeleteAsyncCheckpoint(ctx context.Context, className string, shardNames []string) error {
	index, pr := db.replicatedIndex(className)
	if pr != nil {
		return pr.FirstError()
	}
	return index.deleteAsyncCheckpointShards(ctx, index.resolveShardNames(shardNames))
}

func (db *DB) GetAsyncCheckpointStatus(ctx context.Context, className string, shardNames []string) (map[string]replica.AsyncCheckpointShardStatus, error) {
	index, pr := db.replicatedIndex(className)
	if pr != nil {
		return nil, pr.FirstError()
	}
	targets := index.resolveShardNames(shardNames)
	return index.getAsyncCheckpointShardStatus(ctx, targets)
}

func (db *DB) CreateAsyncCheckpoints(ctx context.Context, className string, cutoffMs int64, shards []string) error {
	index, pr := db.replicatedIndex(className)
	if pr != nil {
		return pr.FirstError()
	}
	return index.CreateAsyncCheckpoints(ctx, cutoffMs, shards)
}

func (db *DB) DeleteAsyncCheckpoints(ctx context.Context, className string, shards []string) error {
	index, pr := db.replicatedIndex(className)
	if pr != nil {
		return pr.FirstError()
	}
	return index.DeleteAsyncCheckpoints(ctx, shards)
}

func (db *DB) GetAsyncCheckpointNodeStatuses(ctx context.Context, className string, shards []string) (map[string][]replica.AsyncCheckpointNodeStatus, error) {
	index, pr := db.replicatedIndex(className)
	if pr != nil {
		return nil, pr.FirstError()
	}
	return index.GetAsyncCheckpointStatus(ctx, shards)
}

func (db *DB) CommitReplication(ctx context.Context,
	class, shard, requestID string,
) any {
	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.CommitReplication(ctx, shard, requestID)
}

func (db *DB) AbortReplication(ctx context.Context,
	class, shard, requestID string,
) any {
	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.AbortReplication(ctx, shard, requestID)
}

func (db *DB) replicatedIndex(name string) (idx *Index, resp *replica.SimpleResponse) {
	if !db.StartupComplete() {
		return nil, &replica.SimpleResponse{Errors: []replicaerrors.Error{
			*replicaerrors.NewError(replicaerrors.StatusNotReady, name),
		}}
	}

	if idx = db.GetIndex(schema.ClassName(name)); idx == nil {
		return nil, &replica.SimpleResponse{Errors: []replicaerrors.Error{
			*replicaerrors.NewError(replicaerrors.StatusClassNotFound, name),
		}}
	}
	return idx, resp
}

func (db *DB) waitForSchemaVersionForIndexWrite(ctx context.Context, schemaVersion uint64) *replica.SimpleResponse {
	if err := db.schemaReader.WaitForUpdate(ctx, schemaVersion); err != nil {
		// Msg carries the human-readable detail because Err is not
		// serialised over the wire (json:"-"); without Msg the remote
		// coordinator would see an empty error and treat it as success.
		return &replica.SimpleResponse{Errors: []replicaerrors.Error{{
			Code: replicaerrors.StatusPreconditionFailed,
			Msg:  fmt.Sprintf("waiting for schema version %d: %v", schemaVersion, err),
			Err:  err,
		}}}
	}
	return nil
}

func (i *Index) writableShard(ctx context.Context, name string) (ShardLike, func(), *replica.SimpleResponse) {
	localShard, release, err := i.getOrInitShard(ctx, name)
	if err != nil {
		return nil, func() {}, &replica.SimpleResponse{Errors: []replicaerrors.Error{
			{Code: replicaerrors.StatusShardNotFound, Msg: fmt.Sprintf("error getting or initializing shard %q: %v", name, err), Err: err},
		}}
	}
	if localShard.isReadOnly() != nil {
		release()

		return nil, func() {}, &replica.SimpleResponse{Errors: []replicaerrors.Error{{
			Code: replicaerrors.StatusReadOnly, Msg: name,
		}}}
	}
	return localShard, release, nil
}

func (i *Index) ReplicateObject(ctx context.Context, shard, requestID string, object *storobj.Object, _ uint64) replica.SimpleResponse {
	localShard, release, pr := i.writableShard(ctx, shard)
	if pr != nil {
		return *pr
	}

	defer release()

	return localShard.preparePutObject(ctx, requestID, object)
}

func (i *Index) ReplicateUpdate(ctx context.Context, shard, requestID string, doc *objects.MergeDocument, _ uint64) replica.SimpleResponse {
	localShard, release, pr := i.writableShard(ctx, shard)
	if pr != nil {
		return *pr
	}

	defer release()

	return localShard.prepareMergeObject(ctx, requestID, doc)
}

func (i *Index) ReplicateDeletion(ctx context.Context, shard, requestID string, uuid strfmt.UUID, deletionTime time.Time, _ uint64) replica.SimpleResponse {
	localShard, release, pr := i.writableShard(ctx, shard)
	if pr != nil {
		return *pr
	}

	defer release()

	return localShard.prepareDeleteObject(ctx, requestID, uuid, deletionTime)
}

func (i *Index) ReplicateObjects(ctx context.Context, shard, requestID string, objects []*storobj.Object, _ uint64) replica.SimpleResponse {
	localShard, release, pr := i.writableShard(ctx, shard)
	if pr != nil {
		return *pr
	}

	defer release()

	return localShard.preparePutObjects(ctx, requestID, objects)
}

func (i *Index) ReplicateDeletions(ctx context.Context, shard, requestID string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, _ uint64,
) replica.SimpleResponse {
	localShard, release, pr := i.writableShard(ctx, shard)
	if pr != nil {
		return *pr
	}

	defer release()

	return localShard.prepareDeleteObjects(ctx, requestID, uuids, deletionTime, dryRun)
}

func (i *Index) ReplicateReferences(ctx context.Context, shard, requestID string, refs []objects.BatchReference, _ uint64) replica.SimpleResponse {
	localShard, release, pr := i.writableShard(ctx, shard)
	if pr != nil {
		return *pr
	}

	defer release()

	return localShard.prepareAddReferences(ctx, requestID, refs)
}

func (i *Index) CommitReplication(ctx context.Context, shard, requestID string) any {
	localShard, release, err := i.GetShard(ctx, shard)
	if err != nil {
		return replica.SimpleResponse{Errors: []replicaerrors.Error{
			{Code: replicaerrors.StatusShardNotFound, Msg: fmt.Sprintf("error getting shard %q: %v", shard, err), Err: err},
		}}
	}
	defer release()

	if localShard == nil {
		return replica.SimpleResponse{Errors: []replicaerrors.Error{
			{Code: replicaerrors.StatusShardNotFound, Msg: shard, Err: fmt.Errorf("shard %q does not exist locally", shard)},
		}}
	}

	i.backupLock.RLock(shard)
	defer i.backupLock.RUnlock(shard)

	return localShard.commitReplication(ctx, requestID)
}

func (i *Index) AbortReplication(ctx context.Context, shard, requestID string) any {
	localShard, release, err := i.GetShard(ctx, shard)
	if err != nil {
		return replica.SimpleResponse{Errors: []replicaerrors.Error{
			{Code: replicaerrors.StatusShardNotFound, Msg: fmt.Sprintf("error getting shard %q: %v", shard, err), Err: err},
		}}
	}
	defer release()

	if localShard == nil {
		return replica.SimpleResponse{Errors: []replicaerrors.Error{
			{Code: replicaerrors.StatusShardNotFound, Msg: shard, Err: fmt.Errorf("shard %q does not exist locally", shard)},
		}}
	}

	return localShard.abortReplication(ctx, requestID)
}

func (i *Index) IncomingFilePutter(ctx context.Context, shardName,
	filePath string,
) (io.WriteCloser, error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("incoming file putter get shard %s err: %w", shardName, err)
	}
	defer release()

	if shard == nil {
		return nil, fmt.Errorf("incoming file putter get shard %s: shard not found", shardName)
	}

	return shard.filePutter(ctx, filePath)
}

func (i *Index) IncomingCreateShard(ctx context.Context, className string, shardName string) error {
	if err := i.initLocalShard(ctx, shardName); err != nil {
		return fmt.Errorf("incoming create shard: %w", err)
	}
	return nil
}

func (i *Index) IncomingReinitShard(ctx context.Context, shardName string) error {
	err := func() error {
		i.closeLock.Lock()
		defer i.closeLock.Unlock()

		if i.closed {
			return errAlreadyShutdown
		}

		i.shardCreateLocks.Lock(shardName)
		defer i.shardCreateLocks.Unlock(shardName)

		shard, ok := i.shards.LoadAndDelete(shardName)
		if ok {
			if err := shard.Shutdown(ctx); err != nil {
				if !errors.Is(err, errAlreadyShutdown) {
					return err
				}
			}
		}

		return nil
	}()
	if err != nil {
		return err
	}

	return i.initLocalShard(ctx, shardName)
}

func (i *Index) IncomingStartChangeCapture(ctx context.Context, shardName, opID string) error {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return fmt.Errorf("incoming start change capture: get shard %q: %w", shardName, err)
	}
	defer release()
	if shard == nil {
		return fmt.Errorf("incoming start change capture: shard %q not found", shardName)
	}
	if _, err := shard.ActivateChangeLog(ctx, opID); err != nil {
		return fmt.Errorf("incoming start change capture: activate op %q: %w", opID, err)
	}
	return nil
}

// IncomingGetChangeLog returns a tailer over the shard's active log. Caller
// owns Close; the tailer has its own file handle and outlives the shard pin.
// untilLSN is the inclusive upper bound on emitted LSNs.
func (i *Index) IncomingGetChangeLog(ctx context.Context, shardName, opID string, untilLSN uint64) (*changelog.Tailer, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("incoming get change log: get shard %q: %w", shardName, err)
	}
	defer release()
	if shard == nil {
		return nil, fmt.Errorf("incoming get change log: shard %q not found", shardName)
	}
	log, ok := shard.GetChangeLog(ctx, opID)
	if !ok {
		return nil, fmt.Errorf("incoming get change log: %s %q on shard %q", changelog.ErrMsgNoActiveLog, opID, shardName)
	}
	return log.NewTailerWithCap(0, untilLSN)
}

// IncomingSnapshotChangeLogLSN returns the current LSN without sealing the log.
func (i *Index) IncomingSnapshotChangeLogLSN(ctx context.Context, shardName, opID string) (uint64, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return 0, fmt.Errorf("incoming snapshot change-log LSN: get shard %q: %w", shardName, err)
	}
	defer release()
	if shard == nil {
		return 0, fmt.Errorf("incoming snapshot change-log LSN: shard %q not found", shardName)
	}
	lsn, err := shard.SnapshotChangeLogLSN(ctx, opID)
	if err != nil {
		return 0, fmt.Errorf("incoming snapshot change-log LSN: op %q: %w", opID, err)
	}
	return lsn, nil
}

func (i *Index) IncomingFinalizeChangeLog(ctx context.Context, shardName, opID string) (uint64, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return 0, fmt.Errorf("incoming finalize change log: get shard %q: %w", shardName, err)
	}
	defer release()
	if shard == nil {
		return 0, fmt.Errorf("incoming finalize change log: shard %q not found", shardName)
	}
	finalLSN, err := shard.FinalizeChangeLog(ctx, opID)
	if err != nil {
		return 0, fmt.Errorf("incoming finalize change log: op %q: %w", opID, err)
	}
	return finalLSN, nil
}

func (i *Index) IncomingStopChangeCapture(ctx context.Context, shardName, opID string) error {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return fmt.Errorf("incoming stop change capture: get shard %q: %w", shardName, err)
	}
	defer release()
	if shard == nil {
		return fmt.Errorf("incoming stop change capture: shard %q not found", shardName)
	}
	if err := shard.StopChangeCapture(ctx, opID); err != nil {
		return fmt.Errorf("incoming stop change capture: op %q: %w", opID, err)
	}
	return nil
}

// IncomingAddAsyncReplicationTargetNode adds the given target node override for async replication.
// If the target node override already exists with a different upper time bound, the existing
// override will use the maximum upper time bound between the two. Async replication will be
// started if it's not already running.
func (i *Index) IncomingAddAsyncReplicationTargetNode(
	ctx context.Context,
	shardName string,
	targetNodeOverride additional.AsyncReplicationTargetNodeOverride,
) error {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return fmt.Errorf("incoming add async replication get shard %s err: %w", shardName, err)
	}
	defer release()
	if shard == nil {
		return fmt.Errorf("incoming add async replication get shard %s: shard not found", shardName)
	}

	return shard.addTargetNodeOverride(ctx, targetNodeOverride)
}

// IncomingRemoveAsyncReplicationTargetNode removes the given target node override for async
// replication. The removal is a no-op if the target node override does not exist
// or if the upper time bound of the given target node override is less than the existing
// override's upper time bound. If there are no target node overrides left, async replication
// will be reset to it's default configuration.
func (i *Index) IncomingRemoveAsyncReplicationTargetNode(ctx context.Context,
	shardName string,
	targetNodeOverride additional.AsyncReplicationTargetNodeOverride,
) error {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return fmt.Errorf("incoming remove async replication get shard %s err: %w", shardName, err)
	}
	defer release()
	if shard == nil {
		return fmt.Errorf("incoming remove async replication get shard %s: shard not found", shardName)
	}

	return shard.removeTargetNodeOverride(ctx, targetNodeOverride)
}

// IncomingAllRemoveAsyncReplicationTargetNodes removes all target node overrides for async
// replication. Async replication will be reset to it's default configuration.
func (i *Index) IncomingRemoveAllAsyncReplicationTargetNodes(ctx context.Context,
	shardName string,
) error {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return fmt.Errorf("incoming remove all async replication get shard %s err: %w", shardName, err)
	}
	defer release()
	if shard == nil {
		return fmt.Errorf("incoming remove all async replication get shard %s: shard not found", shardName)
	}

	return shard.removeAllTargetNodeOverrides(ctx)
}

func (s *Shard) filePutter(ctx context.Context,
	filePath string,
) (io.WriteCloser, error) {
	// TODO: validate file prefix to rule out that we're accidentally writing
	// into another shard
	finalPath := filepath.Join(s.Index().Config.RootPath, filePath)
	dir := path.Dir(finalPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("create parent folder for %s: %w", filePath, err)
	}

	f, err := os.Create(finalPath)
	if err != nil {
		return nil, fmt.Errorf("open file %q for writing: %w", filePath, err)
	}

	return f, nil
}

// OverwriteObjects if their state didn't change in the meantime
// It returns nil if all object have been successfully overwritten
// and otherwise a list of failed operations.
func (idx *Index) OverwriteObjects(ctx context.Context,
	shard string, updates []*objects.VObject,
) ([]types.RepairResponse, error) {
	s, release, err := idx.getOrInitShard(ctx, shard)
	if err != nil {
		return nil, fmt.Errorf("shard %q not found locally", shard)
	}

	defer release()
	if s == nil {
		return nil, fmt.Errorf("shard %q not found locally", shard)
	}

	if s.GetStatus() == storagestate.StatusLoading && idx.replicationEnabled() {
		return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shard))
	}

	var result []types.RepairResponse

	updateBatch := make([]*storobj.Object, 0, len(updates))

	for i, u := range updates {
		incomingObj := u.LatestObject
		lastUpdateTime := u.LastUpdateTimeUnixMilli

		// raw path: decode once for indexing, persist the bytes verbatim on write.
		// FromBinaryDisk (canonical class) because the on-disk class-name may be
		// empty, which FromBinaryNetwork rejects.
		var rawObj *storobj.Object
		if u.RawBytes != nil {
			decoded, decErr := storobj.FromBinaryDisk(u.RawBytes, idx.Config.ClassName.String())
			if decErr != nil {
				result = append(result, types.RepairResponse{
					Err: fmt.Sprintf("decode raw object at position %d: %v", i, decErr),
				})
				continue
			}
			decoded.PrecomputedDiskBinary = u.RawBytes
			rawObj = decoded
			lastUpdateTime = rawObj.LastUpdateTimeUnix()
		}

		if (u.Deleted && u.ID == "") ||
			(!u.Deleted && rawObj == nil && (incomingObj == nil || incomingObj.ID == "")) {
			msg := fmt.Sprintf("received nil object or empty uuid at position %d", i)
			result = append(result, types.RepairResponse{Err: msg})
			continue
		}

		var id strfmt.UUID
		switch {
		case u.Deleted:
			id = u.ID
		case rawObj != nil:
			id = rawObj.ID()
		default:
			id = incomingObj.ID
		}

		var currUpdateTime int64 // 0 means object doesn't exist on this node
		var locallyDeleted bool

		localObj, err := s.ObjectDigestErrDeleted(ctx, id)
		if err == nil {
			currUpdateTime = localObj.UpdateTime
		} else if errors.Is(err, lsmkv.Deleted) {
			locallyDeleted = true
			var errDeleted lsmkv.ErrDeleted
			if errors.As(err, &errDeleted) {
				currUpdateTime = errDeleted.DeletionTime().UnixMilli()
			} // otherwise an unknown deletion time
		} else if !errors.Is(err, lsmkv.NotFound) {
			result = append(result, types.RepairResponse{
				ID:  id.String(),
				Err: err.Error(),
			})
			continue
		}

		if currUpdateTime != u.StaleUpdateTime {

			if currUpdateTime == lastUpdateTime {
				// local object was updated in the mean time, no need to do anything
				continue
			}

			// a conflict is returned except for a particular situation
			// that can be locally solved at this point:
			// the node propagating the object change may have no information about
			// the object from this node because it was deleted, it means that
			// if a time-based resolution is used and the update was more recent
			// than the deletion, the object update can be proccessed despite
			// the fact `currUpdateTime == u.StaleUpdateTime` does not hold.
			if !locallyDeleted ||
				idx.DeletionStrategy() != models.ReplicationConfigDeletionStrategyTimeBasedResolution ||
				currUpdateTime > lastUpdateTime {
				// object changed and its state differs from recent known state
				r := types.RepairResponse{
					ID:         id.String(),
					Deleted:    locallyDeleted,
					UpdateTime: currUpdateTime,
					Err:        "conflict",
				}

				result = append(result, r)
				continue
			}
			// the object is locally deleted, the resolution strategy is time-based and
			// the deletion was not made after the received update
		}

		// another validation is needed for backward-compatibility reasons:
		// objects may have been deleted without a deletionTime, it means
		// if an object is locally deleted currUpdateTime == 0
		// so to avoid creating/updating the locally deleted object
		// time-based strategy and a more recent creation/update is required
		if !u.Deleted && locallyDeleted &&
			(idx.DeletionStrategy() != models.ReplicationConfigDeletionStrategyTimeBasedResolution ||
				currUpdateTime > lastUpdateTime) {
			r := types.RepairResponse{
				ID:         id.String(),
				Deleted:    locallyDeleted,
				UpdateTime: currUpdateTime,
				Err:        "conflict",
			}

			result = append(result, r)
			continue
		}

		if u.Deleted {
			err := s.DeleteObject(ctx, u.ID, time.UnixMilli(u.LastUpdateTimeUnixMilli))
			if err != nil {
				r := types.RepairResponse{
					ID:  u.ID.String(),
					Err: fmt.Sprintf("overwrite deleted object: %v", err),
				}
				result = append(result, r)
			}
			continue
		}

		if rawObj != nil {
			updateBatch = append(updateBatch, rawObj)
		} else {
			updateBatch = append(updateBatch, storobj.FromObject(incomingObj, u.Vector, u.Vectors, u.MultiVectors))
		}
	}

	if len(updateBatch) > 0 {
		errs := s.PutObjectBatch(ctx, updateBatch)
		if len(errs) != 0 {
			for i := range errs {
				id := updateBatch[i].ID()
				err := errs[i]
				if err != nil {
					r := types.RepairResponse{
						ID:  id.String(),
						Err: fmt.Sprintf("overwrite stale object: %v", err),
					}
					result = append(result, r)
				}
			}
		}
	}

	return result, nil
}

func (i *Index) IncomingOverwriteObjects(ctx context.Context,
	shardName string, vobjects []*objects.VObject,
) ([]types.RepairResponse, error) {
	return i.OverwriteObjects(ctx, shardName, vobjects)
}

// ChangeLogReplayEntry is the decoded form of a single changelog frame for
// target-side replay.
type ChangeLogReplayEntry struct {
	ID                      strfmt.UUID
	LastUpdateTimeUnixMilli int64
	IsDelete                bool
	// Payload is the raw storobj.Object bytes for PUTs; empty for deletes.
	Payload []byte
}

// OverwriteObjectsFromChangeLog replays entries under pure LWW by
// LastUpdateTimeUnixMilli — no StaleUpdateTime conflicts and no
// DeletionStrategy, unlike OverwriteObjects. Entries MUST be in LSN order;
// contiguous PUTs coalesce into one PutObjectBatch, and a DELETE flushes the
// buffer first so PUT-then-DELETE for the same UUID never reorders.
func (idx *Index) OverwriteObjectsFromChangeLog(
	ctx context.Context, shard string, updates []ChangeLogReplayEntry,
) error {
	if len(updates) == 0 {
		return nil
	}

	s, release, err := idx.getOrInitShard(ctx, shard)
	if err != nil {
		return fmt.Errorf("shard %q not found locally: %w", shard, err)
	}
	defer release()
	if s == nil {
		return fmt.Errorf("shard %q not found locally", shard)
	}

	type pendingPut struct {
		decoded *storobj.Object
		ts      int64
	}
	pending := map[strfmt.UUID]pendingPut{}

	flushPending := func() error {
		if len(pending) == 0 {
			return nil
		}
		objs := make([]*storobj.Object, 0, len(pending))
		for _, p := range pending {
			objs = append(objs, p.decoded)
		}
		errs := s.PutObjectBatch(ctx, objs)
		for _, e := range errs {
			if e != nil {
				return fmt.Errorf("replay put batch: %w", e)
			}
		}
		clear(pending)
		return nil
	}

	for i := range updates {
		if err := ctx.Err(); err != nil {
			return err
		}
		u := &updates[i]

		var currUpdateTime int64
		localObj, err := s.ObjectDigestErrDeleted(ctx, u.ID)
		switch {
		case err == nil:
			currUpdateTime = localObj.UpdateTime
		case errors.Is(err, lsmkv.Deleted):
			var errDeleted lsmkv.ErrDeleted
			if errors.As(err, &errDeleted) {
				currUpdateTime = errDeleted.DeletionTime().UnixMilli()
			}
		case errors.Is(err, lsmkv.NotFound):
		default:
			return fmt.Errorf("read local digest for %s: %w", u.ID, err)
		}

		if currUpdateTime > u.LastUpdateTimeUnixMilli {
			continue
		}

		if u.IsDelete {
			if err := flushPending(); err != nil {
				return err
			}
			if err := s.DeleteObject(ctx, u.ID, time.UnixMilli(u.LastUpdateTimeUnixMilli)); err != nil {
				return fmt.Errorf("replay delete for %s: %w", u.ID, err)
			}
			continue
		}

		decoded, err := storobj.FromBinaryNetwork(u.Payload)
		if err != nil {
			// Flush first so entries before the failure are durable on retry.
			if flushErr := flushPending(); flushErr != nil {
				return flushErr
			}
			return fmt.Errorf("replay decode payload for %s: %w", u.ID, err)
		}
		// Dedupe by max LastUpdateTimeUnixMilli rather than relying on
		// PutObjectBatch's own dedupe — findDuplicatesInBatchObjects keeps the
		// LAST occurrence by index, not the highest timestamp, so a
		// clock-skewed source whose LSN order disagrees with timestamp order
		// would otherwise let an older PUT win over a newer one.
		if existing, ok := pending[u.ID]; ok && existing.ts >= u.LastUpdateTimeUnixMilli {
			continue
		}
		pending[u.ID] = pendingPut{decoded: decoded, ts: u.LastUpdateTimeUnixMilli}
	}

	return flushPending()
}

func (i *Index) DigestObjects(ctx context.Context,
	shardName string, ids []strfmt.UUID,
) (result []types.RepairResponse, err error) {
	s, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("shard %q not found locally", shardName)
	}
	defer release()

	if s == nil {
		return nil, fmt.Errorf("shard %q not found locally", shardName)
	}

	if s.GetStatus() == storagestate.StatusLoading && i.replicationEnabled() {
		return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	multiIDs := make([]multi.Identifier, len(ids))
	for j := range multiIDs {
		multiIDs[j] = multi.Identifier{ID: ids[j].String()}
	}

	objs, err := s.ObjectDigests(ctx, multiIDs)
	if err != nil {
		return nil, fmt.Errorf("shard objects digest: %w", err)
	}

	for j := range objs {
		if objs[j].ID != "" {
			continue
		}

		deleted, deletionTime, err := s.WasDeleted(ctx, ids[j])
		if err != nil {
			return nil, err
		}

		var updateTime int64
		if deleted && !deletionTime.IsZero() {
			updateTime = deletionTime.UnixMilli()
		}

		objs[j] = types.RepairResponse{
			ID:         ids[j].String(),
			Deleted:    deleted,
			UpdateTime: updateTime,
			// TODO: use version when supported
			Version: 0,
		}
	}

	return objs, nil
}

func (i *Index) IncomingDigestObjects(ctx context.Context,
	shardName string, ids []strfmt.UUID,
) (result []types.RepairResponse, err error) {
	return i.DigestObjects(ctx, shardName, ids)
}

func (i *Index) DigestObjectsInRange(ctx context.Context,
	shardName string, initialUUID, finalUUID strfmt.UUID, limit int,
) (result []types.RepairResponse, err error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("shard %q does not exist locally", shardName)
	}
	defer release()
	if shard == nil {
		return nil, nil
	}

	return shard.ObjectDigestsInRange(ctx, initialUUID, finalUUID, limit)
}

func (i *Index) IncomingDigestObjectsInRange(ctx context.Context,
	shardName string, initialUUID, finalUUID strfmt.UUID, limit int,
) (result []types.RepairResponse, err error) {
	return i.DigestObjectsInRange(ctx, shardName, initialUUID, finalUUID, limit)
}

func (i *Index) CompareDigests(ctx context.Context,
	shardName string, sourceDigests []types.RepairResponse,
) ([]types.RepairResponse, error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("%w: shard %q", err, shardName)
	}
	defer release()
	if shard == nil {
		return nil, fmt.Errorf("shard %q is not yet initialized on this node", shardName)
	}

	return shard.CompareDigests(ctx, sourceDigests)
}

func (i *Index) HashTreeLevel(ctx context.Context,
	shardName string, level int, discriminant *hashtree.Bitset,
) (digests []hashtree.Digest, err error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("%w: shard %q", err, shardName)
	}
	defer release()
	if shard == nil {
		return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	return shard.HashTreeLevel(ctx, level, discriminant)
}

func (i *Index) IncomingHashTreeLevel(ctx context.Context,
	shardName string, level int, discriminant *hashtree.Bitset,
) (digests []hashtree.Digest, err error) {
	return i.HashTreeLevel(ctx, shardName, level, discriminant)
}

func (i *Index) CountObjects(ctx context.Context, shardName string) (int, error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return 0, fmt.Errorf("%w: shard %q", err, shardName)
	}
	defer release()
	if shard == nil {
		return 0, fmt.Errorf("shard %q does not exist locally", shardName)
	}

	return shard.ObjectCount(ctx)
}

func (i *Index) IncomingCountObjects(ctx context.Context, shardName string) (int, error) {
	return i.CountObjects(ctx, shardName)
}

func (i *Index) FetchObject(ctx context.Context,
	shardName string, id strfmt.UUID,
) (replica.Replica, error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return replica.Replica{}, fmt.Errorf("shard %q does not exist locally", shardName)
	}

	defer release()

	if shard == nil {
		return replica.Replica{}, fmt.Errorf("shard %q does not exist locally", shardName)
	}

	if shard.GetStatus() == storagestate.StatusLoading && i.replicationEnabled() {
		return replica.Replica{}, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	obj, err := shard.ObjectByID(ctx, id, nil, additional.Properties{})
	if err != nil {
		return replica.Replica{}, fmt.Errorf("shard %q read repair get object: %w", shard.ID(), err)
	}

	if obj == nil {
		deleted, deletionTime, err := shard.WasDeleted(ctx, id)
		if err != nil {
			return replica.Replica{}, err
		}

		var updateTime int64
		if !deletionTime.IsZero() {
			updateTime = deletionTime.UnixMilli()
		}

		return replica.Replica{
			ID:                      id,
			Deleted:                 deleted,
			LastUpdateTimeUnixMilli: updateTime,
		}, nil
	}

	return replica.Replica{
		Object:                  obj,
		ID:                      obj.ID(),
		LastUpdateTimeUnixMilli: obj.LastUpdateTimeUnix(),
	}, nil
}

func (i *Index) FetchObjects(ctx context.Context,
	shardName string, ids []strfmt.UUID,
) ([]replica.Replica, error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("shard %q does not exist locally", shardName)
	}
	defer release()
	if shard == nil {
		return nil, fmt.Errorf("shard %q does not exist locally", shardName)
	}

	if shard.GetStatus() == storagestate.StatusLoading {
		return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	objs, err := shard.MultiObjectByID(ctx, wrapIDsInMulti(ids))
	if err != nil {
		return nil, fmt.Errorf("shard %q replication multi get objects: %w", shard.ID(), err)
	}

	resp := make([]replica.Replica, len(ids))

	for j, obj := range objs {
		if obj == nil {
			deleted, deletionTime, err := shard.WasDeleted(ctx, ids[j])
			if err != nil {
				return nil, err
			}

			var updateTime int64
			if !deletionTime.IsZero() {
				updateTime = deletionTime.UnixMilli()
			}

			resp[j] = replica.Replica{
				ID:                      ids[j],
				Deleted:                 deleted,
				LastUpdateTimeUnixMilli: updateTime,
			}
		} else {
			resp[j] = replica.Replica{
				Object:                  obj,
				ID:                      obj.ID(),
				LastUpdateTimeUnixMilli: obj.LastUpdateTimeUnix(),
			}
		}
	}

	return resp, nil
}
