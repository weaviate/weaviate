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

package db

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

type Replicator interface {
	ReplicateObject(ctx context.Context, shardName, requestID string,
		object *storobj.Object) replica.SimpleResponse
	ReplicateObjects(ctx context.Context, shardName, requestID string,
		objects []*storobj.Object) replica.SimpleResponse
	ReplicateUpdate(ctx context.Context, shard, requestID string,
		doc *objects.MergeDocument) replica.SimpleResponse
	ReplicateDeletion(ctx context.Context, shardName, requestID string,
		uuid strfmt.UUID, deletionTime time.Time) replica.SimpleResponse
	ReplicateDeletions(ctx context.Context, shardName, requestID string,
		uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64) replica.SimpleResponse
	ReplicateReferences(ctx context.Context, shard, requestID string,
		refs []objects.BatchReference) replica.SimpleResponse
	CommitReplication(shard,
		requestID string) interface{}
	AbortReplication(shardName,
		requestID string) interface{}
}

const tmpCopyExtension = ".copy.tmp" // indexcount and proplen temporary copy

func (db *DB) ReplicateObject(ctx context.Context, class,
	shard, requestID string, object *storobj.Object,
) replica.SimpleResponse {
	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateObject(ctx, shard, requestID, object)
}

func (db *DB) ReplicateObjects(ctx context.Context, class,
	shard, requestID string, objects []*storobj.Object, schemaVersion uint64,
) replica.SimpleResponse {
	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateObjects(ctx, shard, requestID, objects, schemaVersion)
}

func (db *DB) ReplicateUpdate(ctx context.Context, class,
	shard, requestID string, mergeDoc *objects.MergeDocument,
) replica.SimpleResponse {
	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateUpdate(ctx, shard, requestID, mergeDoc)
}

func (db *DB) ReplicateDeletion(ctx context.Context, class,
	shard, requestID string, uuid strfmt.UUID, deletionTime time.Time,
) replica.SimpleResponse {
	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateDeletion(ctx, shard, requestID, uuid, deletionTime)
}

func (db *DB) ReplicateDeletions(ctx context.Context, class,
	shard, requestID string, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) replica.SimpleResponse {
	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateDeletions(ctx, shard, requestID, uuids, deletionTime, dryRun, schemaVersion)
}

func (db *DB) ReplicateReferences(ctx context.Context, class,
	shard, requestID string, refs []objects.BatchReference,
) replica.SimpleResponse {
	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateReferences(ctx, shard, requestID, refs)
}

func (db *DB) CommitReplication(class,
	shard, requestID string,
) interface{} {
	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.CommitReplication(shard, requestID)
}

func (db *DB) AbortReplication(class,
	shard, requestID string,
) interface{} {
	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.AbortReplication(shard, requestID)
}

func (db *DB) replicatedIndex(name string) (idx *Index, resp *replica.SimpleResponse) {
	if !db.StartupComplete() {
		return nil, &replica.SimpleResponse{Errors: []replica.Error{
			*replica.NewError(replica.StatusNotReady, name),
		}}
	}

	if idx = db.GetIndex(schema.ClassName(name)); idx == nil {
		return nil, &replica.SimpleResponse{Errors: []replica.Error{
			*replica.NewError(replica.StatusClassNotFound, name),
		}}
	}
	return
}

func (i *Index) writableShard(name string) (ShardLike, func(), *replica.SimpleResponse) {
	localShard, release, err := i.getOrInitShard(context.Background(), name)
	if err != nil {
		return nil, func() {}, &replica.SimpleResponse{Errors: []replica.Error{
			{Code: replica.StatusShardNotFound, Msg: name},
		}}
	}
	if localShard.isReadOnly() != nil {
		release()

		return nil, func() {}, &replica.SimpleResponse{Errors: []replica.Error{{
			Code: replica.StatusReadOnly, Msg: name,
		}}}
	}
	return localShard, release, nil
}

func (i *Index) ReplicateObject(ctx context.Context, shard, requestID string, object *storobj.Object) replica.SimpleResponse {
	localShard, release, pr := i.writableShard(shard)
	if pr != nil {
		return *pr
	}

	defer release()

	return localShard.preparePutObject(ctx, requestID, object)
}

func (i *Index) ReplicateUpdate(ctx context.Context, shard, requestID string, doc *objects.MergeDocument) replica.SimpleResponse {
	localShard, release, pr := i.writableShard(shard)
	if pr != nil {
		return *pr
	}

	defer release()

	return localShard.prepareMergeObject(ctx, requestID, doc)
}

func (i *Index) ReplicateDeletion(ctx context.Context, shard, requestID string, uuid strfmt.UUID, deletionTime time.Time) replica.SimpleResponse {
	localShard, release, pr := i.writableShard(shard)
	if pr != nil {
		return *pr
	}

	defer release()

	return localShard.prepareDeleteObject(ctx, requestID, uuid, deletionTime)
}

func (i *Index) ReplicateObjects(ctx context.Context, shard, requestID string, objects []*storobj.Object, schemaVersion uint64) replica.SimpleResponse {
	localShard, release, pr := i.writableShard(shard)
	if pr != nil {
		return *pr
	}

	defer release()

	return localShard.preparePutObjects(ctx, requestID, objects)
}

func (i *Index) ReplicateDeletions(ctx context.Context, shard, requestID string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) replica.SimpleResponse {
	localShard, release, pr := i.writableShard(shard)
	if pr != nil {
		return *pr
	}

	defer release()

	return localShard.prepareDeleteObjects(ctx, requestID, uuids, deletionTime, dryRun)
}

func (i *Index) ReplicateReferences(ctx context.Context, shard, requestID string, refs []objects.BatchReference) replica.SimpleResponse {
	localShard, release, pr := i.writableShard(shard)
	if pr != nil {
		return *pr
	}

	defer release()

	return localShard.prepareAddReferences(ctx, requestID, refs)
}

func (i *Index) CommitReplication(shard, requestID string) interface{} {
	localShard, release, err := i.getOrInitShard(context.Background(), shard)
	if err != nil {
		return replica.SimpleResponse{Errors: []replica.Error{
			{Code: replica.StatusShardNotFound, Msg: shard, Err: err},
		}}
	}

	defer release()

	return localShard.commitReplication(context.Background(), requestID, &i.shardTransferMutex)
}

func (i *Index) AbortReplication(shard, requestID string) interface{} {
	localShard, release, err := i.getOrInitShard(context.Background(), shard)
	if err != nil {
		return replica.SimpleResponse{Errors: []replica.Error{
			{Code: replica.StatusShardNotFound, Msg: shard, Err: err},
		}}
	}

	defer release()

	return localShard.abortReplication(context.Background(), requestID)
}

func (i *Index) IncomingFilePutter(ctx context.Context, shardName,
	filePath string,
) (io.WriteCloser, error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("incoming file putter get shard %s err: %w", shardName, err)
	}
	if shard == nil {
		return nil, fmt.Errorf("incoming file putter get shard %s: shard not found", shardName)
	}
	defer release()

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

// IncomingPauseFileActivity pauses the background processes of the specified shard.
// You should explicitly call resumeMaintenanceCycles to resume the background processes after you don't
// need the returned files to stay immutable anymore.
func (i *Index) IncomingPauseFileActivity(ctx context.Context,
	shardName string,
) error {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return fmt.Errorf("incoming pause file activity get shard %s err: %w", shardName, err)
	}
	if shard == nil {
		return fmt.Errorf("incoming pause file activity get shard %s: shard not found", shardName)
	}
	defer release()

	err = shard.HaltForTransfer(ctx, false, i.Config.TransferInactivityTimeout)
	if err != nil {
		return fmt.Errorf("shard %q could not be halted for transfer: %w", shardName, err)
	}

	return nil
}

// IncomingResumeFileActivity resumes the background processes of the specified shard.
func (i *Index) IncomingResumeFileActivity(ctx context.Context,
	shardName string,
) error {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return fmt.Errorf("incoming resume file activity get shard %s err: %w", shardName, err)
	}
	if shard == nil {
		return fmt.Errorf("incoming resume file activity get shard %s: shard not found", shardName)
	}
	defer release()

	err = shard.resumeMaintenanceCycles(ctx)
	if err != nil {
		return fmt.Errorf("shard %q could not be resumed after transfer: %w", shardName, err)
	}

	return nil
}

// IncomingListFiles returns a list of files that can be used to get the
// shard data at the time the pause was requested.
// You should explicitly call resumeMaintenanceCycles to resume the background processes after you don't
// need the returned files to stay immutable anymore.
func (i *Index) IncomingListFiles(ctx context.Context,
	shardName string,
) ([]string, error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("incoming list files get shard %s: %w", shardName, err)
	}
	if shard == nil {
		return nil, fmt.Errorf("incoming list files get shard is nil: %s", shardName)
	}
	defer release()

	sd := backup.ShardDescriptor{Name: shardName}

	// prevent writing into the index during collection of metadata
	i.shardTransferMutex.Lock()
	defer i.shardTransferMutex.Unlock()

	// flushing memtable before gathering the files to prevent the inclusion of a partially written file
	if err = shard.Store().FlushMemtables(ctx); err != nil {
		return nil, fmt.Errorf("flush memtables: %w", err)
	}

	if err := shard.ListBackupFiles(ctx, &sd); err != nil {
		return nil, fmt.Errorf("shard %q could not list backup files: %w", shardName, err)
	}

	err = i.tmpCopy(shard.Counter().FileName(), sd.DocIDCounter)
	if err != nil {
		return nil, err
	}

	err = i.tmpCopy(shard.GetPropertyLengthTracker().FileName(), sd.PropLengthTracker)
	if err != nil {
		return nil, err
	}

	files := []string{
		sd.DocIDCounterPath,
		sd.PropLengthTrackerPath,
		sd.ShardVersionPath,
	}
	files = append(files, sd.Files...)

	return files, nil
}

func (i *Index) tmpCopy(path string, b []byte) error {
	tmpFile, err := os.OpenFile(path+tmpCopyExtension, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	defer tmpFile.Close()

	_, err = io.Copy(tmpFile, bytes.NewBuffer(b))
	return err
}

// IncomingGetFileMetadata returns file metadata at the given path in the specified shards's root
// directory.
func (i *Index) IncomingGetFileMetadata(ctx context.Context, shardName, relativeFilePath string) (file.FileMetadata, error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return file.FileMetadata{}, fmt.Errorf("incoming get file metadata get shard %s err: %w", shardName, err)
	}
	if shard == nil {
		return file.FileMetadata{}, fmt.Errorf("incoming get file metadata get shard %s: shard not found", shardName)
	}
	defer release()

	if strings.HasSuffix(shard.Counter().FileName(), relativeFilePath) ||
		strings.HasSuffix(shard.GetPropertyLengthTracker().FileName(), relativeFilePath) {
		relativeFilePath = relativeFilePath + tmpCopyExtension
	}

	return shard.GetFileMetadata(ctx, relativeFilePath)
}

// IncomingGetFile returns a reader for the file at the given path in the specified shard's root
// directory. The caller must close the returned io.ReadCloser if no error is returned.
func (i *Index) IncomingGetFile(ctx context.Context, shardName,
	relativeFilePath string,
) (io.ReadCloser, error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("incoming get file get shard %s err: %w", shardName, err)
	}
	if shard == nil {
		return nil, fmt.Errorf("incoming get file get shard %s: shard not found", shardName)
	}
	defer release()

	if strings.HasSuffix(shard.Counter().FileName(), relativeFilePath) ||
		strings.HasSuffix(shard.GetPropertyLengthTracker().FileName(), relativeFilePath) {
		relativeFilePath = relativeFilePath + tmpCopyExtension
	}

	return shard.GetFile(ctx, relativeFilePath)
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
	if shard == nil {
		return fmt.Errorf("incoming add async replication get shard %s: shard not found", shardName)
	}
	defer release()

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
	if shard == nil {
		return fmt.Errorf("incoming remove async replication get shard %s: shard not found", shardName)
	}
	defer release()

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
	if shard == nil {
		return fmt.Errorf("incoming remove all async replication get shard %s: shard not found", shardName)
	}
	defer release()

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
	s, release, err := idx.GetShard(ctx, shard)
	if err != nil {
		return nil, fmt.Errorf("shard %q not found locally", shard)
	}
	if s == nil {
		return nil, fmt.Errorf("shard %q not found locally", shard)
	}
	defer release()

	var result []types.RepairResponse

	for i, u := range updates {
		incomingObj := u.LatestObject

		if (u.Deleted && u.ID == "") || (!u.Deleted && (incomingObj == nil || incomingObj.ID == "")) {
			msg := fmt.Sprintf("received nil object or empty uuid at position %d", i)
			result = append(result, types.RepairResponse{Err: msg})
			continue
		}

		var id strfmt.UUID
		if u.Deleted {
			id = u.ID
		} else {
			id = incomingObj.ID
		}

		var currUpdateTime int64 // 0 means object doesn't exist on this node
		var locallyDeleted bool

		localObj, err := s.ObjectByIDErrDeleted(ctx, id, nil, additional.Properties{})
		if err == nil {
			currUpdateTime = localObj.LastUpdateTimeUnix()
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

			if currUpdateTime == u.LastUpdateTimeUnixMilli {
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
				currUpdateTime > u.LastUpdateTimeUnixMilli {
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
				currUpdateTime > u.LastUpdateTimeUnixMilli) {
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

		err = s.PutObject(ctx, storobj.FromObject(incomingObj, u.Vector, u.Vectors, u.MultiVectors))
		if err != nil {
			r := types.RepairResponse{
				ID:  id.String(),
				Err: fmt.Sprintf("overwrite stale object: %v", err),
			}
			result = append(result, r)
			continue
		}
	}

	return result, nil
}

func (i *Index) IncomingOverwriteObjects(ctx context.Context,
	shardName string, vobjects []*objects.VObject,
) ([]types.RepairResponse, error) {
	return i.OverwriteObjects(ctx, shardName, vobjects)
}

func (i *Index) DigestObjects(ctx context.Context,
	shardName string, ids []strfmt.UUID,
) (result []types.RepairResponse, err error) {
	result = make([]types.RepairResponse, len(ids))

	s, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("shard %q not found locally", shardName)
	}

	defer release()

	if s.GetStatus() == storagestate.StatusLoading {
		return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	multiIDs := make([]multi.Identifier, len(ids))
	for j := range multiIDs {
		multiIDs[j] = multi.Identifier{ID: ids[j].String()}
	}

	objs, err := s.MultiObjectByID(ctx, multiIDs)
	if err != nil {
		return nil, fmt.Errorf("shard objects digest: %w", err)
	}

	for j := range objs {
		if objs[j] == nil {
			deleted, deletionTime, err := s.WasDeleted(ctx, ids[j])
			if err != nil {
				return nil, err
			}

			var updateTime int64
			if deleted && !deletionTime.IsZero() {
				updateTime = deletionTime.UnixMilli()
			}

			result[j] = types.RepairResponse{
				ID:         ids[j].String(),
				Deleted:    deleted,
				UpdateTime: updateTime,
				// TODO: use version when supported
				Version: 0,
			}
		} else {
			result[j] = types.RepairResponse{
				ID:         objs[j].ID().String(),
				UpdateTime: objs[j].LastUpdateTimeUnix(),
				// TODO: use version when supported
				Version: 0,
			}
		}
	}

	return
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
	if shard == nil {
		return nil, nil
	}

	defer release()

	return shard.ObjectDigestsInRange(ctx, initialUUID, finalUUID, limit)
}

func (i *Index) IncomingDigestObjectsInRange(ctx context.Context,
	shardName string, initialUUID, finalUUID strfmt.UUID, limit int,
) (result []types.RepairResponse, err error) {
	return i.DigestObjectsInRange(ctx, shardName, initialUUID, finalUUID, limit)
}

func (i *Index) HashTreeLevel(ctx context.Context,
	shardName string, level int, discriminant *hashtree.Bitset,
) (digests []hashtree.Digest, err error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("%w: shard %q", err, shardName)
	}
	if shard == nil {
		return nil, nil
	}

	defer release()

	return shard.HashTreeLevel(ctx, level, discriminant)
}

func (i *Index) IncomingHashTreeLevel(ctx context.Context,
	shardName string, level int, discriminant *hashtree.Bitset,
) (digests []hashtree.Digest, err error) {
	return i.HashTreeLevel(ctx, shardName, level, discriminant)
}

func (i *Index) FetchObject(ctx context.Context,
	shardName string, id strfmt.UUID,
) (replica.Replica, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return replica.Replica{}, fmt.Errorf("shard %q does not exist locally", shardName)
	}

	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
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
	if shard == nil {
		return nil, fmt.Errorf("shard %q does not exist locally", shardName)
	}
	defer release()

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
