//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/noop"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
)

type Replicator interface {
	ReplicateObject(ctx context.Context, shardName, requestID string,
		object *storobj.Object) replica.SimpleResponse
	ReplicateObjects(ctx context.Context, shardName, requestID string,
		objects []*storobj.Object) replica.SimpleResponse
	ReplicateUpdate(ctx context.Context, shard, requestID string,
		doc *objects.MergeDocument) replica.SimpleResponse
	ReplicateDeletion(ctx context.Context, shardName, requestID string,
		uuid strfmt.UUID) replica.SimpleResponse
	ReplicateDeletions(ctx context.Context, shardName, requestID string,
		docIDs []uint64, dryRun bool) replica.SimpleResponse
	ReplicateReferences(ctx context.Context, shard, requestID string,
		refs []objects.BatchReference) replica.SimpleResponse
	CommitReplication(shard,
		requestID string) interface{}
	AbortReplication(shardName,
		requestID string) interface{}
}

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
	shard, requestID string, objects []*storobj.Object,
) replica.SimpleResponse {
	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateObjects(ctx, shard, requestID, objects)
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
	shard, requestID string, uuid strfmt.UUID,
) replica.SimpleResponse {
	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateDeletion(ctx, shard, requestID, uuid)
}

func (db *DB) ReplicateDeletions(ctx context.Context, class,
	shard, requestID string, docIDs []uint64, dryRun bool,
) replica.SimpleResponse {
	index, pr := db.replicatedIndex(class)
	if pr != nil {
		return *pr
	}

	return index.ReplicateDeletions(ctx, shard, requestID, docIDs, dryRun)
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
		return nil
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

func (i *Index) writableShard(name string) (*Shard, *replica.SimpleResponse) {
	localShard, ok := i.Shards[name]
	if !ok {
		return nil, &replica.SimpleResponse{Errors: []replica.Error{
			{Code: replica.StatusShardNotFound, Msg: name},
		}}
	}
	if localShard.isReadOnly() {
		return nil, &replica.SimpleResponse{Errors: []replica.Error{{
			Code: replica.StatusReadOnly, Msg: name,
		}}}
	}
	return localShard, nil
}

func (i *Index) ReplicateObject(ctx context.Context, shard, requestID string, object *storobj.Object) replica.SimpleResponse {
	localShard, pr := i.writableShard(shard)
	if pr != nil {
		return *pr
	}
	return localShard.preparePutObject(ctx, requestID, object)
}

func (i *Index) ReplicateUpdate(ctx context.Context, shard, requestID string, doc *objects.MergeDocument) replica.SimpleResponse {
	localShard, pr := i.writableShard(shard)
	if pr != nil {
		return *pr
	}
	return localShard.prepareMergeObject(ctx, requestID, doc)
}

func (i *Index) ReplicateDeletion(ctx context.Context, shard, requestID string, uuid strfmt.UUID) replica.SimpleResponse {
	localShard, pr := i.writableShard(shard)
	if pr != nil {
		return *pr
	}
	return localShard.prepareDeleteObject(ctx, requestID, uuid)
}

func (i *Index) ReplicateObjects(ctx context.Context, shard, requestID string, objects []*storobj.Object) replica.SimpleResponse {
	localShard, pr := i.writableShard(shard)
	if pr != nil {
		return *pr
	}
	return localShard.preparePutObjects(ctx, requestID, objects)
}

func (i *Index) ReplicateDeletions(ctx context.Context, shard, requestID string, docIDs []uint64, dryRun bool) replica.SimpleResponse {
	localShard, pr := i.writableShard(shard)
	if pr != nil {
		return *pr
	}
	return localShard.prepareDeleteObjects(ctx, requestID, docIDs, dryRun)
}

func (i *Index) ReplicateReferences(ctx context.Context, shard, requestID string, refs []objects.BatchReference) replica.SimpleResponse {
	localShard, pr := i.writableShard(shard)
	if pr != nil {
		return *pr
	}
	return localShard.prepareAddReferences(ctx, requestID, refs)
}

func (i *Index) CommitReplication(shard, requestID string) interface{} {
	localShard, ok := i.Shards[shard]
	if !ok {
		return nil
	}
	return localShard.commit(context.Background(), requestID, &i.backupStateLock)
}

func (i *Index) AbortReplication(shard, requestID string) interface{} {
	localShard, ok := i.Shards[shard]
	if !ok {
		return replica.SimpleResponse{Errors: []replica.Error{
			{Code: replica.StatusShardNotFound, Msg: shard},
		}}
	}
	return localShard.abort(context.Background(), requestID)
}

func (i *Index) IncomingFilePutter(ctx context.Context, shardName,
	filePath string,
) (io.WriteCloser, error) {
	localShard, ok := i.Shards[shardName]
	if !ok {
		return nil, fmt.Errorf("shard %q does not exist locally", shardName)
	}

	return localShard.filePutter(ctx, filePath)
}

func (i *Index) IncomingCreateShard(ctx context.Context,
	shardName string,
) error {
	// TODO: locking???
	if _, ok := i.Shards[shardName]; ok {
		return fmt.Errorf("shard %q exists already", shardName)
	}

	// TODO: metrics
	s, err := NewShard(ctx, nil, shardName, i, nil)
	if err != nil {
		return err
	}

	// TODO: locking???
	i.Shards[shardName] = s

	return nil
}

func (i *Index) IncomingReinitShard(ctx context.Context,
	shardName string,
) error {
	shard, ok := i.Shards[shardName]
	if !ok {
		return fmt.Errorf("shard %q does not exist locally", shardName)
	}

	return shard.reinit(ctx)
}

func (s *Shard) filePutter(ctx context.Context,
	filePath string,
) (io.WriteCloser, error) {
	// TODO: validate file prefix to rule out that we're accidentally writing
	// into another shard
	finalPath := filepath.Join(s.index.Config.RootPath, filePath)
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

func (s *Shard) reinit(ctx context.Context) error {
	if err := s.shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown shard: %w", err)
	}

	hnswUserConfig, ok := s.index.vectorIndexUserConfig.(hnswent.UserConfig)
	if !ok {
		return fmt.Errorf("hnsw vector index: config is not hnsw.UserConfig: %T",
			s.index.vectorIndexUserConfig)
	}

	if hnswUserConfig.Skip {
		s.vectorIndex = noop.NewIndex()
	} else {
		if err := s.initVectorIndex(ctx, hnswUserConfig); err != nil {
			return fmt.Errorf("init vector index: %w", err)
		}

		defer s.vectorIndex.PostStartup()
	}

	if err := s.initNonVector(ctx, nil); err != nil {
		return fmt.Errorf("init non-vector: %w", err)
	}

	return nil
}

func (i *Index) OverwriteObjects(ctx context.Context,
	shard string, vobjects []*objects.VObject,
) ([]*objects.VObject, error) {
	result := make([]*objects.VObject, len(vobjects))
	shd := i.Shards[shard]
	if shd == nil {
		return nil, fmt.Errorf("shard %q not found locally", shard)
	}
	for j, vobj := range vobjects {
		found, err := shd.objectByID(ctx, vobj.LatestObject.ID, nil, additional.Properties{})
		if err != nil {
			return nil, err
		}
		// the stored object is not the most recent version. in
		// this case, we overwrite it with the more recent one.
		if found.LastUpdateTimeUnix() == vobj.StaleUpdateTime {
			err := shd.putObject(ctx, storobj.FromObject(vobj.LatestObject, vobj.LatestObject.Vector))
			if err != nil {
				return nil, fmt.Errorf("overwrite stale object: %w", err)
			}
			result[j] = vobj
		} else {
			result[j] = nil
		}
	}
	return result, nil
}

func (i *Index) IncomingOverwriteObjects(ctx context.Context,
	shard string, vobjects []*objects.VObject,
) ([]*objects.VObject, error) {
	return i.OverwriteObjects(ctx, shard, vobjects)
}
