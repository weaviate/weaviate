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

package sharding

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/objects"
)

type RemoteIndex struct {
	class        string
	stateGetter  shardingStateGetter
	client       RemoteIndexClient
	nodeResolver nodeResolver
}

type shardingStateGetter interface {
	// ShardOwner returns id of owner node
	ShardOwner(class, shard string) (string, error)
	ShardReplicas(class, shard string) ([]string, error)
}

func NewRemoteIndex(className string,
	stateGetter shardingStateGetter, nodeResolver nodeResolver,
	client RemoteIndexClient,
) *RemoteIndex {
	return &RemoteIndex{
		class:        className,
		stateGetter:  stateGetter,
		client:       client,
		nodeResolver: nodeResolver,
	}
}

type nodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
}

type RemoteIndexClient interface {
	PutObject(ctx context.Context, hostName, indexName, shardName string,
		obj *storobj.Object, schemaVersion uint64) error
	BatchPutObjects(ctx context.Context, hostName, indexName, shardName string,
		objs []*storobj.Object, repl *additional.ReplicationProperties, schemaVersion uint64) []error
	BatchAddReferences(ctx context.Context, hostName, indexName, shardName string,
		refs objects.BatchReferences, schemaVersion uint64) []error
	GetObject(ctx context.Context, hostname, indexName, shardName string,
		id strfmt.UUID, props search.SelectProperties,
		additional additional.Properties) (*storobj.Object, error)
	Exists(ctx context.Context, hostname, indexName, shardName string,
		id strfmt.UUID) (bool, error)
	DeleteObject(ctx context.Context, hostname, indexName, shardName string,
		id strfmt.UUID, deletionTime time.Time, schemaVersion uint64) error
	MergeObject(ctx context.Context, hostname, indexName, shardName string,
		mergeDoc objects.MergeDocument, schemaVersion uint64) error
	MultiGetObjects(ctx context.Context, hostname, indexName, shardName string,
		ids []strfmt.UUID) ([]*storobj.Object, error)
	SearchShard(ctx context.Context, hostname, indexName, shardName string,
		searchVector []models.Vector, targetVector []string, distance float32, limit int, filters *filters.LocalFilter,
		keywordRanking *searchparams.KeywordRanking, sort []filters.Sort,
		cursor *filters.Cursor, groupBy *searchparams.GroupBy,
		additional additional.Properties, targetCombination *dto.TargetCombination, properties []string,
	) ([]*storobj.Object, []float32, error)

	Aggregate(ctx context.Context, hostname, indexName, shardName string,
		params aggregation.Params) (*aggregation.Result, error)
	FindUUIDs(ctx context.Context, hostName, indexName, shardName string,
		filters *filters.LocalFilter) ([]strfmt.UUID, error)
	DeleteObjectBatch(ctx context.Context, hostName, indexName, shardName string,
		uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64) objects.BatchSimpleObjects
	GetShardQueueSize(ctx context.Context, hostName, indexName, shardName string) (int64, error)
	GetShardStatus(ctx context.Context, hostName, indexName, shardName string) (string, error)
	UpdateShardStatus(ctx context.Context, hostName, indexName, shardName, targetStatus string, schemaVersion uint64) error

	PutFile(ctx context.Context, hostName, indexName, shardName, fileName string,
		payload io.ReadSeekCloser) error

	// PauseFileActivity pauses the shard replica background processes on the specified node.
	// You should explicitly resume the background processes once you're done.
	PauseFileActivity(ctx context.Context, hostName, indexName, shardName string, schemaVersion uint64) error
	// ResumeFileActivity resumes the shard replica background processes on the specified node.
	ResumeFileActivity(ctx context.Context, hostName, indexName, shardName string) error
	// ListFiles returns a list of files that can be used to get the shard data at the time the pause was
	// requested.
	ListFiles(ctx context.Context, hostName, indexName, shardName string) ([]string, error)
	// GetFileMetadata returns file info at the given path in the shard's root directory.
	GetFileMetadata(ctx context.Context, hostName, indexName, shardName, fileName string) (file.FileMetadata, error)
	// GetFile returns a reader for the file at the given path in the shard's root directory.
	// The caller must close the returned io.ReadCloser if no error is returned.
	GetFile(ctx context.Context, hostName, indexName, shardName, fileName string) (io.ReadCloser, error)
	// AddAsyncReplicationTargetNode adds the async replication target node for a shard.
	AddAsyncReplicationTargetNode(ctx context.Context, hostName, indexName, shardName string, targetNodeOverride additional.AsyncReplicationTargetNodeOverride, schemaVersion uint64) error
	// RemoveAsyncReplicationTargetNode removes the async replication target node for a shard.
	RemoveAsyncReplicationTargetNode(ctx context.Context, hostName, indexName, shardName string, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error
}

func (ri *RemoteIndex) PutObject(ctx context.Context, shardName string,
	obj *storobj.Object, schemaVersion uint64,
) error {
	owner, err := ri.stateGetter.ShardOwner(ri.class, shardName)
	if err != nil {
		return fmt.Errorf("class %s has no physical shard %q: %w", ri.class, shardName, err)
	}

	host, ok := ri.nodeResolver.NodeHostname(owner)
	if !ok {
		return fmt.Errorf("resolve node name %q to host", owner)
	}

	return ri.client.PutObject(ctx, host, ri.class, shardName, obj, schemaVersion)
}

// helper for single errors that affect the entire batch, assign the error to
// every single item in the batch
func duplicateErr(in error, count int) []error {
	out := make([]error, count)
	for i := range out {
		out[i] = in
	}
	return out
}

func (ri *RemoteIndex) BatchPutObjects(ctx context.Context, shardName string,
	objs []*storobj.Object, schemaVersion uint64,
) []error {
	owner, err := ri.stateGetter.ShardOwner(ri.class, shardName)
	if err != nil {
		return duplicateErr(fmt.Errorf("class %s has no physical shard %q: %w",
			ri.class, shardName, err), len(objs))
	}

	host, ok := ri.nodeResolver.NodeHostname(owner)
	if !ok {
		return duplicateErr(fmt.Errorf("resolve node name %q to host",
			owner), len(objs))
	}

	return ri.client.BatchPutObjects(ctx, host, ri.class, shardName, objs, nil, schemaVersion)
}

func (ri *RemoteIndex) BatchAddReferences(ctx context.Context, shardName string,
	refs objects.BatchReferences, schemaVersion uint64,
) []error {
	owner, err := ri.stateGetter.ShardOwner(ri.class, shardName)
	if err != nil {
		return duplicateErr(fmt.Errorf("class %s has no physical shard %q: %w",
			ri.class, shardName, err), len(refs))
	}

	host, ok := ri.nodeResolver.NodeHostname(owner)
	if !ok {
		return duplicateErr(fmt.Errorf("resolve node name %q to host",
			owner), len(refs))
	}

	return ri.client.BatchAddReferences(ctx, host, ri.class, shardName, refs, schemaVersion)
}

func (ri *RemoteIndex) Exists(ctx context.Context, shardName string,
	id strfmt.UUID,
) (bool, error) {
	owner, err := ri.stateGetter.ShardOwner(ri.class, shardName)
	if err != nil {
		return false, fmt.Errorf("class %s has no physical shard %q: %w", ri.class, shardName, err)
	}

	host, ok := ri.nodeResolver.NodeHostname(owner)
	if !ok {
		return false, fmt.Errorf("resolve node name %q to host", owner)
	}

	return ri.client.Exists(ctx, host, ri.class, shardName, id)
}

func (ri *RemoteIndex) DeleteObject(ctx context.Context, shardName string,
	id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) error {
	owner, err := ri.stateGetter.ShardOwner(ri.class, shardName)
	if err != nil {
		return fmt.Errorf("class %s has no physical shard %q: %w", ri.class, shardName, err)
	}

	host, ok := ri.nodeResolver.NodeHostname(owner)
	if !ok {
		return fmt.Errorf("resolve node name %q to host", owner)
	}

	return ri.client.DeleteObject(ctx, host, ri.class, shardName, id, deletionTime, schemaVersion)
}

func (ri *RemoteIndex) MergeObject(ctx context.Context, shardName string,
	mergeDoc objects.MergeDocument, schemaVersion uint64,
) error {
	owner, err := ri.stateGetter.ShardOwner(ri.class, shardName)
	if err != nil {
		return fmt.Errorf("class %s has no physical shard %q: %w", ri.class, shardName, err)
	}

	host, ok := ri.nodeResolver.NodeHostname(owner)
	if !ok {
		return fmt.Errorf("resolve node name %q to host", owner)
	}

	return ri.client.MergeObject(ctx, host, ri.class, shardName, mergeDoc, schemaVersion)
}

func (ri *RemoteIndex) GetObject(ctx context.Context, shardName string,
	id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
	owner, err := ri.stateGetter.ShardOwner(ri.class, shardName)
	if err != nil {
		return nil, fmt.Errorf("class %s has no physical shard %q: %w", ri.class, shardName, err)
	}

	host, ok := ri.nodeResolver.NodeHostname(owner)
	if !ok {
		return nil, fmt.Errorf("resolve node name %q to host", owner)
	}

	return ri.client.GetObject(ctx, host, ri.class, shardName, id, props, additional)
}

func (ri *RemoteIndex) MultiGetObjects(ctx context.Context, shardName string,
	ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	owner, err := ri.stateGetter.ShardOwner(ri.class, shardName)
	if err != nil {
		return nil, fmt.Errorf("class %s has no physical shard %q: %w", ri.class, shardName, err)
	}

	host, ok := ri.nodeResolver.NodeHostname(owner)
	if !ok {
		return nil, fmt.Errorf("resolve node name %q to host", owner)
	}

	return ri.client.MultiGetObjects(ctx, host, ri.class, shardName, ids)
}

type ReplicasSearchResult struct {
	Objects []*storobj.Object
	Scores  []float32
	Node    string
}

func (ri *RemoteIndex) SearchAllReplicas(ctx context.Context,
	log logrus.FieldLogger,
	shard string,
	queryVec []models.Vector,
	targetVector []string,
	distance float32,
	limit int,
	filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking,
	sort []filters.Sort,
	cursor *filters.Cursor,
	groupBy *searchparams.GroupBy,
	adds additional.Properties,
	replEnabled bool,
	localNode string,
	targetCombination *dto.TargetCombination,
	properties []string,
) ([]ReplicasSearchResult, error) {
	remoteShardQuery := func(node, host string) (ReplicasSearchResult, error) {
		objs, scores, err := ri.client.SearchShard(ctx, host, ri.class, shard,
			queryVec, targetVector, distance, limit, filters, keywordRanking, sort, cursor, groupBy, adds, targetCombination, properties)
		if err != nil {
			return ReplicasSearchResult{}, err
		}
		return ReplicasSearchResult{Objects: objs, Scores: scores, Node: node}, nil
	}
	return ri.queryAllReplicas(ctx, log, shard, remoteShardQuery, localNode)
}

func (ri *RemoteIndex) SearchShard(ctx context.Context, shard string,
	queryVec []models.Vector,
	targetVector []string,
	distance float32,
	limit int,
	filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking,
	sort []filters.Sort,
	cursor *filters.Cursor,
	groupBy *searchparams.GroupBy,
	adds additional.Properties,
	replEnabled bool,
	targetCombination *dto.TargetCombination,
	properties []string,
) ([]*storobj.Object, []float32, string, error) {
	type pair struct {
		first  []*storobj.Object
		second []float32
	}
	f := func(node, host string) (interface{}, error) {
		objs, scores, err := ri.client.SearchShard(ctx, host, ri.class, shard,
			queryVec, targetVector, distance, limit, filters, keywordRanking, sort, cursor, groupBy, adds, targetCombination, properties)
		if err != nil {
			return nil, err
		}
		return pair{objs, scores}, err
	}
	rr, node, err := ri.queryReplicas(ctx, shard, f)
	if err != nil {
		return nil, nil, node, err
	}
	r := rr.(pair)
	return r.first, r.second, node, err
}

func (ri *RemoteIndex) Aggregate(
	ctx context.Context,
	shard string,
	params aggregation.Params,
) (*aggregation.Result, error) {
	f := func(_, host string) (interface{}, error) {
		r, err := ri.client.Aggregate(ctx, host, ri.class, shard, params)
		if err != nil {
			return nil, err
		}
		return r, nil
	}
	rr, _, err := ri.queryReplicas(ctx, shard, f)
	if err != nil {
		return nil, err
	}
	return rr.(*aggregation.Result), err
}

func (ri *RemoteIndex) FindUUIDs(ctx context.Context, shardName string,
	filters *filters.LocalFilter,
) ([]strfmt.UUID, error) {
	owner, err := ri.stateGetter.ShardOwner(ri.class, shardName)
	if err != nil {
		return nil, fmt.Errorf("class %s has no physical shard %q: %w", ri.class, shardName, err)
	}

	host, ok := ri.nodeResolver.NodeHostname(owner)
	if !ok {
		return nil, fmt.Errorf("resolve node name %q to host", owner)
	}

	return ri.client.FindUUIDs(ctx, host, ri.class, shardName, filters)
}

func (ri *RemoteIndex) DeleteObjectBatch(ctx context.Context, shardName string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) objects.BatchSimpleObjects {
	owner, err := ri.stateGetter.ShardOwner(ri.class, shardName)
	if err != nil {
		err := fmt.Errorf("class %s has no physical shard %q: %w", ri.class, shardName, err)
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	host, ok := ri.nodeResolver.NodeHostname(owner)
	if !ok {
		err := fmt.Errorf("resolve node name %q to host", owner)
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	return ri.client.DeleteObjectBatch(ctx, host, ri.class, shardName, uuids, deletionTime, dryRun, schemaVersion)
}

func (ri *RemoteIndex) GetShardQueueSize(ctx context.Context, shardName string) (int64, error) {
	owner, err := ri.stateGetter.ShardOwner(ri.class, shardName)
	if err != nil {
		return 0, fmt.Errorf("class %s has no physical shard %q: %w", ri.class, shardName, err)
	}

	host, ok := ri.nodeResolver.NodeHostname(owner)
	if !ok {
		return 0, fmt.Errorf("resolve node name %q to host", owner)
	}

	return ri.client.GetShardQueueSize(ctx, host, ri.class, shardName)
}

func (ri *RemoteIndex) GetShardStatus(ctx context.Context, shardName string) (string, error) {
	owner, err := ri.stateGetter.ShardOwner(ri.class, shardName)
	if err != nil {
		return "", fmt.Errorf("class %s has no physical shard %q: %w", ri.class, shardName, err)
	}

	host, ok := ri.nodeResolver.NodeHostname(owner)
	if !ok {
		return "", fmt.Errorf("resolve node name %q to host", owner)
	}

	return ri.client.GetShardStatus(ctx, host, ri.class, shardName)
}

func (ri *RemoteIndex) UpdateShardStatus(ctx context.Context, shardName, targetStatus string, schemaVersion uint64) error {
	owner, err := ri.stateGetter.ShardOwner(ri.class, shardName)
	if err != nil {
		return fmt.Errorf("class %s has no physical shard %q: %w", ri.class, shardName, err)
	}

	host, ok := ri.nodeResolver.NodeHostname(owner)
	if !ok {
		return fmt.Errorf("resolve node name %q to host", owner)
	}

	return ri.client.UpdateShardStatus(ctx, host, ri.class, shardName, targetStatus, schemaVersion)
}

func (ri *RemoteIndex) queryAllReplicas(
	ctx context.Context,
	log logrus.FieldLogger,
	shard string,
	do func(nodeName, host string) (ReplicasSearchResult, error),
	localNode string,
) (resp []ReplicasSearchResult, err error) {
	replicas, err := ri.stateGetter.ShardReplicas(ri.class, shard)
	if err != nil || len(replicas) == 0 {
		return nil, fmt.Errorf("class %q has no physical shard %q: %w", ri.class, shard, err)
	}

	queryOne := func(replica string) (ReplicasSearchResult, error) {
		host, ok := ri.nodeResolver.NodeHostname(replica)
		if !ok || host == "" {
			return ReplicasSearchResult{}, fmt.Errorf("unable to resolve node name %q to host", replica)
		}
		return do(replica, host)
	}

	var queriesSent atomic.Int64

	queryAll := func(replicas []string) (resp []ReplicasSearchResult, err error) {
		var mu sync.Mutex // protect resp + errlist
		var searchResult ReplicasSearchResult
		var errList error

		wg := sync.WaitGroup{}
		for _, node := range replicas {
			node := node // prevent loop variable capture
			if node == localNode {
				// Skip local node to ensure we don't query again our local shard -> it is handled separately in the search
				continue
			}

			wg.Add(1)
			enterrors.GoWrapper(func() {
				defer wg.Done()

				if errC := ctx.Err(); errC != nil {
					mu.Lock()
					errList = errors.Join(errList, fmt.Errorf("error while searching shard=%s replica node=%s: %w", shard, node, errC))
					mu.Unlock()
					return
				}

				queriesSent.Add(1)
				if searchResult, err = queryOne(node); err != nil {
					mu.Lock()
					errList = errors.Join(errList, fmt.Errorf("error while searching shard=%s replica node=%s: %w", shard, node, err))
					mu.Unlock()
					return
				}

				mu.Lock()
				resp = append(resp, searchResult)
				mu.Unlock()
			}, log)
		}
		wg.Wait()

		if errList != nil {
			// Simply log the errors but don't return them unless we have no valid result
			log.Warnf("errors happened during full replicas search for shard '%s' errors: %s", shard, errList)
		}

		if len(resp) == 0 {
			return nil, errList
		}
		if len(resp) != int(queriesSent.Load()) {
			log.Warnf("full replicas search response does not match replica count: response=%d replicas=%d", len(resp), len(replicas))
		}
		return resp, nil
	}
	return queryAll(replicas)
}

func (ri *RemoteIndex) queryReplicas(
	ctx context.Context,
	shard string,
	do func(nodeName, host string) (interface{}, error),
) (resp interface{}, node string, err error) {
	replicas, err := ri.stateGetter.ShardReplicas(ri.class, shard)
	if err != nil || len(replicas) == 0 {
		return nil,
			"",
			fmt.Errorf("class %q has no physical shard %q: %w", ri.class, shard, err)
	}

	queryOne := func(replica string) (interface{}, error) {
		host, ok := ri.nodeResolver.NodeHostname(replica)
		if !ok || host == "" {
			return nil, fmt.Errorf("resolve node name %q to host", replica)
		}
		return do(replica, host)
	}

	queryUntil := func(replicas []string) (resp interface{}, node string, err error) {
		for _, node = range replicas {
			if errC := ctx.Err(); errC != nil {
				return nil, node, errC
			}
			if resp, err = queryOne(node); err == nil {
				return resp, node, nil
			}
		}
		return
	}
	first := rand.Intn(len(replicas))
	if resp, node, err = queryUntil(replicas[first:]); err != nil && first != 0 {
		return queryUntil(replicas[:first])
	}
	return
}
