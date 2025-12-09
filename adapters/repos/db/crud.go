//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	objectttl "github.com/weaviate/weaviate/usecases/object_ttl"

	"github.com/weaviate/weaviate/adapters/repos/db/refcache"
	"github.com/weaviate/weaviate/adapters/repos/db/ttl"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

func (db *DB) PutObject(ctx context.Context, obj *models.Object,
	vector []float32, vectors map[string][]float32, multivectors map[string][][]float32,
	repl *additional.ReplicationProperties,
	schemaVersion uint64,
) error {
	object := storobj.FromObject(obj, vector, vectors, multivectors)
	idx := db.GetIndex(object.Class())
	if idx == nil {
		return fmt.Errorf("import into non-existing index for %s", object.Class())
	}

	if err := idx.putObject(ctx, object, repl, obj.Tenant, schemaVersion); err != nil {
		return fmt.Errorf("import into index %s: %w", idx.ID(), err)
	}

	return nil
}

// DeleteObject from of a specific class giving its ID
func (db *DB) DeleteObject(ctx context.Context, class string, id strfmt.UUID,
	deletionTime time.Time, repl *additional.ReplicationProperties, tenant string, schemaVersion uint64,
) error {
	idx := db.GetIndex(schema.ClassName(class))
	if idx == nil {
		return fmt.Errorf("delete from non-existing index for %s", class)
	}

	err := idx.deleteObject(ctx, id, deletionTime, repl, tenant, schemaVersion)
	if err != nil {
		return fmt.Errorf("delete from index %q: %w", idx.ID(), err)
	}

	return nil
}

func (db *DB) triggerDeletionObjectsExpiredLocalNode(ctx context.Context, collections []string,
	ttlTime, deletionTime time.Time,
) error {
	start := time.Now()

	ec := errorcompounder.NewSafe()
	eg := enterrors.NewErrorGroupWrapper(db.logger)
	eg.SetLimit(concurrency.TimesFloatGOMAXPROCS(db.config.ObjectsTtlConcurrencyFactor))

	objectsDeleted := atomic.Int32{}
	onObjectsDeleted := func(count int32) { objectsDeleted.Add(count) }

	for _, collection := range collections {
		onCollectionError := func(err error) {
			if err != nil {
				ec.Add(fmt.Errorf("collection %q [%w]", collection, err))
			}
		}
		func() {
			if err := ctx.Err(); err != nil {
				onCollectionError(err)
				return
			}

			idx, release, deleteOnPropName, ttlThreshold := db.extractTtlDataFromCollection(collection, ttlTime)
			if idx == nil {
				onCollectionError(fmt.Errorf("index not found"))
				return
			}
			defer release()

			// TODO aliszka:ttl schemaVersion?
			idx.IncomingDeleteObjectsExpired(deleteOnPropName, ttlThreshold, deletionTime, eg, onCollectionError, onObjectsDeleted, 0)
		}()
	}

	// ignore errors from eg as they are already collected in ec
	eg.Wait()

	l := db.logger.WithFields(logrus.Fields{
		"action":        "object_ttl_deletions",
		"total_deleted": objectsDeleted.Load(),
		"took":          time.Since(start).String(),
	})

	if err := ec.ToError(); err != nil {
		l.WithError(err).Errorf("ttl deletions finished with errors")
		return fmt.Errorf("deleting expired objects: %w", err)
	}
	l.Info("ttl deletions successfully finished")
	return nil
}

func (db *DB) triggerDeletionObjectsExpiredRemoteNode(ctx context.Context, remoteObjectTTL *objectttl.RemoteObjectTTL, collections []string,
	ttlTime, deletionTime time.Time, nodes []string,
) error {
	var node string

	switch nodesCount := len(nodes); nodesCount {
	case 0:
		return fmt.Errorf("no nodes provided")
	case 1:
		node = nodes[0]
	default:
		i := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(nodesCount)
		node = nodes[i]
	}

	ttlCollections := make([]objectttl.ObjectsExpiredPayload, 0, len(collections))
	for _, collection := range collections {
		func() {
			idx, release, deleteOnPropName, ttlThreshold := db.extractTtlDataFromCollection(collection, ttlTime)
			if idx == nil {
				return
			}
			defer release()

			ttlCollections = append(ttlCollections, objectttl.ObjectsExpiredPayload{
				Class:    collection,
				Prop:     deleteOnPropName,
				TtlMilli: ttlThreshold.UnixMilli(),
				DelMilli: deletionTime.UnixMilli(),
			})
		}()
	}

	if len(ttlCollections) == 0 {
		return nil
	}

	// check if deletion is running on the last node we picked
	if db.objectTTLLastNode != "" {
		ttlOngoing, err := remoteObjectTTL.CheckIfStillRunning(ctx, db.objectTTLLastNode)
		if err != nil {
			db.logger.WithFields(logrus.Fields{
				"action": "object_ttl_deletions",
				"node":   db.objectTTLLastNode,
			}).Errorf("Checking objectTTL running status failed: %v", err)
		} else if ttlOngoing {
			db.logger.WithFields(logrus.Fields{
				"action": "object_ttl_deletions",
				"node":   db.objectTTLLastNode,
			}).Warn("ObjectTTL is still running, skipping this round")
			return nil // deletion for collection still running, skip this round
		}
	}

	db.objectTTLLastNode = node
	return remoteObjectTTL.StartRemoteDelete(ctx, node, ttlCollections)
}

func (db *DB) extractTtlDataFromCollection(collection string, ttlTime time.Time,
) (idx *Index, release func(), deleteOnPropName string, ttlThreshold time.Time) {
	db.indexLock.RLock()
	idx = db.indices[collection]
	if idx != nil {
		idx.closeLock.RLock()
	}
	db.indexLock.RUnlock()

	if idx != nil {
		// ec consistency issue between index and RAFT store
		if class := idx.getClass(); class != nil {
			if ttl.IsTtlEnabled(class.ObjectTTLConfig) {
				deleteOnPropName = class.ObjectTTLConfig.DeleteOn
				ttlThreshold = ttlTime.Add(-time.Second * time.Duration(class.ObjectTTLConfig.DefaultTTL))
				return idx, idx.closeLock.RUnlock, deleteOnPropName, ttlThreshold
			}
		}
		idx.closeLock.RUnlock()
	}
	return nil, func() {}, "", time.Time{}
}

func (db *DB) TriggerDeletionObjectsExpired(ctx context.Context, remoteObjectTTL *objectttl.RemoteObjectTTL, ttlTime, deletionTime time.Time, targetOwnNode bool) error {
	if !db.objectTTLOngoing.CompareAndSwap(false, true) {
		return fmt.Errorf("TTL deletion already ongoing")
	}
	defer db.objectTTLOngoing.Store(false)

	collections := []string{}
	db.indexLock.RLock()
	for collection := range db.indices {
		collections = append(collections, collection)
	}
	db.indexLock.RUnlock()

	if len(collections) == 0 {
		return nil
	}

	localNode := db.schemaGetter.NodeName()
	allNodes := db.schemaGetter.Nodes()
	remoteNodes := make([]string, 0, len(allNodes))
	if targetOwnNode {
		remoteNodes = append(remoteNodes, localNode)
	} else {
		for _, node := range allNodes {
			if node != localNode {
				remoteNodes = append(remoteNodes, node)
			}
		}
	}

	db.logger.WithFields(logrus.Fields{
		"action":        "object_ttl_deletions",
		"all_nodes":     allNodes,
		"remote_nodes":  remoteNodes,
		"ttl_time":      ttlTime,
		"deletion_time": deletionTime,
	}).Debug("triggering deletion of expired objects")

	if len(remoteNodes) == 0 {
		return db.triggerDeletionObjectsExpiredLocalNode(ctx, collections, ttlTime, deletionTime)
	}
	return db.triggerDeletionObjectsExpiredRemoteNode(ctx, remoteObjectTTL, collections, ttlTime, deletionTime, remoteNodes)
}

func (db *DB) MultiGet(ctx context.Context, query []multi.Identifier,
	additional additional.Properties, tenant string,
) ([]search.Result, error) {
	byIndex := map[string][]multi.Identifier{}
	db.indexLock.RLock()
	defer db.indexLock.RUnlock()

	for i, q := range query {
		// store original position to make assembly easier later
		q.OriginalPosition = i

		for _, index := range db.indices {
			if index.Config.ClassName != schema.ClassName(q.ClassName) {
				continue
			}

			queue := byIndex[index.ID()]
			queue = append(queue, q)
			byIndex[index.ID()] = queue
		}
	}

	out := make(search.Results, len(query))
	for indexID, queries := range byIndex {
		indexRes, err := db.indices[indexID].multiObjectByID(ctx, queries, tenant)
		if err != nil {
			return nil, fmt.Errorf("index %q: %w", indexID, err)
		}

		for i, obj := range indexRes {
			if obj == nil {
				continue
			}
			res := obj.SearchResult(additional, tenant)
			out[queries[i].OriginalPosition] = *res
		}
	}

	return out, nil
}

// ObjectByID checks every index of the particular kind for the ID
//
// @warning: this function is deprecated by Object()
func (db *DB) ObjectByID(ctx context.Context, id strfmt.UUID,
	props search.SelectProperties, additional additional.Properties,
	tenant string,
) (*search.Result, error) {
	results, err := db.ObjectsByID(ctx, id, props, additional, tenant)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return &results[0], nil
}

// ObjectsByID checks every index of the particular kind for the ID
// this method is only used for Explore queries where we don't have
// a class context
func (db *DB) ObjectsByID(ctx context.Context, id strfmt.UUID,
	props search.SelectProperties, additional additional.Properties,
	tenant string,
) (search.Results, error) {
	var result []*storobj.Object
	// TODO: Search in parallel, rather than sequentially or this will be
	// painfully slow on large schemas
	db.indexLock.RLock()

	for _, index := range db.indices {
		res, err := index.objectByID(ctx, id, props, additional, nil, tenant)
		if err != nil {
			db.indexLock.RUnlock()
			switch {
			case errors.As(err, &objects.ErrMultiTenancy{}):
				return nil, objects.NewErrMultiTenancy(fmt.Errorf("search index %s: %w", index.ID(), err))
			default:
				return nil, fmt.Errorf("search index %s: %w", index.ID(), err)
			}
		}

		if res != nil {
			result = append(result, res)
		}
	}
	db.indexLock.RUnlock()

	if result == nil {
		return nil, nil
	}

	return db.ResolveReferences(ctx,
		storobj.SearchResults(result, additional, tenant), props, nil, additional, tenant)
}

// Object gets object with id from index of specified class.
func (db *DB) Object(ctx context.Context, class string, id strfmt.UUID,
	props search.SelectProperties, addl additional.Properties,
	repl *additional.ReplicationProperties, tenant string,
) (*search.Result, error) {
	idx := db.GetIndex(schema.ClassName(class))
	if idx == nil {
		return nil, nil
	}

	obj, err := idx.objectByID(ctx, id, props, addl, repl, tenant)
	if err != nil {
		var errMultiTenancy objects.ErrMultiTenancy
		switch {
		case errors.As(err, &errMultiTenancy):
			return nil, objects.NewErrMultiTenancy(fmt.Errorf("search index %s: %w", idx.ID(), err))
		default:
			return nil, fmt.Errorf("search index %s: %w", idx.ID(), err)
		}
	}
	var r *search.Result
	if obj != nil {
		r = obj.SearchResult(addl, tenant)
	}
	if r == nil {
		return nil, nil
	}
	return db.enrichRefsForSingle(ctx, r, props, addl, tenant)
}

func (db *DB) enrichRefsForSingle(ctx context.Context, obj *search.Result,
	props search.SelectProperties, additional additional.Properties, tenant string,
) (*search.Result, error) {
	res, err := refcache.NewResolver(refcache.NewCacher(db, db.logger, tenant)).
		Do(ctx, []search.Result{*obj}, props, additional)
	if err != nil {
		return nil, fmt.Errorf("resolve cross-refs: %w", err)
	}

	return &res[0], nil
}

func (db *DB) Exists(ctx context.Context, class string, id strfmt.UUID,
	repl *additional.ReplicationProperties, tenant string,
) (bool, error) {
	if class == "" {
		return db.anyExists(ctx, id, repl)
	}
	index := db.GetIndex(schema.ClassName(class))
	if index == nil {
		return false, nil
	}
	return index.exists(ctx, id, repl, tenant)
}

func (db *DB) anyExists(ctx context.Context, id strfmt.UUID,
	repl *additional.ReplicationProperties,
) (bool, error) {
	// TODO: Search in parallel, rather than sequentially or this will be
	// painfully slow on large schemas
	db.indexLock.RLock()
	defer db.indexLock.RUnlock()

	for _, index := range db.indices {
		ok, err := index.exists(ctx, id, repl, "")
		if err != nil {
			switch {
			case errors.As(err, &objects.ErrMultiTenancy{}):
				return false, objects.NewErrMultiTenancy(fmt.Errorf("search index %s: %w", index.ID(), err))
			default:
				return false, fmt.Errorf("search index %s: %w", index.ID(), err)
			}
		}
		if ok {
			return true, nil
		}
	}

	return false, nil
}

func (db *DB) AddReference(ctx context.Context, source *crossref.RefSource, target *crossref.Ref,
	repl *additional.ReplicationProperties, tenant string, schemaVersion uint64,
) error {
	return db.Merge(ctx, objects.MergeDocument{
		Class:      source.Class.String(),
		ID:         source.TargetID,
		UpdateTime: time.Now().UnixMilli(),
		References: objects.BatchReferences{
			objects.BatchReference{
				From: source,
				To:   target,
			},
		},
	}, repl, tenant, schemaVersion)
}

func (db *DB) Merge(ctx context.Context, merge objects.MergeDocument,
	repl *additional.ReplicationProperties, tenant string, schemaVersion uint64,
) error {
	idx := db.GetIndex(schema.ClassName(merge.Class))
	if idx == nil {
		return fmt.Errorf("merge from non-existing index for %s", merge.Class)
	}

	err := idx.mergeObject(ctx, merge, repl, tenant, schemaVersion)
	if err != nil {
		return fmt.Errorf("merge into index %s: %w", idx.ID(), err)
	}

	return nil
}
