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
	"time"

	"github.com/containerd/log"
	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/usecases/objectTTL"

	"github.com/weaviate/weaviate/adapters/repos/db/refcache"
	"github.com/weaviate/weaviate/adapters/repos/db/ttl"
	"github.com/weaviate/weaviate/entities/additional"
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

func (db *DB) triggerDeletionObjectsExpiredSingleNode(ctx context.Context, collections []string,
	ttlTime, deletionTime time.Time,
) error {
	ec := errorcompounder.New()
	eg := enterrors.NewErrorGroupWrapper(db.logger)

	for _, collection := range collections {
		func() {
			if err := ctx.Err(); err != nil {
				ec.Add(err)
				return
			}
			idx, release, deleteOnPropName, ttlThreshold := db.extractTtlDataFromCollection(collection, ttlTime)
			if idx == nil {
				return
			}
			defer release()

			// TODO aliszka:ttl schemaVersion?
			if err := idx.IncomingDeleteObjectsExpired(eg, deleteOnPropName, ttlThreshold, deletionTime, 0); err != nil {
				ec.Add(fmt.Errorf("delete expired for class %q: %w", collection, err))
			}
		}()
	}

	// ignore errors from eg as they are already collected in ec
	eg.Wait()

	return ec.ToError()
}

func (db *DB) triggerDeletionObjectsExpiredMultiNode(ctx context.Context, remoteObjectTTL *objectTTL.RemoteObjectTTL, collections []string,
	ttlTime, deletionTime time.Time, nodes []string,
) error {
	pickNode := func() string { return nodes[0] }
	if nodesCount := len(nodes); nodesCount > 1 {
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		i := rnd.Intn(nodesCount)

		pickNode = func() string {
			i = (i + 1) % nodesCount
			return nodes[i]
		}
	}

	ttlCollections := make([]objectTTL.ObjectsExpiredPayload, 0, len(collections))
	for _, collection := range collections {
		func() {
			idx, release, deleteOnPropName, ttlThreshold := db.extractTtlDataFromCollection(collection, ttlTime)
			if idx == nil {
				return
			}
			defer release()

			ttlCollections = append(ttlCollections, objectTTL.ObjectsExpiredPayload{
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

	// check if deletion is ongoing on the last node we picked
	if db.objectTTLLastNode != "" {
		ttlOngoing, err := remoteObjectTTL.CheckIfStillRunning(ctx, db.objectTTLLastNode)
		if err != nil {
			db.logger.WithFields(log.Fields{
				"action": "ObjectTTLTriggerDeletions",
				"node":   db.objectTTLLastNode,
			}).Errorf("Checking objectTTL ongoing status failed: %v", err)
		} else if ttlOngoing {
			db.logger.WithFields(log.Fields{
				"action": "ObjectTTLTriggerDeletions",
				"node":   db.objectTTLLastNode,
			}).Warn("ObjectTTL is still running, skipping this round")
			return nil // deletion for collection still ongoing, skip this round
		}
	}

	node := pickNode()
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
		class := idx.getClass()
		if class == nil { // ec consistency issue between index and RAFT store
			idx.closeLock.RUnlock()
			return nil, func() {}, "", time.Time{}
		}
		if ttl.IsTtlEnabled(class.ObjectTTLConfig) {
			deleteOnPropName = class.ObjectTTLConfig.DeleteOn
			ttlThreshold = ttlTime.Add(-time.Second * time.Duration(class.ObjectTTLConfig.DefaultTTL))

			return idx, idx.closeLock.RUnlock, deleteOnPropName, ttlThreshold
		}
		idx.closeLock.RUnlock()
	}
	return nil, func() {}, "", time.Time{}
}

func (db *DB) TriggerDeletionObjectsExpired(ctx context.Context, remoteObjectTTL *objectTTL.RemoteObjectTTL, ttlTime, deletionTime time.Time, targetOwnNode bool) error {
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
	otherNodes := make([]string, 0, len(allNodes))
	if targetOwnNode {
		otherNodes = append(otherNodes, localNode)
	} else {
		for _, node := range allNodes {
			if node != localNode {
				otherNodes = append(otherNodes, node)
			}
		}
	}

	db.logger.WithFields(log.Fields{
		"action":        "ObjectTTLTriggerDeletions",
		"all_nodes":     allNodes,
		"other_nodes":   otherNodes,
		"ttl_time":      ttlTime,
		"deletion_time": deletionTime,
	}).Info("Trigger deletion objects expired.")

	if len(otherNodes) == 0 {
		return db.triggerDeletionObjectsExpiredSingleNode(ctx, collections, ttlTime, deletionTime)
	}

	return db.triggerDeletionObjectsExpiredMultiNode(ctx, remoteObjectTTL, collections, ttlTime, deletionTime, otherNodes)
}

func (db *DB) DeleteObjectsExpired(ctx context.Context, expirationTime time.Time, concurrency int) error {
	fmt.Printf("  ==> expirationTime %s\n\n", expirationTime)

	db.indexLock.RLock()
	colNames := make([]string, len(db.indices))
	i := 0
	for colName := range db.indices {
		colNames[i] = colName
		i++
	}
	db.indexLock.RUnlock()

	if len(colNames) == 0 {
		return nil
	}

	chErrs := make([]<-chan error, 0, len(colNames))
	eg := enterrors.NewErrorGroupWrapper(db.logger)
	eg.SetLimit(concurrency)
	for _, colName := range colNames {
		db.indexLock.RLock()
		idx := db.indices[colName]
		db.indexLock.RUnlock()

		if idx != nil {
			// TODO aliszka:ttl prevent index from being removed while processed?
			chErr := idx.deleteObjectsExpiredAsync(ctx, eg, expirationTime)
			chErrs = append(chErrs, chErr)
		}
	}
	eg.Wait()

	ec := errorcompounder.New()
	for _, chErr := range chErrs {
		ec.Add(<-chErr)
	}
	return ec.ToError()
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
