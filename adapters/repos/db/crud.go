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
	"context"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/refcache"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

func (db *DB) PutObject(ctx context.Context, obj *models.Object,
	vector []float32, repl *additional.ReplicationProperties,
) error {
	object := storobj.FromObject(obj, vector)
	idx := db.GetIndex(object.Class())
	if idx == nil {
		return fmt.Errorf("import into non-existing index for %s", object.Class())
	}

	if err := idx.putObject(ctx, object, repl); err != nil {
		return fmt.Errorf("import into index %s: %w", idx.ID(), err)
	}

	return nil
}

// DeleteObject from of a specific class giving its ID
func (db *DB) DeleteObject(ctx context.Context, class string, id strfmt.UUID,
	repl *additional.ReplicationProperties, tenant string,
) error {
	idx := db.GetIndex(schema.ClassName(class))
	if idx == nil {
		return fmt.Errorf("delete from non-existing index for %s", class)
	}

	err := idx.deleteObject(ctx, id, repl, tenant)
	if err != nil {
		return fmt.Errorf("delete from index %q: %w", idx.ID(), err)
	}

	return nil
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
			return nil, errors.Wrapf(err, "index %q", indexID)
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
			switch err.(type) {
			case objects.ErrMultiTenancy:
				return nil, objects.NewErrMultiTenancy(fmt.Errorf("search index %s: %w", index.ID(), err))
			default:
				return nil, errors.Wrapf(err, "search index %s", index.ID())
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
		switch err.(type) {
		case objects.ErrMultiTenancy:
			return nil, objects.NewErrMultiTenancy(fmt.Errorf("search index %s: %w", idx.ID(), err))
		default:
			return nil, errors.Wrapf(err, "search index %s", idx.ID())
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
		return nil, errors.Wrap(err, "resolve cross-refs")
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
			switch err.(type) {
			case objects.ErrMultiTenancy:
				return false, objects.NewErrMultiTenancy(fmt.Errorf("search index %s: %w", index.ID(), err))
			default:
				return false, errors.Wrapf(err, "search index %s", index.ID())
			}
		}
		if ok {
			return true, nil
		}
	}

	return false, nil
}

func (db *DB) AddReference(ctx context.Context, source *crossref.RefSource, target *crossref.Ref,
	repl *additional.ReplicationProperties, tenant string,
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
	}, repl, tenant)
}

func (db *DB) Merge(ctx context.Context, merge objects.MergeDocument,
	repl *additional.ReplicationProperties, tenant string,
) error {
	idx := db.GetIndex(schema.ClassName(merge.Class))
	if idx == nil {
		return fmt.Errorf("merge from non-existing index for %s", merge.Class)
	}

	err := idx.mergeObject(ctx, merge, repl, tenant)
	if err != nil {
		return errors.Wrapf(err, "merge into index %s", idx.ID())
	}

	return nil
}
