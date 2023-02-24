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

func (d *DB) PutObject(ctx context.Context, obj *models.Object,
	vector []float32, repl *additional.ReplicationProperties,
) error {
	object := storobj.FromObject(obj, vector)
	idx := d.GetIndex(object.Class())
	if idx == nil {
		return fmt.Errorf("import into non-existing index for %s", object.Class())
	}

	if err := idx.putObject(ctx, object, repl); err != nil {
		return errors.Wrapf(err, "import into index %s", idx.ID())
	}

	return nil
}

// DeleteObject from of a specific class giving its ID
func (d *DB) DeleteObject(ctx context.Context, class string,
	id strfmt.UUID, repl *additional.ReplicationProperties,
) error {
	idx := d.GetIndex(schema.ClassName(class))
	if idx == nil {
		return fmt.Errorf("delete from non-existing index for %s", class)
	}

	err := idx.deleteObject(ctx, id, repl)
	if err != nil {
		return fmt.Errorf("delete from index %q: %w", idx.ID(), err)
	}

	return nil
}

func (d *DB) MultiGet(ctx context.Context,
	query []multi.Identifier,
	additional additional.Properties,
) ([]search.Result, error) {
	byIndex := map[string][]multi.Identifier{}
	d.indexLock.RLock()
	defer d.indexLock.RUnlock()

	for i, q := range query {
		// store original position to make assembly easier later
		q.OriginalPosition = i

		for _, index := range d.indices {
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
		indexRes, err := d.indices[indexID].multiObjectByID(ctx, queries)
		if err != nil {
			return nil, errors.Wrapf(err, "index %q", indexID)
		}

		for i, obj := range indexRes {
			if obj == nil {
				continue
			}
			res := obj.SearchResult(additional)
			out[queries[i].OriginalPosition] = *res
		}
	}

	return out, nil
}

// ObjectByID checks every index of the particular kind for the ID
//
// @warning: this function is deprecated by Object()
func (d *DB) ObjectByID(ctx context.Context, id strfmt.UUID,
	props search.SelectProperties,
	additional additional.Properties,
) (*search.Result, error) {
	results, err := d.ObjectsByID(ctx, id, props, additional)
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
func (d *DB) ObjectsByID(ctx context.Context, id strfmt.UUID,
	props search.SelectProperties,
	additional additional.Properties,
) (search.Results, error) {
	var result []*storobj.Object
	// TODO: Search in parallel, rather than sequentially or this will be
	// painfully slow on large schemas
	d.indexLock.RLock()

	for _, index := range d.indices {
		res, err := index.objectByID(ctx, id, props, additional, nil)
		if err != nil {
			d.indexLock.RUnlock()
			return nil, errors.Wrapf(err, "search index %s", index.ID())
		}

		if res != nil {
			result = append(result, res)
		}
	}
	d.indexLock.RUnlock()

	if result == nil {
		return nil, nil
	}

	return d.ResolveReferences(ctx,
		storobj.SearchResults(result, additional), props, additional)
}

// Object gets object with id from index of specified class.
func (d *DB) Object(ctx context.Context, class string,
	id strfmt.UUID, props search.SelectProperties,
	adds additional.Properties, repl *additional.ReplicationProperties,
) (*search.Result, error) {
	idx := d.GetIndex(schema.ClassName(class))
	if idx == nil {
		return nil, nil
	}

	obj, err := idx.objectByID(ctx, id, props, adds, repl)
	if err != nil {
		return nil, errors.Wrapf(err, "search index %s", idx.ID())
	}
	var r *search.Result
	if obj != nil {
		r = obj.SearchResult(adds)
	}
	if r == nil {
		return nil, nil
	}
	return d.enrichRefsForSingle(ctx, r, props, adds)
}

func (d *DB) enrichRefsForSingle(ctx context.Context, obj *search.Result,
	props search.SelectProperties, additional additional.Properties,
) (*search.Result, error) {
	res, err := refcache.NewResolver(refcache.NewCacher(d, d.logger)).
		Do(ctx, []search.Result{*obj}, props, additional)
	if err != nil {
		return nil, errors.Wrap(err, "resolve cross-refs")
	}

	return &res[0], nil
}

func (d *DB) Exists(ctx context.Context, class string,
	id strfmt.UUID, repl *additional.ReplicationProperties,
) (bool, error) {
	if class == "" {
		return d.anyExists(ctx, id, repl)
	}
	index := d.GetIndex(schema.ClassName(class))
	if index == nil {
		return false, nil
	}
	return index.exists(ctx, id, repl)
}

func (d *DB) anyExists(ctx context.Context, id strfmt.UUID,
	repl *additional.ReplicationProperties,
) (bool, error) {
	// TODO: Search in parallel, rather than sequentially or this will be
	// painfully slow on large schemas
	d.indexLock.RLock()
	defer d.indexLock.RUnlock()

	for _, index := range d.indices {
		ok, err := index.exists(ctx, id, repl)
		if err != nil {
			return false, errors.Wrapf(err, "search index %s", index.ID())
		}
		if ok {
			return true, nil
		}
	}

	return false, nil
}

func (d *DB) AddReference(ctx context.Context,
	className string, source strfmt.UUID, propName string,
	ref *models.SingleRef, repl *additional.ReplicationProperties,
) error {
	target, err := crossref.ParseSingleRef(ref)
	if err != nil {
		return err
	}

	return d.Merge(ctx, objects.MergeDocument{
		Class:      className,
		ID:         source,
		UpdateTime: time.Now().UnixMilli(),
		References: objects.BatchReferences{
			objects.BatchReference{
				From: crossref.NewSource(schema.ClassName(className),
					schema.PropertyName(propName), source),
				To: target,
			},
		},
	}, repl)
}

func (d *DB) Merge(ctx context.Context, merge objects.MergeDocument,
	repl *additional.ReplicationProperties,
) error {
	idx := d.GetIndex(schema.ClassName(merge.Class))
	if idx == nil {
		return fmt.Errorf("merge from non-existing index for %s", merge.Class)
	}

	err := idx.mergeObject(ctx, merge, repl)
	if err != nil {
		return errors.Wrapf(err, "merge into index %s", idx.ID())
	}

	return nil
}
