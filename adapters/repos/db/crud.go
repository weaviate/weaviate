//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/refcache"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/multi"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (d *DB) PutObject(ctx context.Context, obj *models.Object,
	vector []float32) error {
	object := storobj.FromObject(obj, vector)
	idx := d.GetIndex(object.Class())
	if idx == nil {
		return fmt.Errorf("import into non-existing index for %s", object.Class())
	}

	err := idx.putObject(ctx, object)
	if err != nil {
		return errors.Wrapf(err, "import into index %s", idx.ID())
	}

	return nil
}

func (d *DB) DeleteObject(ctx context.Context, className string,
	id strfmt.UUID) error {
	idx := d.GetIndex(schema.ClassName(className))
	if idx == nil {
		return fmt.Errorf("delete from non-existing index for %s", className)
	}

	err := idx.deleteObject(ctx, id)
	if err != nil {
		return errors.Wrapf(err, "delete from index %s", idx.ID())
	}

	return nil
}

func (d *DB) MultiGet(ctx context.Context,
	query []multi.Identifier,
	additional traverser.AdditionalProperties) ([]search.Result, error) {
	byIndex := map[string][]multi.Identifier{}

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
func (d *DB) ObjectByID(ctx context.Context, id strfmt.UUID,
	props traverser.SelectProperties,
	additional traverser.AdditionalProperties) (*search.Result, error) {
	var result *search.Result
	// TODO: Search in parallel, rather than sequentially or this will be
	// painfully slow on large schemas
	for _, index := range d.indices {
		res, err := index.objectByID(ctx, id, props, additional)
		if err != nil {
			return nil, errors.Wrapf(err, "search index %s", index.ID())
		}

		if res != nil {
			result = res.SearchResult(additional)
			break
		}
	}

	if result == nil {
		return nil, nil
	}

	return d.enrichRefsForSingle(ctx, result, props, additional)
}

func (d *DB) enrichRefsForSingle(ctx context.Context, obj *search.Result,
	props traverser.SelectProperties, additional traverser.AdditionalProperties) (*search.Result, error) {
	res, err := refcache.NewResolver(refcache.NewCacher(d, d.logger)).
		Do(ctx, []search.Result{*obj}, props, additional)
	if err != nil {
		return nil, errors.Wrap(err, "resolve cross-refs")
	}

	return &res[0], nil
}

func (d *DB) Exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	// TODO: Search in parallel, rather than sequentially or this will be
	// painfully slow on large schemas
	for _, index := range d.indices {
		ok, err := index.exists(ctx, id)
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
	ref *models.SingleRef) error {
	target, err := crossref.ParseSingleRef(ref)
	if err != nil {
		return err
	}

	return d.Merge(ctx, objects.MergeDocument{
		Class:      className,
		ID:         source,
		UpdateTime: time.Now().UnixNano(),
		References: objects.BatchReferences{
			objects.BatchReference{
				From: crossref.NewSource(schema.ClassName(className),
					schema.PropertyName(propName), source),
				To: target,
			},
		},
	})
}

func (d *DB) Merge(ctx context.Context, merge objects.MergeDocument) error {
	idx := d.GetIndex(schema.ClassName(merge.Class))
	if idx == nil {
		return fmt.Errorf("merge from non-existing index for %s", merge.Class)
	}

	err := idx.mergeObject(ctx, merge)
	if err != nil {
		return errors.Wrapf(err, "merge into index %s", idx.ID())
	}

	return nil
}
