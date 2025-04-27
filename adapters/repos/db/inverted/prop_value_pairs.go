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

package inverted

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/concurrency"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
)

type propValuePair struct {
	prop     string
	operator filters.Operator

	// set for all values that can be served by an inverted index, i.e. anything
	// that's not a geoRange
	value []byte

	// only set if operator=OperatorWithinGeoRange, as that cannot be served by a
	// byte value from an inverted index
	valueGeoRange      *filters.GeoRange
	docIDs             docBitmap
	children           []*propValuePair
	hasFilterableIndex bool
	hasSearchableIndex bool
	hasRangeableIndex  bool
	Class              *models.Class // The schema
}

func newPropValuePair(class *models.Class) (*propValuePair, error) {
	if class == nil {
		return nil, errors.Errorf("class must not be nil")
	}
	return &propValuePair{docIDs: newDocBitmap(), Class: class}, nil
}

func (pv *propValuePair) fetchDocIDs(ctx context.Context, s *Searcher, limit int) error {
	if pv.operator.OnValue() {
		// TODO text_rbm_inverted_index find better way check whether prop len
		if strings.HasSuffix(pv.prop, filters.InternalPropertyLength) &&
			!pv.Class.InvertedIndexConfig.IndexPropertyLength {
			return errors.Errorf("Property length must be indexed to be filterable! add `IndexPropertyLength: true` to the invertedIndexConfig in %v.  Geo-coordinates, phone numbers and data blobs are not supported by property length.", pv.Class.Class)
		}

		if pv.operator == filters.OperatorIsNull && !pv.Class.InvertedIndexConfig.IndexNullState {
			return errors.Errorf("Nullstate must be indexed to be filterable! Add `indexNullState: true` to the invertedIndexConfig")
		}

		if (pv.prop == filters.InternalPropCreationTimeUnix ||
			pv.prop == filters.InternalPropLastUpdateTimeUnix) &&
			!pv.Class.InvertedIndexConfig.IndexTimestamps {
			return errors.Errorf("Timestamps must be indexed to be filterable! Add `IndexTimestamps: true` to the InvertedIndexConfig in %v", pv.Class.Class)
		}

		bucketName := pv.getBucketName()
		if bucketName == "" {
			return errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		b := s.store.Bucket(bucketName)

		// TODO:  I think we can delete this check entirely.  The bucket will never be nill, and routines should now check if their particular feature is active in the schema.  However, not all those routines have checks yet.
		if b == nil && pv.operator != filters.OperatorWithinGeoRange {
			// a nil bucket is ok for a WithinGeoRange filter, as this query is not
			// served by the inverted index, but propagated to a secondary index in
			// .docPointers()
			return errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		dbm, err := s.docBitmap(ctx, b, limit, pv)
		if err != nil {
			return err
		}
		pv.docIDs = dbm
	} else {
		eg := enterrors.NewErrorGroupWrapper(s.logger)
		// prevent unbounded concurrency, see
		// https://github.com/weaviate/weaviate/issues/3179 for details
		eg.SetLimit(2 * _NUMCPU)
		for i, child := range pv.children {
			i, child := i, child
			eg.Go(func() error {
				// Explicitly set the limit to 0 (=unlimited) as this is a nested filter,
				// otherwise we run into situations where each subfilter on their own
				// runs into the limit, possibly yielding in "less than limit" results
				// after merging.
				err := child.fetchDocIDs(ctx, s, 0)
				if err != nil {
					return errors.Wrapf(err, "nested child %d", i)
				}

				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return fmt.Errorf("nested query: %w", err)
		}
	}

	return nil
}

func (pv *propValuePair) mergeDocIDs() (*docBitmap, error) {
	if pv.operator.OnValue() {
		return &pv.docIDs, nil
	}

	switch pv.operator {
	case filters.OperatorAnd:
	case filters.OperatorOr:
		// ok
	default:
		return nil, fmt.Errorf("unsupported operator: %s", pv.operator.Name())
	}

	switch len(pv.children) {
	case 0:
		return nil, fmt.Errorf("no children for operator: %s", pv.operator.Name())
	case 1:
		return pv.children[0].mergeDocIDs()
	}

	var err error
	dbms := make([]*docBitmap, len(pv.children))
	for i, child := range pv.children {
		dbms[i], err = child.mergeDocIDs()
		if err != nil {
			return nil, errors.Wrapf(err, "retrieve doc bitmap of child %d", i)
		}
	}

	var mergeFn func(*sroar.Bitmap, int) *sroar.Bitmap
	if pv.operator == filters.OperatorOr {
		// biggest to smallest, so smaller bitmaps are merged into biggest one,
		// minimising chance of expanding destination bitmap (memory allocations)
		sort.Slice(dbms, func(i, j int) bool {
			return dbms[i].docIDs.CompareNumKeys(dbms[j].docIDs) > 0
		})
		mergeFn = dbms[0].docIDs.OrConc
	} else {
		// smallest to biggest, so data is removed from smallest bitmap
		// allowing bigger bitmaps to be garbage collected asap
		sort.Slice(dbms, func(i, j int) bool {
			return dbms[i].docIDs.CompareNumKeys(dbms[j].docIDs) < 0
		})
		mergeFn = dbms[0].docIDs.AndConc
	}

	for i := 1; i < len(dbms); i++ {
		mergeFn(dbms[i].docIDs, concurrency.SROAR_MERGE)
		dbms[i].release()
	}
	return dbms[0], nil
}

func (pv *propValuePair) getBucketName() string {
	if pv.hasRangeableIndex {
		switch pv.operator {
		// decide whether handle equal/not_equal with rangeable index
		case filters.OperatorGreaterThan,
			filters.OperatorGreaterThanEqual,
			filters.OperatorLessThan,
			filters.OperatorLessThanEqual:
			return helpers.BucketRangeableFromPropNameLSM(pv.prop)
		default:
		}
	}
	if pv.hasFilterableIndex {
		return helpers.BucketFromPropNameLSM(pv.prop)
	}
	// fallback equal/not_equal to rangeable
	if pv.hasRangeableIndex {
		switch pv.operator {
		case filters.OperatorEqual,
			filters.OperatorNotEqual:
			return helpers.BucketRangeableFromPropNameLSM(pv.prop)
		default:
		}
	}
	if pv.hasSearchableIndex {
		return helpers.BucketSearchableFromPropNameLSM(pv.prop)
	}
	return ""
}
