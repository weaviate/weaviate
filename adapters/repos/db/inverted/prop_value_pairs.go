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

package inverted

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/errorcompounder"
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

func (pv *propValuePair) resolveDocIDs(ctx context.Context, s *Searcher, limit int) (*docBitmap, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if pv.operator.OnValue() {
		return pv.fetchDocIDs(ctx, s, limit)
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
		return pv.children[0].resolveDocIDs(ctx, s, limit)
	default:
		// proceed
	}

	// Explicitly set the limit to 0 (=unlimited) as this is a nested filter,
	// otherwise we run into situations where each subfilter on their own
	// runs into the limit, possibly yielding in "less than limit" results
	// after merging.
	limit = 0

	maxN := 32                           // number of children to be fetched before merging them
	dbmCh := make(chan *docBitmap, maxN) // subresults to merge
	resultCh := make(chan *docBitmap, 1) // merge result
	var err error

	// merge subresults in separate goroutine using dbmCh (in) and resultCh (out)
	enterrors.GoWrapper(func() {
		processDocIDs(maxN, pv.operator, dbmCh, resultCh)
	}, s.logger)

	outerConcurrencyLimit := concurrency.BudgetFromCtx(ctx, concurrency.NUMCPU)
	if outerConcurrencyLimit <= 1 {
		// resolve docIDs sequentially in main goroutine
		for i, child := range pv.children {
			dbm, err2 := child.resolveDocIDs(ctx, s, limit)
			if err2 != nil {
				// break on first error
				err = errors.Wrapf(err, "nested child %d", i)
				break
			}
			dbmCh <- dbm
		}
	} else {
		// resolve docIDs in parallel using goroutines
		concurrencyReductionFactor := min(len(pv.children), outerConcurrencyLimit)

		// collect all errors from goroutines (not only 1st one)
		ec := errorcompounder.NewSafe()
		// use error group's context to skip remaining children after 1st error
		eg, gctx := enterrors.NewErrorGroupWithContextWrapper(s.logger, ctx)
		eg.SetLimit(outerConcurrencyLimit - 1)

		for i, child := range pv.children {
			i, child := i, child
			eg.Go(func() error {
				if err := gctx.Err(); err != nil {
					// some child failed, skip processing
					return nil
				}

				ctx := concurrency.ContextWithFractionalBudget(ctx, concurrencyReductionFactor, concurrency.NUMCPU)
				dbm, err := child.resolveDocIDs(ctx, s, limit)
				if err != nil {
					err = errors.Wrapf(err, "nested child %d", i)
					ec.Add(err)
					return err
				}
				dbmCh <- dbm
				return nil
			})
			// some child failed, skip remaining children
			if gctx.Err() != nil {
				break
			}
		}
		errWait := eg.Wait()
		err = ec.ToError()
		if err == nil {
			// if parent context gets expired/cancelled groupcontext might prevent execution of any goroutine,
			// making error compounder empty. in that case take potencial error (context related) from wait
			err = errWait
		}
	}

	close(dbmCh)
	result := <-resultCh

	if err != nil {
		result.release()
		return nil, fmt.Errorf("nested query: %w", err)
	}
	return result, nil
}

// processDocIDs merges received from dbmCh channel docBitmaps and sends result to resultCh channel.
// Children are merged in batches of size [maxN]
func processDocIDs(maxN int, operator filters.Operator, dbmCh <-chan *docBitmap, resultCh chan<- *docBitmap) {
	dbms := make([]*docBitmap, 0, maxN)
	var result *docBitmap
	defer func() {
		if result == nil {
			empty := newDocBitmap()
			result = &empty
		}
		resultCh <- result
	}()

	for dbm := range dbmCh {
		dbms = append(dbms, dbm)
		// merge if [maxN] children is received
		if len(dbms) == maxN {
			dbms = mergeDocIDs(operator, dbms)
		}
	}
	// merge remaining children
	if dbms = mergeDocIDs(operator, dbms); len(dbms) > 0 {
		// merged result is first element of docBitmaps slice
		result = dbms[0]
	}
}

// mergeDocIDs merges provided docBitmaps using given operator.
// It mutates given docBitmaps slice, by changing its length to 1 and putting
// merge result as first element.
// If slice of size 0 or 1 is provided, it is returned without any change.
// Merge is performed starting from bitmap with most containers for OR operator
// or starting from bitmap with least containers for AND operator.
func mergeDocIDs(operator filters.Operator, dbms []*docBitmap) []*docBitmap {
	if len(dbms) <= 1 {
		return dbms
	}

	var mergeFn func(*sroar.Bitmap, int) *sroar.Bitmap
	if operator == filters.OperatorOr {
		// biggest to smallest, so smaller bitmaps are merged into biggest one,
		// minimising chance of expanding destination bitmap (memory allocations)
		slices.SortFunc(dbms, func(dbma, dbmb *docBitmap) int {
			return -dbma.docIDs.CompareNumKeys(dbmb.docIDs)
		})
		mergeFn = dbms[0].docIDs.OrConc
	} else {
		// smallest to biggest, so data is removed from smallest bitmap
		// allowing bigger bitmaps to be garbage collected asap
		slices.SortFunc(dbms, func(dbma, dbmb *docBitmap) int {
			return dbma.docIDs.CompareNumKeys(dbmb.docIDs)
		})
		mergeFn = dbms[0].docIDs.AndConc
	}

	for i := 1; i < len(dbms); i++ {
		mergeFn(dbms[i].docIDs, concurrency.SROAR_MERGE)
		// release resources of docBitmaps merged into 1st one
		dbms[i].release()
	}

	return dbms[:1]
}

func (pv *propValuePair) fetchDocIDs(ctx context.Context, s *Searcher, limit int) (*docBitmap, error) {
	// TODO text_rbm_inverted_index find better way check whether prop len
	if strings.HasSuffix(pv.prop, filters.InternalPropertyLength) &&
		!pv.Class.InvertedIndexConfig.IndexPropertyLength {
		return nil, errors.Errorf("Property length must be indexed to be filterable! add `IndexPropertyLength: true` to the invertedIndexConfig in %v.  Geo-coordinates, phone numbers and data blobs are not supported by property length.", pv.Class.Class)
	}

	if pv.operator == filters.OperatorIsNull && !pv.Class.InvertedIndexConfig.IndexNullState {
		return nil, errors.Errorf("Nullstate must be indexed to be filterable! Add `indexNullState: true` to the invertedIndexConfig")
	}

	if (pv.prop == filters.InternalPropCreationTimeUnix ||
		pv.prop == filters.InternalPropLastUpdateTimeUnix) &&
		!pv.Class.InvertedIndexConfig.IndexTimestamps {
		return nil, errors.Errorf("Timestamps must be indexed to be filterable! Add `IndexTimestamps: true` to the InvertedIndexConfig in %v", pv.Class.Class)
	}

	bucketName := pv.getBucketName()
	if bucketName == "" {
		return nil, errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
	}

	b := s.store.Bucket(bucketName)

	// TODO:  I think we can delete this check entirely.  The bucket will never be nill, and routines should now check if their particular feature is active in the schema.  However, not all those routines have checks yet.
	if b == nil && pv.operator != filters.OperatorWithinGeoRange {
		// a nil bucket is ok for a WithinGeoRange filter, as this query is not
		// served by the inverted index, but propagated to a secondary index in
		// .docPointers()
		return nil, errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
	}

	dbm, err := s.docBitmap(ctx, b, limit, pv)
	if err != nil {
		return nil, err
	}
	return &dbm, nil
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
