//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"context"
	"fmt"
	"strings"

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

	ln := len(pv.children)
	switch pv.operator {
	case filters.OperatorAnd, filters.OperatorOr:
		switch ln {
		case 0:
			return nil, fmt.Errorf("no children for operator %q", pv.operator.Name())
		case 1:
			return pv.children[0].resolveDocIDs(ctx, s, limit)
		default:
			return pv.resolveDocIDsAndOr(ctx, s)
		}

	case filters.OperatorNot:
		switch ln {
		case 0:
			return nil, fmt.Errorf("no children for operator %q", pv.operator.Name())
		case 1:
			return pv.resolveDocIDsNot(ctx, s)
		default:
			return nil, fmt.Errorf("too many children for operator %q. Expected 1, given %q", pv.operator.Name(), ln)
		}

	default:
		return nil, fmt.Errorf("unsupported operator: %s", pv.operator.Name())
	}
}

func (pv *propValuePair) resolveDocIDsAndOr(ctx context.Context, s *Searcher) (*docBitmap, error) {
	// Explicitly set the limit to 0 (=unlimited) as this is a nested filter,
	// otherwise we run into situations where each subfilter on their own
	// runs into the limit, possibly yielding in "less than limit" results
	// after merging.
	limit := 0

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
		return nil, fmt.Errorf("nested AND/OR query: %w", err)
	}
	return result, nil
}

func (pv *propValuePair) resolveDocIDsNot(ctx context.Context, s *Searcher) (*docBitmap, error) {
	// Explicitly set the limit to 0 (=unlimited) as this is a nested filter,
	// otherwise we run into situations where each subfilter on their own
	// runs into the limit, possibly yielding in "less than limit" results
	// after merging.
	limit := 0

	dbm, err := pv.children[0].resolveDocIDs(ctx, s, limit)
	if err != nil {
		return nil, fmt.Errorf("nested NOT query: %w", err)
	}

	dbm.isDenyList = !dbm.isDenyList // invert allow/deny list
	return dbm, nil
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

func mergeBitmapsAndOrWithDenyList(a, b *docBitmap, operator filters.Operator) *docBitmap {
	//	- both A and B are denylists
	//	  !A  or !B -> !(A and B) -> denylist A.And(B)
	//	  !A and !B -> !(A  or B) -> denylist A.Or(B)
	//
	//	- one of A and B is a denylist, the other an allowlist
	//	  !A and B ->   B and !A  -> allowlist B.AndNot(A)
	//	  !A  or B -> !(A and !B) ->  denylist A.AndNot(B)
	//
	//	  (done by swapping A and B in the code below to avoid code duplication)
	//	  A and !B -> allowlist A.AndNot(B): (same as !B and A)
	//	  A  or !B -> !B or A -> !(B and !A) -> denylist B.AndNot(A): (same as !A or B)
	//
	//	- base case: both A and B are allowlists
	//	  A or B -> allowlist A.Or(B)
	//	  A and B -> allowlist A.And(B)
	//
	//	- for completeness, here are the remaining combinations, used as part of the Not and NotEqual operators:
	//	  A -> allowlist A
	//	  !A -> denylist A
	//	  !!A -> allowlist A

	// clean up resources of bitmap that is not used in the final result.
	defer func() {
		if b != nil {
			b.release()
		}
	}()

	// swapForEfficiency puts the larger bitmap in `a` for Or (fewer union ops),
	// or the smaller bitmap in `a` for And (fewer intersection ops).
	swapForEfficiency := func(op filters.Operator) {
		if (op == filters.OperatorOr && a.docIDs.CompareNumKeys(b.docIDs) < 0) ||
			(op == filters.OperatorAnd && a.docIDs.CompareNumKeys(b.docIDs) > 0) {
			a, b = b, a
		}
	}

	switch {
	case a.IsDenyList() && b.IsDenyList():
		// Both denylists — apply De Morgan: invert the operation.
		// !A and !B -> !(A or B)  -> denylist A.Or(B)
		// !A  or !B -> !(A and B) -> denylist A.And(B)
		if operator == filters.OperatorAnd {
			swapForEfficiency(filters.OperatorOr)
			a.docIDs.OrConc(b.docIDs, concurrency.SROAR_MERGE)
		} else {
			swapForEfficiency(filters.OperatorAnd)
			a.docIDs.AndConc(b.docIDs, concurrency.SROAR_MERGE)
		}

	case a.IsDenyList() || b.IsDenyList():
		// Mixed: one denylist, one allowlist.
		// Normalise so that a=denylist, b=allowlist.
		if b.IsDenyList() {
			a, b = b, a
		}
		// !A and B -> allowlist B.AndNot(A)
		// !A  or B -> denylist  A.AndNot(B)
		if operator == filters.OperatorAnd {
			b.docIDs.AndNotConc(a.docIDs, concurrency.SROAR_MERGE)
			a, b = b, a // a=result(allowlist), b=old denylist(released by defer)
		} else {
			a.docIDs.AndNotConc(b.docIDs, concurrency.SROAR_MERGE)
		}

	default:
		// Both allowlists.
		swapForEfficiency(operator)
		if operator == filters.OperatorAnd {
			a.docIDs.AndConc(b.docIDs, concurrency.SROAR_MERGE)
		} else {
			a.docIDs.OrConc(b.docIDs, concurrency.SROAR_MERGE)
		}
	}
	return a
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

	for i := 0; i < len(dbms)-1; i++ {
		dbms[0] = mergeBitmapsAndOrWithDenyList(dbms[0], dbms[i+1], operator)
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
