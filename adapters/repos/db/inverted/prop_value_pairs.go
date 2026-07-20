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
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
)

// maxBatchViewHoldKeys caps how many keys share one lsmkv.BucketConsistentView.
// Larger ranges are split into sequential slabs, each acquiring its own view,
// so a held view never stalls compaction cleanup for longer than one slab.
const maxBatchViewHoldKeys = 500

type propValuePair struct {
	prop     string
	operator filters.Operator

	// set for all values that can be served by an inverted index, i.e. anything
	// that's not a geoRange
	value []byte

	// only set if operator=OperatorWithinGeoRange, as that cannot be served by a
	// byte value from an inverted index
	valueGeoRange      *filters.GeoRange
	children           []*propValuePair
	hasFilterableIndex bool
	hasSearchableIndex bool
	hasRangeableIndex  bool
	nested             nestedInfo
	Class              *models.Class // The schema
}

func newPropValuePair(class *models.Class) (*propValuePair, error) {
	if class == nil {
		return nil, errors.Errorf("class must not be nil")
	}
	return &propValuePair{Class: class}, nil
}

func (pv *propValuePair) resolveDocIDs(ctx context.Context, s *Searcher, limit int) (*docBitmap, error) {
	if err := ctxExpired(ctx); err != nil {
		return nil, err
	}

	// Correlated nested AND created during extraction: all children target the
	// same root property (stored in pv.prop) and require same-element semantics.
	if pv.nested.isWithinRootSubtree {
		return pv.resolveNestedSubtree(ctx, s)
	}

	// Nested ContainsNone is a first-class operator (first-class-operator approach) with the
	// scalar-array path on pv.nested.relPath. Dispatch BEFORE the isNested
	// check because the ContainsNone wrapper itself is not a leaf — its
	// children are the per-value pvps.
	if pv.operator == filters.ContainsNone && pv.nested.relPath != "" {
		return pv.fetchNestedContainsNone(ctx, s)
	}

	// All nested-specific dispatch: IsNull, value filters, and correlated AND
	// are all routed here so nested logic stays out of the flat fetch path.
	if pv.nested.isNested {
		switch {
		case pv.operator == filters.OperatorIsNull:
			return pv.fetchNestedIsNull(ctx, s)
		case pv.operator.OnValue():
			return pv.fetchNestedDocIDs(ctx, s)
		}
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
			return nil, fmt.Errorf("too many children for operator %q. Expected 1, given %d", pv.operator.Name(), ln)
		}

	default:
		return nil, fmt.Errorf("unsupported operator: %s", pv.operator.Name())
	}
}

// chunkBounds splits [0, n) into numChunks contiguous, roughly equal ranges
// (remainder distributed to the leading chunks); numChunks is clamped to [1, n].
func chunkBounds(n, numChunks int) [][2]int {
	if numChunks < 1 {
		numChunks = 1
	}
	if numChunks > n {
		numChunks = n
	}
	bounds := make([][2]int, 0, numChunks)
	base, rem := n/numChunks, n%numChunks
	start := 0
	for i := 0; i < numChunks; i++ {
		size := base
		if i < rem {
			size++
		}
		end := start + size
		bounds = append(bounds, [2]int{start, end})
		start = end
	}
	return bounds
}

// flatEqualChildrenBucket returns the shared lsmkv.Bucket if every child of
// pv is a non-nested OperatorEqual leaf against the same roaring-set bucket
// (the shape ContainsAny/ContainsAll desugar into), or nil otherwise. Batch
// view acquisition is only safe when every read targets the same bucket.
func (pv *propValuePair) flatEqualChildrenBucket(s *Searcher) *lsmkv.Bucket {
	if len(pv.children) == 0 {
		return nil
	}
	first := pv.children[0]
	if first.operator != filters.OperatorEqual || first.nested.isNested {
		return nil
	}
	bucketName := first.getBucketName()
	if bucketName == "" {
		return nil
	}
	b := s.store.Bucket(bucketName)
	if b == nil || b.Strategy() != lsmkv.StrategyRoaringSet {
		return nil
	}
	for _, child := range pv.children[1:] {
		if child.operator != filters.OperatorEqual || child.nested.isNested || child.getBucketName() != bucketName {
			return nil
		}
	}
	return b
}

// resolveFlatEqualSlab resolves pv.children[start:end] (caller-bounded to
// <=maxBatchViewHoldKeys) sequentially under one shared
// lsmkv.BucketConsistentView instead of one view per value.
func resolveFlatEqualSlab(ctx context.Context, s *Searcher, pv *propValuePair, bucket *lsmkv.Bucket,
	start, end, limit, mergeConc int,
) (*docBitmap, error) {
	view := bucket.GetConsistentView()
	defer view.ReleaseView()
	viewCtx := lsmkv.ContextWithConsistentView(ctx, view)

	var result *docBitmap
	for i := start; i < end; i++ {
		dbm, err := pv.children[i].resolveDocIDs(viewCtx, s, limit)
		if err != nil {
			if result != nil {
				result.release()
			}
			return nil, errors.Wrapf(err, "nested child %d", i)
		}
		if result == nil {
			result = dbm
		} else {
			result = mergeBitmapsAndOrWithDenyList(result, dbm, pv.operator, mergeConc)
		}
	}
	return result, nil
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

	// merge runs alongside child resolution, which already splits the budget
	// across children: deliberate mild overshoot (mirrors roaringsetrange/reader.go).
	mergeConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)

	// merge subresults in separate goroutine using dbmCh (in) and resultCh (out)
	enterrors.GoWrapper(func() {
		processDocIDs(maxN, pv.operator, dbmCh, resultCh, mergeConc)
	}, s.logger)

	// non-nil only for the ContainsAny/ContainsAll flat-equal shape; enables
	// batch view acquisition below instead of one view per value.
	batchViewBucket := pv.flatEqualChildrenBucket(s)

	outerConcurrencyLimit := concurrency.BudgetFromCtx(ctx, concurrency.GOMAXPROCS)
	if outerConcurrencyLimit <= 1 {
		if batchViewBucket != nil {
			// resolve docIDs sequentially in main goroutine, batching the
			// consistent view across <=maxBatchViewHoldKeys-key slabs
			for slabStart := 0; slabStart < len(pv.children); slabStart += maxBatchViewHoldKeys {
				slabEnd := min(slabStart+maxBatchViewHoldKeys, len(pv.children))
				dbm, err2 := resolveFlatEqualSlab(ctx, s, pv, batchViewBucket, slabStart, slabEnd, limit, mergeConc)
				if err2 != nil {
					err = err2
					break
				}
				if dbm != nil {
					dbmCh <- dbm
				}
			}
		} else {
			// resolve docIDs sequentially in main goroutine
			for i, child := range pv.children {
				dbm, err2 := child.resolveDocIDs(ctx, s, limit)
				if err2 != nil {
					// break on first error
					err = errors.Wrapf(err2, "nested child %d", i)
					break
				}
				dbmCh <- dbm
			}
		}
	} else {
		// Resolve docIDs in parallel, chunked to at most outerConcurrencyLimit-1
		// goroutines instead of one goroutine per child, which previously scaled
		// goroutine/channel-send count with N unbounded across requests (GH 12242).
		numChunks := min(len(pv.children), outerConcurrencyLimit-1)
		if numChunks < 1 {
			numChunks = 1
		}
		chunks := chunkBounds(len(pv.children), numChunks)

		// collect all errors from goroutines (not only 1st one)
		ec := errorcompounder.NewSafe()
		// use error group's context to skip remaining children after 1st error
		eg, gctx := enterrors.NewErrorGroupWithContextWrapper(s.logger, ctx)
		eg.SetLimit(outerConcurrencyLimit - 1)

		for _, bounds := range chunks {
			start, end := bounds[0], bounds[1]
			eg.Go(func() error {
				if batchViewBucket != nil {
					// process this chunk's range in <=maxBatchViewHoldKeys slabs,
					// each under one shared consistent view
					var chunkResult *docBitmap
					for slabStart := start; slabStart < end; slabStart += maxBatchViewHoldKeys {
						if err := gctx.Err(); err != nil {
							// some chunk failed, skip remaining slabs
							if chunkResult != nil {
								chunkResult.release()
							}
							return nil
						}
						slabEnd := min(slabStart+maxBatchViewHoldKeys, end)
						childCtx := concurrency.ContextWithFractionalBudget(ctx, numChunks, concurrency.GOMAXPROCS)
						slabResult, err := resolveFlatEqualSlab(childCtx, s, pv, batchViewBucket, slabStart, slabEnd, limit, mergeConc)
						if err != nil {
							ec.Add(err)
							if chunkResult != nil {
								chunkResult.release()
							}
							return err
						}
						if slabResult == nil {
							continue
						}
						if chunkResult == nil {
							chunkResult = slabResult
						} else {
							chunkResult = mergeBitmapsAndOrWithDenyList(chunkResult, slabResult, pv.operator, mergeConc)
						}
					}
					if chunkResult != nil {
						dbmCh <- chunkResult
					}
					return nil
				}

				var chunkResult *docBitmap
				for i := start; i < end; i++ {
					if err := gctx.Err(); err != nil {
						// some child failed, skip processing
						if chunkResult != nil {
							chunkResult.release()
						}
						return nil
					}

					ctx := concurrency.ContextWithFractionalBudget(ctx, numChunks, concurrency.GOMAXPROCS)
					dbm, err := pv.children[i].resolveDocIDs(ctx, s, limit)
					if err != nil {
						err = errors.Wrapf(err, "nested child %d", i)
						ec.Add(err)
						if chunkResult != nil {
							chunkResult.release()
						}
						return err
					}
					if chunkResult == nil {
						chunkResult = dbm
					} else {
						chunkResult = mergeBitmapsAndOrWithDenyList(chunkResult, dbm, pv.operator, mergeConc)
					}
				}
				if chunkResult != nil {
					dbmCh <- chunkResult
				}
				return nil
			})
			// some child failed, skip remaining chunks
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
func processDocIDs(maxN int, operator filters.Operator, dbmCh <-chan *docBitmap, resultCh chan<- *docBitmap, maxConc int) {
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
			dbms = mergeDocIDs(operator, dbms, maxConc)
		}
	}
	// merge remaining children
	if dbms = mergeDocIDs(operator, dbms, maxConc); len(dbms) > 0 {
		// merged result is first element of docBitmaps slice
		result = dbms[0]
	}
}

func mergeBitmapsAndOrWithDenyList(a, b *docBitmap, operator filters.Operator, maxConc int) *docBitmap {
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
		if (op == filters.OperatorOr && a.docIDs.NumContainers() < b.docIDs.NumContainers()) ||
			(op == filters.OperatorAnd && a.docIDs.NumContainers() > b.docIDs.NumContainers()) {
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
			a.docIDs.OrConc(b.docIDs, maxConc)
		} else {
			swapForEfficiency(filters.OperatorAnd)
			a.docIDs.AndConc(b.docIDs, maxConc)
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
			b.docIDs.AndNotConc(a.docIDs, maxConc)
			a, b = b, a // a=result(allowlist), b=old denylist(released by defer)
		} else {
			a.docIDs.AndNotConc(b.docIDs, maxConc)
		}

	default:
		// Both allowlists.
		swapForEfficiency(operator)
		if operator == filters.OperatorAnd {
			a.docIDs.AndConc(b.docIDs, maxConc)
		} else {
			a.docIDs.OrConc(b.docIDs, maxConc)
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
//
// A pure-allowlist OR batch routes through fastOrMerge's single-pass FastOr
// instead; FastOr has no deny-list algebra, so any other shape falls through
// to the pairwise reduce below.
func mergeDocIDs(operator filters.Operator, dbms []*docBitmap, maxConc int) []*docBitmap {
	if len(dbms) <= 1 {
		return dbms
	}

	if operator == filters.OperatorOr && allAllowLists(dbms) {
		return []*docBitmap{fastOrMerge(dbms)}
	}

	for i := 0; i < len(dbms)-1; i++ {
		dbms[0] = mergeBitmapsAndOrWithDenyList(dbms[0], dbms[i+1], operator, maxConc)
	}

	return dbms[:1]
}

// allAllowLists reports whether every docBitmap in dbms is an allowlist;
// FastOr has no deny-list algebra, so it's only safe when this holds.
func allAllowLists(dbms []*docBitmap) bool {
	for _, dbm := range dbms {
		if dbm.IsDenyList() {
			return false
		}
	}
	return true
}

// fastOrMerge unions every docBitmap in dbms (all confirmed allowlists by
// the caller) in a single sroar.FastOr pass instead of the M-1 pairwise
// OrConc reduce.
//
// Never calls sroar.FastParOr: FastParOr v0.0.15 (bitmap.go:1176-1195) has a
// data race between its launching goroutine's `append` and concurrent
// indexed writes from already-spawned goroutines into the same slice -- a
// bug in the vendored dependency, not this call site. The parallelism it
// would offer is redundant anyway, since every fastOrMerge call already runs
// inside one of resolveDocIDsAndOr's chunk goroutines.
//
// FastOr always allocates a fresh destination bitmap for the >1-input case
// reached here, so every input's buffer can be released immediately after.
func fastOrMerge(dbms []*docBitmap) *docBitmap {
	bitmaps := make([]*sroar.Bitmap, len(dbms))
	for i, dbm := range dbms {
		bitmaps[i] = dbm.docIDs
	}

	merged := sroar.FastOr(bitmaps...)

	for _, dbm := range dbms {
		dbm.release()
	}

	return &docBitmap{docIDs: merged, isDenyList: false, release: func() {}}
}

// fetchDocIDs resolves a value filter on a flat (non-nested) property.
func (pv *propValuePair) fetchDocIDs(ctx context.Context, s *Searcher, limit int) (*docBitmap, error) {
	return pv.readFromBucket(ctx, s, limit)
}

// readFromBucket is the shared low-level implementation for all fetch methods.
func (pv *propValuePair) readFromBucket(ctx context.Context, s *Searcher, limit int) (*docBitmap, error) {
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
	if pv.nested.isNested {
		if pv.hasFilterableIndex {
			return helpers.BucketNestedFromPropNameLSM(pv.prop)
		}
		return ""
	}

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
