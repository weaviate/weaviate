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
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/weaviate/weaviate/entities/concurrency"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/sorter"
	"github.com/weaviate/weaviate/entities/additional"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/filters"
	filnested "github.com/weaviate/weaviate/entities/filters/nested"
	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/tokenizer"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// IsRangeableLocallyReady returns true when this shard's local rangeable
// bucket for the given property is fully populated and safe to query.
// During an enable-rangeable migration the cluster-wide schema flag
// `IndexRangeFilters` can flip to true as soon as the first replica
// completes its swap, but other replicas may still be mid-iteration
// with an empty PreReindexHook-created rangeable bucket — so a query
// using the rangeable bucket on those replicas would return partial /
// zero counts. When this callback returns false, the filter resolver
// treats the property as if it had no rangeable index for THIS shard
// only and falls back to the filterable bucket walk (slow but correct).
// Returns true for properties that have no in-flight migration on disk
// — i.e. either never migrated (native rangeable from collection
// creation) or already-completed migrations.
type IsRangeableLocallyReady func(propName string) bool

type Searcher struct {
	logger                  logrus.FieldLogger
	store                   *lsmkv.Store
	getClass                func(string) *models.Class
	classSearcher           ClassSearcher // to allow recursive searches on ref-props
	propIndices             propertyspecific.Indices
	stopwordProvider        *stopwords.Provider
	shardVersion            uint16
	isFallbackToSearchable  IsFallbackToSearchable
	isRangeableLocallyReady IsRangeableLocallyReady
	tenant                  string
	// nestedCrossRefLimit limits the number of nested cross refs returned for a query
	nestedCrossRefLimit int64
	bitmapFactory       *roaringset.BitmapFactory
	nestedBitmapOps     *invnested.BitmapOps
	// tokResolver, when non-nil, overrides prop.Tokenization on query
	// input analysis. Used by the per-shard tokenization overlay to
	// keep query tokenization aligned with the bucket content during
	// the FINALIZING window of a change-tokenization migration. Nil =
	// use prop.Tokenization directly (tests and callers with no
	// in-flight migration).
	tokResolver TokenizationResolver
}

// WithTokenizationResolver attaches a [TokenizationResolver] used by query
// input analysis to consult a per-shard tokenization overlay before
// falling back to the schema-stored `prop.Tokenization`. Returns the
// receiver for fluent chaining at construction sites.
//
// Production callers wire `Shard.TokenizationFor` here so a
// change-tokenization migration's FINALIZING-window overlay routes
// query input to the post-swap tokenization. See [TokenizationResolver]
// for the misalignment this closes.
//
// Pass nil (the default) to use the schema-stored value directly.
func (s *Searcher) WithTokenizationResolver(r TokenizationResolver) *Searcher {
	s.tokResolver = r
	return s
}

// hasUsableRangeableIndex combines the schema-level [HasRangeableIndex] check
// with the runtime [Searcher.isRangeableLocallyReady] gate. Always pair them
// at query-extraction sites: schema may say the rangeable bucket exists, but
// a swap-recovery window on this node may have it offline. Folding the two
// removes the per-call-site reminder.
func (s *Searcher) hasUsableRangeableIndex(prop *models.Property) bool {
	return HasRangeableIndex(prop) && s.isRangeableLocallyReady(prop.Name)
}

var ErrOnlyStopwords = fmt.Errorf("invalid search term, only stopwords provided. " +
	"Stopwords can be configured in class.invertedIndexConfig.stopwords")

func NewSearcher(logger logrus.FieldLogger, store *lsmkv.Store,
	getClass func(string) *models.Class, propIndices propertyspecific.Indices,
	classSearcher ClassSearcher, stopwordProvider *stopwords.Provider,
	shardVersion uint16, isFallbackToSearchable IsFallbackToSearchable,
	isRangeableLocallyReady IsRangeableLocallyReady,
	tenant string, nestedCrossRefLimit int64, bitmapFactory *roaringset.BitmapFactory,
) *Searcher {
	if isRangeableLocallyReady == nil {
		// Default: always ready. Callers that don't know about the per-shard
		// rangeable-readiness state (e.g. tests without a migration in
		// flight, or the brief gap between Searcher construction and the
		// migration hook being wired) get the historical behavior of
		// trusting the schema flag verbatim.
		isRangeableLocallyReady = func(string) bool { return true }
	}
	return &Searcher{
		logger:                  logger,
		store:                   store,
		getClass:                getClass,
		propIndices:             propIndices,
		classSearcher:           classSearcher,
		stopwordProvider:        stopwordProvider,
		shardVersion:            shardVersion,
		isFallbackToSearchable:  isFallbackToSearchable,
		isRangeableLocallyReady: isRangeableLocallyReady,
		tenant:                  tenant,
		nestedCrossRefLimit:     nestedCrossRefLimit,
		bitmapFactory:           bitmapFactory,
		nestedBitmapOps:         invnested.NewBitmapOps(bitmapFactory.BufPool()),
	}
}

// Objects returns a list of full objects
func (s *Searcher) Objects(ctx context.Context, limit int,
	filter *filters.LocalFilter, sort []filters.Sort, additional additional.Properties,
	className schema.ClassName, properties []string,
	disableInvertedSorter *runtime.DynamicValue[bool],
) ([]*storobj.Object, error) {
	ctx = concurrency.CtxWithBudget(ctx, concurrency.TimesGOMAXPROCS(2))
	beforeFilters := time.Now()
	allowList, err := s.docIDs(ctx, filter, className, limit)
	if err != nil {
		return nil, err
	}
	defer allowList.Close()
	helpers.AnnotateSlowQueryLog(ctx, "build_allow_list_took", time.Since(beforeFilters))
	helpers.AnnotateSlowQueryLog(ctx, "allow_list_doc_ids_count", allowList.Len())

	var it docIDsIterator
	if len(sort) > 0 {
		beforeSort := time.Now()
		docIDs, err := s.sort(ctx, limit, sort, allowList, className, disableInvertedSorter)
		if err != nil {
			return nil, fmt.Errorf("sort doc ids: %w", err)
		}
		helpers.AnnotateSlowQueryLog(ctx, "sort_doc_ids_took", time.Since(beforeSort))
		it = newSliceDocIDsIterator(docIDs)
	} else {
		it = allowList.Iterator()
		defer it.Stop()
	}

	beforeObjects := time.Now()
	defer func() {
		helpers.AnnotateSlowQueryLog(ctx, "objects_by_doc_ids_took", time.Since(beforeObjects))
	}()
	return s.objectsByDocID(ctx, it, additional, limit, properties)
}

func (s *Searcher) sort(ctx context.Context, limit int, sort []filters.Sort,
	docIDs helpers.AllowList, className schema.ClassName,
	disableInvertedSorter *runtime.DynamicValue[bool],
) ([]uint64, error) {
	lsmSorter, err := sorter.NewLSMSorter(s.store, s.getClass, className, disableInvertedSorter)
	if err != nil {
		return nil, err
	}
	return lsmSorter.SortDocIDs(ctx, limit, sort, docIDs)
}

func (s *Searcher) objectsByDocID(ctx context.Context, it docIDsIterator,
	additional additional.Properties, limit int, properties []string,
) ([]*storobj.Object, error) {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)

	if bucket == nil {
		return nil, fmt.Errorf("objects bucket not found")
	}

	className, err := bucket.ClassName()
	if err != nil {
		return nil, fmt.Errorf("getting objects bucket class name: %w", err)
	}

	// Prevent unbounded iteration
	if limit == 0 {
		limit = int(config.DefaultQueryMaximumResults)
	}
	outlen := it.Len()
	if outlen > limit {
		outlen = limit
	}

	out := make([]*storobj.Object, outlen)
	docIDBytes := make([]byte, 8)

	propertyPaths := make([][]string, len(properties))
	for j := range properties {
		propertyPaths[j] = []string{properties[j]}
	}

	props := &storobj.PropertyExtraction{
		PropertyPaths: propertyPaths,
	}

	deletedIds := sroar.NewBitmap()
	deletedCount := 0
	handleDeletedId := func(id uint64) {
		if deletedIds.Set(id) {
			if deletedCount++; deletedCount >= 1024 {
				s.bitmapFactory.Remove(deletedIds)
				deletedIds = sroar.NewBitmap().CloneToBuf(deletedIds.ToBuffer())
				deletedCount = 0
			}
		}
	}
	defer func() {
		if deletedCount > 0 {
			s.bitmapFactory.Remove(deletedIds)
		}
	}()

	i := 0
	loop := 0
	for docID, ok := it.Next(); ok; docID, ok = it.Next() {
		if loop%1000 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		loop++

		binary.LittleEndian.PutUint64(docIDBytes, docID)
		res, err := bucket.GetBySecondary(ctx, 0, docIDBytes)
		if err != nil {
			return nil, err
		}

		if res == nil {
			handleDeletedId(docID)
			continue
		}

		var unmarshalled *storobj.Object
		if additional.ReferenceQuery {
			unmarshalled, err = storobj.FromBinaryUUIDOnlyDisk(res, className)
		} else {
			unmarshalled, err = storobj.FromBinaryOptionalDisk(res, className, additional, props)
		}
		if err != nil {
			return nil, fmt.Errorf("unmarshal data object at position %d: %w", i, err)
		}

		out[i] = unmarshalled
		i++

		if i >= limit {
			break
		}
	}

	return out[:i], nil
}

// DocIDs is similar to Objects, but does not actually resolve the docIDs to
// full objects. Instead it returns the pure object id pointers. They can then
// be used in a secondary index (e.g. vector index)
//
// DocID queries does not contain a limit by design, as we won't know if the limit
// wouldn't remove the item that is most important for the follow up query.
// Imagine the user sets the limit to 1 and the follow-up is a vector search.
// If we already limited the allowList to 1, the vector search would be
// pointless, as only the first element would be allowed, regardless of which
// had the shortest distance
func (s *Searcher) DocIDs(ctx context.Context, filter *filters.LocalFilter,
	additional additional.Properties, className schema.ClassName,
) (helpers.AllowList, error) {
	ctx = concurrency.CtxWithBudget(ctx, concurrency.GOMAXPROCSx2)
	return s.docIDs(ctx, filter, className, 0)
}

func (s *Searcher) DocIDsLimited(ctx context.Context, filter *filters.LocalFilter,
	additional additional.Properties, className schema.ClassName, limit int,
) (helpers.AllowList, error) {
	ctx = concurrency.CtxWithBudget(ctx, concurrency.GOMAXPROCSx2)
	return s.docIDs(ctx, filter, className, max(0, limit))
}

func (s *Searcher) docIDs(ctx context.Context, filter *filters.LocalFilter,
	className schema.ClassName, limit int,
) (helpers.AllowList, error) {
	pv, err := s.extractPropValuePair(ctx, filter.Root, className)
	if err != nil {
		return nil, err
	}

	beforeResolve := time.Now()
	helpers.AnnotateSlowQueryLog(ctx, "build_allow_list_resolve_len", len(pv.children))
	dbm, err := pv.resolveDocIDs(ctx, s, limit)
	if err != nil {
		return nil, fmt.Errorf("resolve doc ids for prop/value pair: %w", err)
	}
	helpers.AnnotateSlowQueryLog(ctx, "build_allow_list_resolve_took", time.Since(beforeResolve))

	// invert once at the end if it's a deny list, to avoid multiple inversions in case of nested ORs
	if dbm.isDenyList {
		universe, universeRelease := s.bitmapFactory.GetBitmap()
		universe.AndNotConc(dbm.docIDs, concurrency.SROAR_MERGE)
		dbm.release()
		return helpers.NewAllowListCloseableFromBitmap(universe, universeRelease), nil
	}

	return helpers.NewAllowListCloseableFromBitmap(dbm.docIDs, dbm.release), nil
}

// extractPropValuePair is the entry point. It delegates type-dispatched
// construction of the propValuePair tree to buildPropValuePair, then
// runs a single post-order pass over the tree to group same-root
// subtrees at every AND node. Recursive calls from
// extractPropValuePairs go directly to buildPropValuePair to avoid
// re-grouping during construction.
func (s *Searcher) extractPropValuePair(
	ctx context.Context, filter *filters.Clause, className schema.ClassName,
) (*propValuePair, error) {
	class := s.getClass(className.String())
	if class == nil {
		return nil, fmt.Errorf("class %q not found", className)
	}
	out, err := s.buildPropValuePair(ctx, filter, className, class)
	if err != nil {
		return nil, err
	}
	return groupNestedSubtrees(out, class), nil
}

// groupNestedSubtrees walks pv's tree post-order and applies the
// same-root grouping rule of groupNestedByProp at every AND / OR node
// it finds, and additionally marks NOT-of-nested-operand as
// isWithinRootSubtree so the scope-aware planner inverts the NOT at
// the operand's natural LCA (top-level NOT wrapping). Leaf nodes pass through;
// their children are still walked so AND / OR / NOT nodes deeper in
// the tree get processed. Pre-marked wrappers (isWithinRootSubtree=
// true, e.g. tokenization wrappers from buildNestedTextFilterPair)
// are skipped — their children are already the leaves of one same-
// root subtree and need no further grouping.
//
// When grouping collapses every child of an AND / OR into a single
// same-root wrapper, the outer operator is promoted in place: its own
// isWithinRootSubtree flag and prop are set, and the redundant single-
// child wrapping level is elided. Mixed-root nodes keep the per-group
// wrappers as separate children.
//
// NOT-of-IsNull never reaches here — buildPropValuePair rewrites it to
// its DeMorgan dual (singleton-NOT/OR wrapping) so the NOT cancels at
// extraction time. Any NOT seen here has a non-IsNull operand.
func groupNestedSubtrees(pv *propValuePair, class *models.Class) *propValuePair {
	if pv == nil || len(pv.children) == 0 {
		return pv
	}
	for i := range pv.children {
		pv.children[i] = groupNestedSubtrees(pv.children[i], class)
	}
	if pv.nested.isWithinRootSubtree {
		return pv
	}
	switch pv.operator {
	case filters.OperatorAnd, filters.OperatorOr, filters.ContainsAll, filters.ContainsAny:
		// ContainsAll / ContainsAny are AND / OR aliases on a nested path
		// (first-class-operator approach — operator identity preserved by extractContains).
		grouped := groupNestedByProp(pv.children, class, pv.operator)
		if len(grouped) == 1 && grouped[0].nested.isWithinRootSubtree {
			// Collapse: every child landed in one same-root wrapper.
			// Promote pv to be that wrapper instead of holding it as a
			// useless single-child outer node. pv keeps its operator
			// (AND/OR or ContainsAll/Any), so the planner sees the right shape.
			w := grouped[0]
			pv.nested.isWithinRootSubtree = true
			pv.prop = w.prop
			pv.children = w.children
			return pv
		}
		pv.children = grouped
		return pv
	case filters.OperatorNot:
		if len(pv.children) != 1 {
			return pv
		}
		operand := pv.children[0]
		operandRoot := nestedRootProp(operand)
		if operandRoot == "" {
			return pv
		}
		pv.nested.isWithinRootSubtree = true
		pv.prop = operandRoot
		return pv
	default:
		return pv
	}
}

// flipNestedIsNull returns a copy of pv with its IsNull boolean byte
// inverted (0x01 ↔ 0x00). Used by buildPropValuePair to apply the
// DeMorgan rewrite NOT(IsNull=v) → IsNull=!v on nested IsNull leaves.
// pv must satisfy operator==OperatorIsNull and nested.isNested==true.
//
// The value slice is copied to avoid mutating any shared underlying
// array; pv itself is shallow-cloned for the same reason (the original
// pv may still be referenced by other parts of the filter tree under
// pathological reuse, although today's extraction does not share).
func flipNestedIsNull(pv *propValuePair) *propValuePair {
	flipped := *pv
	flipped.value = make([]byte, max(1, len(pv.value)))
	copy(flipped.value, pv.value)
	if flipped.value[0] == 0x01 {
		flipped.value[0] = 0x00
	} else {
		flipped.value[0] = 0x01
	}
	return &flipped
}

// buildPropValuePair constructs the propValuePair from filter without
// applying the same-root grouping pass. Used recursively by
// extractPropValuePairs; the top-level extractPropValuePair wrapper
// invokes groupNestedSubtrees once on the final tree.
func (s *Searcher) buildPropValuePair(
	ctx context.Context, filter *filters.Clause, className schema.ClassName, class *models.Class,
) (*propValuePair, error) {
	out, err := newPropValuePair(class)
	if err != nil {
		return nil, fmt.Errorf("new prop value pair: %w", err)
	}
	if filter.Operands != nil {
		// nested filter
		children, err := s.extractPropValuePairs(ctx, filter.Operands, filter.Operator, className)
		if err != nil {
			return nil, err
		}
		// DeMorgan rewrite: NOT(IsNull=v) on a nested
		// path is equivalent to IsNull=!v under Phase 6's scope-aware IsNull
		// semantics (both invert at the operand's natural LCA, so NOT cancels
		// algebraically). Rewriting here eliminates the NOT before grouping
		// and avoids routing IsNull leaves through operator subtrees, which
		// fetchOperatorSubtreeBitmaps does not support.
		if filter.Operator == filters.OperatorNot && len(children) == 1 &&
			children[0].operator == filters.OperatorIsNull && children[0].nested.isNested {
			return flipNestedIsNull(children[0]), nil
		}
		out.children = children
		out.operator = filter.Operator
		return out, nil
	}

	switch filter.Operator {
	case filters.ContainsAll, filters.ContainsAny, filters.ContainsNone:
		return s.extractContains(ctx, filter.On, filter.Value.Type, filter.Value.Value, filter.Operator, class)
	default:
		// proceed
	}

	// on value or non-nested filter
	props := filter.On.Slice()
	// Strip any arr[N] index from the first segment so that "addresses[1].city"
	// correctly resolves to the "addresses" property in the schema.
	// Only the first segment is cleaned here; extractNestedProp receives the
	// original props[0] so that [N] indices on sub-paths are preserved.
	propName := filnested.RootPropName(props[0])

	if s.onInternalProp(propName) {
		return s.extractInternalProp(propName, filter.Value.Type, filter.Value.Value, filter.Operator, class)
	}

	if extractedPropName, ok := schema.IsPropertyLength(propName, 0); ok {
		class := s.getClass(schema.ClassName(className).String())
		if class == nil {
			return nil, fmt.Errorf("could not find class %s in schema", className)
		}

		property, err := schema.GetPropertyByName(class, extractedPropName)
		if err != nil {
			return nil, err
		}
		return s.extractPropertyLength(property, filter.Value.Type, filter.Value.Value, filter.Operator, class)
	}

	property, err := schema.GetPropertyByName(class, propName)
	if err != nil {
		return nil, err
	}

	if _, ok := schema.AsNested(property.DataType); ok {
		// Defensive preview gate. The validator catches this first under the
		// normal request flow; this duplicate guards any code path that
		// reaches the searcher without going through validation.
		if !entcfg.NestedFilteringEnabled() {
			return nil, entcfg.NestedFilteringDisabledError()
		}
		return s.extractNestedProp(filter, props[0], property, class)
	}

	if s.onRefProp(property) && len(props) != 1 {
		return s.extractReferenceFilter(property, filter, class)
	}

	if s.onRefProp(property) && filter.Value.Type == schema.DataTypeInt {
		// ref prop and int type is a special case, the user is looking for the
		// reference count as opposed to the content
		return s.extractReferenceCount(property, filter.Value.Value, filter.Operator, class)
	}

	if filter.Operator == filters.OperatorIsNull {
		return s.extractPropertyNull(property, filter.Value.Type, filter.Value.Value, filter.Operator, class)
	}

	if s.onGeoProp(property) {
		return s.extractGeoFilter(property, filter.Value.Value, filter.Value.Type, filter.Operator, class)
	}

	if s.onUUIDProp(property) {
		return s.extractUUIDFilter(property, filter.Value.Value, filter.Value.Type, filter.Operator, class)
	}

	if s.onTokenizableProp(property) {
		return s.extractTokenizableProp(property, filter.Value.Type, filter.Value.Value, filter.Operator, class)
	}

	return s.extractPrimitiveProp(property, filter.Value.Type, filter.Value.Value, filter.Operator, class)
}

// extractPropValuePairs extracts each operand recursively via
// buildPropValuePair (not the public extractPropValuePair wrapper) so
// the same-root grouping pass runs only once at the outermost call.
// The top-level groupNestedSubtrees pass walks the full tree post-order
// and groups every AND node — including those nested inside OR/NOT —
// so this recursion model doesn't miss any AND nodes.
func (s *Searcher) extractPropValuePairs(ctx context.Context,
	operands []filters.Clause, operator filters.Operator, className schema.ClassName,
) ([]*propValuePair, error) {
	class := s.getClass(className.String())
	if class == nil {
		return nil, fmt.Errorf("class %q not found", className)
	}
	children := make([]*propValuePair, len(operands))
	eg := enterrors.NewErrorGroupWrapper(s.logger)
	outerConcurrencyLimit := concurrency.BudgetFromCtx(ctx, concurrency.GOMAXPROCS)
	eg.SetLimit(outerConcurrencyLimit)

	concurrencyReductionFactor := min(len(operands), outerConcurrencyLimit)

	for i, clause := range operands {
		i, clause := i, clause
		eg.Go(func() error {
			ctx := concurrency.ContextWithFractionalBudget(ctx, concurrencyReductionFactor, concurrency.GOMAXPROCS)
			child, err := s.buildPropValuePair(ctx, &clause, className, class)
			// check for stopword errors on ContainsAny operator only at the end
			if err != nil && errors.Is(err, ErrOnlyStopwords) && operator == filters.ContainsAny {
				return nil
			}
			if err != nil {
				return fmt.Errorf("nested clause at pos %d: %w", i, err)
			}
			children[i] = child
			return nil
		}, clause)
	}
	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("nested query: %w", err)
	}
	i := 0
	for _, c := range children {
		if c != nil {
			children[i] = c
			i++
		}
	}
	children = children[:i]

	// if all children were stopwords and the operator is ContainsAny,
	// we return the stopword error anyway for consistency with other
	// filter behaviors
	// The logic is, if at least one of the provided terms is a valid
	// search term (i.e., not a stopword), we proceed with the search,
	// as we can still match on that term.
	// However, if all provided terms are stopwords, we return an error
	// for consistency with other filter behaviors.
	//
	// TODO(amourao): the stopword logic should be rethought globally,
	// as returning errors for specific filter values may generate unexpected
	// results for the end user
	if len(children) == 0 && operator == filters.ContainsAny {
		return nil, ErrOnlyStopwords
	}
	return children, nil
}

func (s *Searcher) extractReferenceFilter(prop *models.Property,
	filter *filters.Clause, class *models.Class,
) (*propValuePair, error) {
	ctx := context.TODO()
	return newRefFilterExtractor(s.logger, s.classSearcher, filter, class, prop, s.tenant, s.nestedCrossRefLimit).
		Do(ctx)
}

func (s *Searcher) extractPrimitiveProp(prop *models.Property, propType schema.DataType,
	value interface{}, operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	var extractValueFn func(in interface{}) ([]byte, error)
	switch propType {
	case schema.DataTypeBoolean:
		extractValueFn = s.extractBoolValue
	case schema.DataTypeInt:
		extractValueFn = s.extractIntValue
	case schema.DataTypeNumber:
		extractValueFn = s.extractNumberValue
	case schema.DataTypeDate:
		extractValueFn = s.extractDateValue
	case "":
		return nil, fmt.Errorf("data type cannot be empty")
	default:
		return nil, fmt.Errorf("data type %q not supported in query", propType)
	}

	byteValue, err := extractValueFn(value)
	if err != nil {
		return nil, err
	}

	hasFilterableIndex := HasFilterableIndex(prop)
	hasSearchableIndex := HasSearchableIndex(prop)
	hasRangeableIndex := s.hasUsableRangeableIndex(prop)

	if !hasFilterableIndex && !hasSearchableIndex && !hasRangeableIndex {
		return nil, inverted.NewMissingFilterableIndexError(prop.Name)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               prop.Name,
		operator:           operator,
		hasFilterableIndex: hasFilterableIndex,
		hasSearchableIndex: hasSearchableIndex,
		hasRangeableIndex:  hasRangeableIndex,
		Class:              class,
	}, nil
}

func (s *Searcher) extractReferenceCount(prop *models.Property, value interface{},
	operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	byteValue, err := s.extractIntCountValue(value)
	if err != nil {
		return nil, err
	}

	hasFilterableIndex := HasFilterableIndexMetaCount && HasAnyInvertedIndex(prop)
	hasSearchableIndex := HasSearchableIndexMetaCount && HasAnyInvertedIndex(prop)
	hasRangeableIndex := HasRangeableIndexMetaCount && HasAnyInvertedIndex(prop)

	if !hasFilterableIndex && !hasSearchableIndex && !hasRangeableIndex {
		return nil, inverted.NewMissingFilterableMetaCountIndexError(prop.Name)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               helpers.MetaCountProp(prop.Name),
		operator:           operator,
		hasFilterableIndex: hasFilterableIndex,
		hasSearchableIndex: hasSearchableIndex,
		hasRangeableIndex:  hasRangeableIndex,
		Class:              class,
	}, nil
}

func (s *Searcher) extractGeoFilter(prop *models.Property, value interface{},
	valueType schema.DataType, operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	if valueType != schema.DataTypeGeoCoordinates {
		return nil, fmt.Errorf("prop %q is of type geoCoordinates, it can only"+
			"be used with geoRange filters", prop.Name)
	}

	parsed := value.(filters.GeoRange)

	return &propValuePair{
		value:              nil, // not going to be served by an inverted index
		valueGeoRange:      &parsed,
		prop:               prop.Name,
		operator:           operator,
		hasFilterableIndex: HasFilterableIndex(prop),
		hasSearchableIndex: HasSearchableIndex(prop),
		hasRangeableIndex:  s.hasUsableRangeableIndex(prop),
		Class:              class,
	}, nil
}

func (s *Searcher) extractUUIDFilter(prop *models.Property, value interface{},
	valueType schema.DataType, operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	if valueType != schema.DataTypeText {
		return nil, fmt.Errorf("prop %q is of type uuid, the uuid to filter "+
			"on must be specified as a string (e.g. valueText:<uuid>)", prop.Name)
	}

	byteValue, err := s.extractUUIDValue(value)
	if err != nil {
		return nil, err
	}

	hasFilterableIndex := HasFilterableIndex(prop)
	hasSearchableIndex := HasSearchableIndex(prop)
	hasRangeableIndex := s.hasUsableRangeableIndex(prop)

	if !hasFilterableIndex && !hasSearchableIndex && !hasRangeableIndex {
		return nil, inverted.NewMissingFilterableIndexError(prop.Name)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               prop.Name,
		operator:           operator,
		hasFilterableIndex: hasFilterableIndex,
		hasSearchableIndex: hasSearchableIndex,
		hasRangeableIndex:  hasRangeableIndex,
		Class:              class,
	}, nil
}

func (s *Searcher) extractInternalProp(propName string, propType schema.DataType, value interface{},
	operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	switch propName {
	case filters.InternalPropBackwardsCompatID, filters.InternalPropID:
		return s.extractIDProp(propName, propType, value, operator, class)
	case filters.InternalPropCreationTimeUnix, filters.InternalPropLastUpdateTimeUnix:
		return s.extractTimestampProp(propName, propType, value, operator, class)
	default:
		return nil, fmt.Errorf(
			"failed to extract internal prop, unsupported internal prop '%s'", propName)
	}
}

func (s *Searcher) extractIDProp(propName string, propType schema.DataType,
	value interface{}, operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	var byteValue []byte

	switch propType {
	case schema.DataTypeText:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected value to be string, got '%T'", value)
		}
		byteValue = []byte(v)
	default:
		return nil, fmt.Errorf(
			"failed to extract id prop, unsupported type '%T' for prop '%s'", propType, propName)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               filters.InternalPropID,
		operator:           operator,
		hasFilterableIndex: HasFilterableIndexIdProp,
		hasSearchableIndex: HasSearchableIndexIdProp,
		Class:              class,
	}, nil
}

func (s *Searcher) extractTimestampProp(propName string, propType schema.DataType, value interface{},
	operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	var byteValue []byte

	switch propType {
	case schema.DataTypeText:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected value to be string, got '%T'", value)
		}
		_, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("expected value to be timestamp, got '%s'", v)
		}
		byteValue = []byte(v)
	case schema.DataTypeDate:
		var asInt64 int64

		switch t := value.(type) {
		case string:
			parsed, err := time.Parse(time.RFC3339, t)
			if err != nil {
				return nil, fmt.Errorf("trying parse time as RFC3339 string: %w", err)
			}
			asInt64 = parsed.UnixMilli()

		case time.Time:
			asInt64 = t.UnixMilli()

		default:
			return nil, fmt.Errorf("expected value to be string or time.Time, got '%T'", value)
		}

		// if propType is a `valueDate`, we need to convert
		// it to ms before fetching. this is the format by
		// which our timestamps are indexed
		byteValue = []byte(strconv.FormatInt(asInt64, 10))
	default:
		return nil, fmt.Errorf(
			"failed to extract timestamp prop, unsupported type '%T' for prop '%s'", propType, propName)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               propName,
		operator:           operator,
		hasFilterableIndex: HasFilterableIndexTimestampProp, // TODO text_rbm_inverted_index & with settings
		hasSearchableIndex: HasSearchableIndexTimestampProp, // TODO text_rbm_inverted_index & with settings
		Class:              class,
	}, nil
}

func (s *Searcher) extractTokenizableProp(prop *models.Property, propType schema.DataType,
	value interface{}, operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	valueString, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected value to be string, got '%T'", value)
	}

	var terms []string
	switch propType {
	case schema.DataTypeText:
		// effectiveTok consults the per-shard tokenization overlay (set
		// during the FINALIZING window of a change-tokenization
		// migration) before falling back to the schema-stored value.
		// Both the LIKE wildcard path and the standard analyze path
		// tokenize query input against effectiveTok so the resulting
		// terms match the bucket content on this shard.
		effectiveTok := ResolveTokenization(s.tokResolver, prop.Name, prop.Tokenization)
		// if the operator is like, we cannot apply the regular text-splitting
		// logic as it would remove all wildcard symbols
		if operator == filters.OperatorLike {
			// LIKE queries need special wildcard-preserving tokenization;
			// fold manually then use the wildcard tokenizer.
			text := valueString
			if prop.TextAnalyzer != nil && prop.TextAnalyzer.ASCIIFold {
				ignore := tokenizer.BuildIgnoreSet(prop.TextAnalyzer.ASCIIFoldIgnore)
				text = tokenizer.FoldASCII(text, ignore)
			}
			terms = tokenizer.TokenizeWithWildcardsForClass(effectiveTok, text, class.Class)
		} else {
			var sw tokenizer.StopwordDetector
			if effectiveTok == models.PropertyTokenizationWord {
				d, err := s.stopwordProvider.Get(prop)
				if err != nil {
					return nil, err
				}
				sw = d
			}
			prepared := tokenizer.NewPreparedAnalyzer(prop.TextAnalyzer)
			result := tokenizer.Analyze(valueString, effectiveTok, class.Class, prepared, sw)
			terms = result.Query
		}
	default:
		return nil, fmt.Errorf("expected value type to be text, got %v", propType)
	}

	hasFilterableIndex := HasFilterableIndex(prop) && !s.isFallbackToSearchable()
	hasSearchableIndex := HasSearchableIndex(prop)
	hasRangeableIndex := s.hasUsableRangeableIndex(prop)

	if !hasFilterableIndex && !hasSearchableIndex && !hasRangeableIndex {
		return nil, inverted.NewMissingFilterableIndexError(prop.Name)
	}

	propValuePairs := make([]*propValuePair, 0, len(terms))
	for _, term := range terms {
		propValuePairs = append(propValuePairs, &propValuePair{
			value:              []byte(term),
			prop:               prop.Name,
			operator:           operator,
			hasFilterableIndex: hasFilterableIndex,
			hasSearchableIndex: hasSearchableIndex,
			hasRangeableIndex:  hasRangeableIndex,
			Class:              class,
		})
	}

	if len(propValuePairs) > 1 {
		return &propValuePair{operator: filters.OperatorAnd, children: propValuePairs, Class: class}, nil
	}
	if len(propValuePairs) == 1 {
		return propValuePairs[0], nil
	}
	return nil, ErrOnlyStopwords
}

func (s *Searcher) extractPropertyLength(prop *models.Property, propType schema.DataType,
	value interface{}, operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	var byteValue []byte

	switch propType {
	case schema.DataTypeInt:
		b, err := s.extractIntValue(value)
		if err != nil {
			return nil, err
		}
		byteValue = b
	default:
		return nil, fmt.Errorf(
			"failed to extract length of prop, unsupported type '%T' for length of prop '%s'", propType, prop.Name)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               helpers.PropLength(prop.Name),
		operator:           operator,
		hasFilterableIndex: HasFilterableIndexPropLength, // TODO text_rbm_inverted_index & with settings
		hasSearchableIndex: HasSearchableIndexPropLength, // TODO text_rbm_inverted_index & with settings
		Class:              class,
	}, nil
}

func (s *Searcher) extractPropertyNull(prop *models.Property, propType schema.DataType,
	value interface{}, operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	var valResult []byte

	switch propType {
	case schema.DataTypeBoolean:
		b, err := s.extractBoolValue(value)
		if err != nil {
			return nil, err
		}
		valResult = b
	default:
		return nil, fmt.Errorf(
			"failed to extract null prop, unsupported type '%T' for null prop '%s'", propType, prop.Name)
	}

	return &propValuePair{
		value:              valResult,
		prop:               helpers.PropNull(prop.Name),
		operator:           operator,
		hasFilterableIndex: HasFilterableIndexPropNull, // TODO text_rbm_inverted_index & with settings
		hasSearchableIndex: HasSearchableIndexPropNull, // TODO text_rbm_inverted_index & with settings
		Class:              class,
	}, nil
}

func (s *Searcher) extractContains(ctx context.Context,
	path *filters.Path, propType schema.DataType, value interface{},
	operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	var operands []filters.Clause
	switch propType {
	case schema.DataTypeText, schema.DataTypeTextArray:
		valueStringArray, err := s.extractStringArray(value)
		if err != nil {
			return nil, err
		}
		operands = getContainsOperands(propType, path, valueStringArray)
	case schema.DataTypeInt, schema.DataTypeIntArray:
		valueIntArray, err := s.extractIntArray(value)
		if err != nil {
			return nil, err
		}
		operands = getContainsOperands(propType, path, valueIntArray)
	case schema.DataTypeNumber, schema.DataTypeNumberArray:
		valueFloat64Array, err := s.extractFloat64Array(value)
		if err != nil {
			return nil, err
		}
		operands = getContainsOperands(propType, path, valueFloat64Array)
	case schema.DataTypeBoolean, schema.DataTypeBooleanArray:
		valueBooleanArray, err := s.extractBoolArray(value)
		if err != nil {
			return nil, err
		}
		operands = getContainsOperands(propType, path, valueBooleanArray)
	case schema.DataTypeDate, schema.DataTypeDateArray:
		valueDateArray, err := s.extractStringArray(value)
		if err != nil {
			return nil, err
		}
		operands = getContainsOperands(propType, path, valueDateArray)
	default:
		return nil, fmt.Errorf("unsupported type '%T' for '%v' operator", propType, operator)
	}

	children, err := s.extractPropValuePairs(ctx, operands, operator, schema.ClassName(class.Class))
	if err != nil {
		return nil, err
	}
	// first-class-operator approach: on a nested path each Contains* operator keeps its operator
	// identity instead of desugaring to AND / OR / NOT(OR). Downstream
	// switches treat ContainsAll as an AND alias and ContainsAny as an OR
	// alias; ContainsNone has dedicated dispatch (a first-class resolver
	// that reads `_exists.{relPath}` as the inversion universe so sibling-
	// branch leaves and phantom leaves cannot leak through the AndNot).
	// Non-nested (flat) paths keep the old desugared shapes which the flat
	// resolver handles.
	out, err := newPropValuePair(class)
	if err != nil {
		return nil, fmt.Errorf("new prop value pair: %w", err)
	}

	out.children = children
	out.Class = class

	// Nested detection covers both direct leaves (isNested=true) and tokenization
	// wrappers produced by buildNestedTextFilterPair for multi-token text values
	// (isWithinRootSubtree=true, childrenFromTokenization=true, isNested=false).
	// Without the isWithinRootSubtree branch, a nested ContainsAll/Any/None on a
	// multi-token text value would misclassify as non-nested and lose operator
	// identity / reintroduce the ContainsNone universe-leak.
	nested := len(children) > 0 && (children[0].nested.isNested || children[0].nested.isWithinRootSubtree)

	switch operator {
	case filters.ContainsAll:
		if nested {
			// Single-value Contains is semantically the bare Equal leaf;
			// return it directly so we don't end up with an unwrapped
			// ContainsAll compound (which groupNestedSubtrees can't promote
			// to isWithinRootSubtree and resolveDocIDs would route through
			// fetchDocIDs on an empty prop).
			if len(children) == 1 {
				return children[0], nil
			}
			out.operator = filters.ContainsAll
			return out, nil
		}
		out.operator = filters.OperatorAnd
	case filters.ContainsAny:
		if nested {
			if len(children) == 1 {
				return children[0], nil
			}
			out.operator = filters.ContainsAny
			return out, nil
		}
		out.operator = filters.OperatorOr
	case filters.ContainsNone:
		if nested {
			// Nested path: keep ContainsNone as a first-class operator. The
			// pvp carries the operand relPath (from the children, which all
			// share it by construction) and the children list as values.
			out.operator = filters.ContainsNone
			out.prop = children[0].prop
			out.nested.relPath = children[0].nested.relPath
			// arr[N] pins are propagated from any child — all children of a
			// single ContainsNone clause share the same pins by construction
			// (extracted from the same Path). The resolver uses them to
			// restrict the `_exists.{relPath}` universe lookup.
			out.nested.arrayIndices = children[0].nested.arrayIndices
			// Note: out.nested.isNested stays false — this is a compound
			// operator node, not a leaf; the value-leaves live as children.
			return out, nil
		}
		// Non-nested path: keep the desugared NOT(OR(...)) shape. The
		// existing flat resolver handles it.
		out.operator = filters.OperatorOr

		parent, err := newPropValuePair(class)
		if err != nil {
			return nil, fmt.Errorf("new prop value pair: %w", err)
		}
		parent.operator = filters.OperatorNot
		parent.children = []*propValuePair{out}
		parent.Class = class
		out = parent
	default:
		return nil, fmt.Errorf("unknown contains operator %v", operator)
	}
	return out, nil
}

// TODO: repeated calls to on... aren't too efficient because we iterate over
// the schema each time, might be smarter to have a single method that
// determines the type and then we switch based on the result. However, the
// effect of that should be very small unless the schema is absolutely massive.
func (s *Searcher) onRefProp(property *models.Property) bool {
	return schema.IsRefDataType(property.DataType)
}

// TODO: repeated calls to on... aren't too efficient because we iterate over
// the schema each time, might be smarter to have a single method that
// determines the type and then we switch based on the result. However, the
// effect of that should be very small unless the schema is absolutely massive.
func (s *Searcher) onGeoProp(prop *models.Property) bool {
	return schema.DataType(prop.DataType[0]) == schema.DataTypeGeoCoordinates
}

// Note: A UUID prop is a user-specified prop of type UUID. This has nothing to
// do with the primary ID of an object which happens to always be a UUID in
// Weaviate v1
//
// TODO: repeated calls to on... aren't too efficient because we iterate over
// the schema each time, might be smarter to have a single method that
// determines the type and then we switch based on the result. However, the
// effect of that should be very small unless the schema is absolutely massive.
func (s *Searcher) onUUIDProp(prop *models.Property) bool {
	switch dt, _ := schema.AsPrimitive(prop.DataType); dt {
	case schema.DataTypeUUID, schema.DataTypeUUIDArray:
		return true
	default:
		return false
	}
}

func (s *Searcher) onInternalProp(propName string) bool {
	return filters.IsInternalProperty(schema.PropertyName(propName))
}

func (s *Searcher) onTokenizableProp(prop *models.Property) bool {
	switch dt, _ := schema.AsPrimitive(prop.DataType); dt {
	case schema.DataTypeText, schema.DataTypeTextArray:
		return true
	default:
		return false
	}
}

func (s *Searcher) extractStringArray(value interface{}) ([]string, error) {
	switch v := value.(type) {
	case []string:
		return v, nil
	case []interface{}:
		vals := make([]string, len(v))
		for i := range v {
			val, ok := v[i].(string)
			if !ok {
				return nil, fmt.Errorf("value[%d] type should be string but is %T", i, v[i])
			}
			vals[i] = val
		}
		return vals, nil
	default:
		return nil, fmt.Errorf("value type should be []string but is %T", value)
	}
}

func (s *Searcher) extractIntArray(value interface{}) ([]int, error) {
	switch v := value.(type) {
	case []int:
		return v, nil
	case []interface{}:
		vals := make([]int, len(v))
		for i := range v {
			// in this case all number values are unmarshalled to float64, so we need to cast to float64
			// and then make int
			val, ok := v[i].(float64)
			if !ok {
				return nil, fmt.Errorf("value[%d] type should be float64 but is %T", i, v[i])
			}
			vals[i] = int(val)
		}
		return vals, nil
	default:
		return nil, fmt.Errorf("value type should be []int but is %T", value)
	}
}

func (s *Searcher) extractFloat64Array(value interface{}) ([]float64, error) {
	switch v := value.(type) {
	case []float64:
		return v, nil
	case []interface{}:
		vals := make([]float64, len(v))
		for i := range v {
			val, ok := v[i].(float64)
			if !ok {
				return nil, fmt.Errorf("value[%d] type should be float64 but is %T", i, v[i])
			}
			vals[i] = val
		}
		return vals, nil
	default:
		return nil, fmt.Errorf("value type should be []float64 but is %T", value)
	}
}

func (s *Searcher) extractBoolArray(value interface{}) ([]bool, error) {
	switch v := value.(type) {
	case []bool:
		return v, nil
	case []interface{}:
		vals := make([]bool, len(v))
		for i := range v {
			val, ok := v[i].(bool)
			if !ok {
				return nil, fmt.Errorf("value[%d] type should be bool but is %T", i, v[i])
			}
			vals[i] = val
		}
		return vals, nil
	default:
		return nil, fmt.Errorf("value type should be []bool but is %T", value)
	}
}

func getContainsOperands[T any](propType schema.DataType, path *filters.Path, values []T) []filters.Clause {
	operands := make([]filters.Clause, len(values))
	for i := range values {
		operands[i] = filters.Clause{
			Operator: filters.OperatorEqual,
			On:       path,
			Value: &filters.Value{
				Type:  propType,
				Value: values[i],
			},
		}
	}
	return operands
}

type docIDsIterator interface {
	Next() (uint64, bool)
	Len() int
	Stop()
}

type sliceDocIDsIterator struct {
	docIDs []uint64
	pos    int
}

func newSliceDocIDsIterator(docIDs []uint64) docIDsIterator {
	return &sliceDocIDsIterator{docIDs: docIDs, pos: 0}
}

func (it *sliceDocIDsIterator) Next() (uint64, bool) {
	if it.pos >= len(it.docIDs) {
		return 0, false
	}
	pos := it.pos
	it.pos++
	return it.docIDs[pos], true
}

func (it *sliceDocIDsIterator) Stop() {
	// No-op for slice iterator as there's no cleanup needed
}

func (it *sliceDocIDsIterator) Len() int {
	return len(it.docIDs)
}

type docBitmap struct {
	docIDs     *sroar.Bitmap
	isDenyList bool
	release    func()
}

// newUninitializedDocBitmap can be used whenever we can be sure that the first
// user of the docBitmap will set or replace the bitmap, such as a row reader
func newUninitializedDocBitmap() docBitmap {
	return docBitmap{}
}

func newDocBitmap() docBitmap {
	return docBitmap{docIDs: sroar.NewBitmap(), release: func() {}, isDenyList: false}
}

func (dbm *docBitmap) count() int {
	if dbm.docIDs == nil {
		return 0
	}
	return dbm.docIDs.GetCardinality()
}

func (dbm *docBitmap) IDs() []uint64 {
	if dbm.docIDs == nil {
		return []uint64{}
	}
	return dbm.docIDs.ToArray()
}

func (dbm *docBitmap) IDsWithLimit(limit int) []uint64 {
	card := dbm.docIDs.GetCardinality()
	if limit >= card {
		return dbm.IDs()
	}

	out := make([]uint64, limit)
	for i := range out {
		// safe to ignore error, it can only error if the index is >= cardinality
		// which we have already ruled out
		out[i], _ = dbm.docIDs.Select(uint64(i))
	}

	return out
}

func (dbm *docBitmap) IsDenyList() bool {
	return dbm.isDenyList
}
