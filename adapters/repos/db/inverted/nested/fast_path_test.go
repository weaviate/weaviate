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

package nested

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/tokenizer"
)

// fast_path_test.go is the pre-DB harness for the fast-path nested filter
// execution model. Tests exercise the (Raw, Witnesses, Scope,
// Ceiling) composition rules directly against AssignPositions output,
// skipping the parser / planner / shard write layers.

// ---------------------------------------------------------------------------
// AND merge dispatch
// ---------------------------------------------------------------------------
//
// andLeaves dispatches on the relationship between the two inputs'
// Scopes (same Scope, ancestor+child, siblings). The branch picked
// determines merge scope, lift strategy, and the ceiling that becomes
// result.Ceiling.
//
// Notation: A = ancestor (shallower Scope), C = child (deeper Scope).
//
//   scope relation       branch condition                                  merge scope   action
//   ─────────────────────────────────────────────────────────────────────────────────────────────
//   same Scope           (always)                                          left.Scope    andAtScope
//   ─────────────────────────────────────────────────────────────────────────────────────────────
//   ancestor + child     A.ceilingAboveScope                                 C.Scope       andAtScope
//   ancestor + child     depth(C.Ceiling) ≤ depth(A.Scope) (branch 1 missed) A.Scope  andAtScope (no lift)
//   ancestor + child     C not clean at A.Scope                            A.Scope       liftToScope(C, A.Scope) then andAtScope
//   ─────────────────────────────────────────────────────────────────────────────────────────────
//   siblings (LCA above) (always)                                          LCA           lift both to LCA, recurse same-Scope
//
// The broadcasting branch fires when ancestor.ceilingAboveScope() is true.
// By the Ceiling convention, this is true iff ancestor was constructed
// from the elementPositions encoding (positive leaves, OR-of-positives,
// or broadcasting-derived compounds) — operands whose Bitmaps have BOTH
// authentic chain bits above Scope AND authentic descendant bits below.
// Positive leaves set Ceiling=pathRoot (depth 0), so ceilingAboveScope
// is true for them at any schema depth. Compounds from same-Scope/
// siblings AND, negate, etc. set Ceiling=Scope, so ceilingAboveScope
// is false and the broadcasting branch is correctly skipped.
//
// The second ancestor+child branch uses depth(C.Ceiling) ≤ depth(A.Scope),
// NOT just C.ceilingAboveScope. C may be clean somewhere above its own Scope
// without that cleanness reaching A.Scope — e.g. an ancestor+child AND
// result whose Ceiling sits at an intermediate scope between its Scope and A.Scope.
// In that case C's bits at A.Scope aren't authentic and a direct merge
// would let ghosts leak; the third branch lifts C to A.Scope instead.
//
// Cleanness ceilings:
//
//   - Same Scope / siblings: ceiling = merge scope. Chain bits at parent
//     are shared across elements, so same-scope AND can produce owner-
//     level ghosts strictly above the merge scope.
//
//   - Ancestor+child, A clean above A.Scope: A.Raw broadcasts down
//     through descendantSelves; intersection at any scope in [C.Scope,
//     A.Scope] is per-element. Above A.Scope the bits collapse to shared
//     chain — ghost prone. Ceiling = deepestPath(A.Scope, C.Ceiling).
//
//   - Ancestor+child, A NOT clean above its Scope, C trustworthy at
//     A.Scope: direct intersection at A.Scope; A's ghosts above persist.
//     Ceiling = A.Scope.
//
//   - Ancestor+child, neither trustworthy: C.Raw at A.Scope may have
//     ghosts that would leak into the result. Lift C to A.Scope first
//     (LiftToAncestor wipes ghosts), then merge same-scope at A.Scope.
//
// The core merge formula (raw = left.Raw ∩ right.Raw, witnesses = raw ∩
// _anchor(mergeScope)) is identical in every branch; only the merge scope
// and lift strategy vary.

// ---------------------------------------------------------------------------
// OR merge dispatch matrix
// ---------------------------------------------------------------------------
//
// orLeaves dispatches on scope relation × per-operand cleanness reach.
// Unlike AND, OR never creates new ghosts — a chain bit at parent in
// (A.Raw ∪ B.Raw) legitimately means "this parent contains a
// matching element of either kind." OR always lands at the LCA: merging
// below A.Scope would lose A's "container exists" semantic (a doc with a
// warsaw garage but zero cars must still satisfy `warsaw OR cars.X`).
//
// Shorthand below: X is "clean@LCA" iff pathDepth(X.Ceiling) ≤
// pathDepth(LCA) — i.e. X's Raw is authentic at LCA without lifting.
// X is "ghosty@LCA" otherwise.
//
//   scope relation       cleanness         merge   result.Ceiling          per-operand action
//   ─────────────────────────────────────────────────────────────────────────────────────────────
//   same Scope           (any, any)        scope   deeperOf(A.CA, B.CA)       none
//   ancestor + child     (any, C clean)    A.Scope deeperOf(A.CA, C.CA)       C: cheap anchor-intersect
//   ancestor + child     (any, C ghosty)   A.Scope A.Scope (C lift caps it)   C: real LiftToAncestor on Witnesses
//   siblings (LCA above) (A clean, B clean) LCA    deeperOf(A.CA, B.CA)       both: cheap
//   siblings             (A clean, B ghosty) LCA   LCA (B lift caps)          A: cheap; B: real lift
//   siblings             (A ghosty, B clean) LCA   LCA (A lift caps)          A: real lift; B: cheap
//   siblings             (A ghosty, B ghosty) LCA  LCA (both lifts cap)       both: real lift
//
// "Cheap anchor-intersect" = X.Raw ∩ _anchor(LCA) — works because
// X clean@LCA guarantees X.Raw is authentic at LCA already.
// "Real lift" = LiftToAncestor on X.Witnesses + Raw reconstruction at
// LCA — needed when X is ghosty@LCA because X.Raw at LCA carries
// ghost chain bits that would leak into the union's Witnesses.
//
// result.Ceiling = deepestPath(lifted_A.Ceiling, lifted_B.Ceiling)
// in every row — i.e. the most restrictive of the two operands' clean
// reaches after lifting. A real lift to LCA caps the operand's reach at
// LCA (Ceiling gets reset to targetScope); a cheap lift preserves the
// operand's existing Ceiling. So a real-lifted side caps the result
// at LCA; two clean@LCA operands can keep a shallower (further up) Ceiling.
//
// Why each rule: //
//   - Same Scope: union preserves authentic chain bits from each side; no
//     new ghosts. Ceiling is whatever ghost characteristics each
//     operand brought to the merge.
//
//   - Ancestor+child, C clean@LCA: C.Raw is authentic at every scope
//     ≥ C.Scope up to root, including A.Scope. Direct union at A.Scope
//     works without lifting; the result preserves both operands' reach.
//
//   - Ancestor+child, C ghosty@LCA: C.Raw at A.Scope may have ghosts
//     that would leak into Witnesses at A.Scope via the anchor
//     intersection. Lift C up so its A.Scope bits are rebuilt from
//     authentic Witnesses. C's reach gets capped at A.Scope.
//
//   - Siblings: same logic per-operand — each clean@LCA operand
//     contributes authentically at LCA without lifting; each ghosty@LCA
//     operand needs a real lift to clean its LCA bits. Two clean@LCA
//     operands preserve their reach above LCA; any ghosty side caps the
//     result at LCA.
//
// The core merge formula (raw = lifted_A.Raw ∪ lifted_B.Raw, witnesses =
// lifted_A.Witnesses ∪ lifted_B.Witnesses) is identical in every branch; only the per-
// operand lift strategy and result Ceiling vary. The shortcut for Witnesses (union of
// per-operand Witnesses instead of raw ∩ _anchor(LCA)) is cheaper than the AND
// formula and correct because union of authentic sets is authentic.

// pathRoot is the sentinel scope sitting one step above every propName-
// rooted path. Two roles:
//
//   - Ceiling value: "Raw is authentic all the way up — no scope above
//     carries owner-level ghosts." Used by positive leaves and
//     OR-of-positive compounds.
//
//   - Scope value: doc-level results from pinned-root positives (e.g.
//     `cars[1].year=2020` when the outermost pin is at the property
//     root). The pinned helper uses MaskRootLeaf instead of
//     LiftToAncestor because the doc-level anchor (rootIdx=0) lives in
//     a different (rootIdx, docID) bucket than any descendant marker.
//     The result coincides because Encode(0, 0, docID) == docID.
//
// Path traversal: pathDepth(pathRoot) = 0; parentPath of any propName-
// level path returns pathRoot; parentPath(pathRoot) returns itself
// (idempotent terminal). commonScope returns pathRoot whenever two
// scopes share no propName-level prefix.
//
// idx.docUniverse holds the doc-level universe — one position per
// ingested doc, Encode(0, 0, docID) == docID. Kept out of the
// anchor/exists maps because those mirror on-disk meta entries (one
// per real property scope) and the universe isn't one. Reads route via
// anchorAt / existsAt so callers don't special-case the sentinel; with
// that the trustworthy-scope invariant Raw ∩ anchorAt(Scope) ==
// Witnesses holds uniformly across pathRoot and real schema scopes.
const pathRoot = "_root"

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// fastPathIndex aggregates AssignPositions output across many docs into the
// four bucket families a nested filter reader sees on disk: value, _exists,
// _idx, _anchor. All bitmaps are keyed by *logical* path (property name
// prepended).
//
// ops uses a noop pool — buffer lifecycle is not what these tests are about.
type fastPathIndex struct {
	propName string
	values   map[string]map[any]*sroar.Bitmap // path -> raw value -> positions
	exists   map[string]*sroar.Bitmap         // path -> positions
	idx      map[string]map[int]*sroar.Bitmap // path -> array index -> positions
	anchor   map[string]*sroar.Bitmap         // path -> self-marker positions
	// tokenization: leaf path -> tokenization strategy (e.g. "field",
	// "word") propagated from the schema by AssignPositions. Empty for
	// non-text leaves. Read by addDoc to tokenize stored values and by
	// the value-leaf helpers (leafPositive, leafContainsAny, etc.) to
	// tokenize query values, mirroring the production analyzer's
	// placement of tokenizer.Tokenize between AssignPositions and
	// bucket emission.
	tokenization map[string]string
	// docUniverse: doc-level universe of ingested docs, one Encode(0, 0,
	// docID) per doc. Lives outside `anchor`/`exists` because those mirror
	// on-disk meta buckets and the universe isn't a meta entry — the
	// production reader will get this raw from somewhere else.
	docUniverse *sroar.Bitmap
	ops         *BitmapOps
}

func newFastPathIndex(propName string) *fastPathIndex {
	return &fastPathIndex{
		propName:     propName,
		values:       map[string]map[any]*sroar.Bitmap{},
		exists:       map[string]*sroar.Bitmap{},
		idx:          map[string]map[int]*sroar.Bitmap{},
		anchor:       map[string]*sroar.Bitmap{},
		tokenization: map[string]string{},
		docUniverse:  sroar.NewBitmap(),
		ops:          NewBitmapOps(roaringset.NewBitmapBufPoolNoop()),
	}
}

// anchorAt returns the per-element self-marker raw for scope. Reads
// idx.anchor for real schema scopes; routes the pathRoot sentinel to the
// out-of-band doc universe so callers don't have to special-case it.
func (i *fastPathIndex) anchorAt(scope string) *sroar.Bitmap {
	if scope == pathRoot {
		return i.docUniverse
	}
	return i.anchor[scope]
}

// existsAt returns the existence raw for scope. Same routing as
// anchorAt: pathRoot maps to docUniverse (every ingested doc is "live"
// at the doc level), other scopes read from idx.exists.
func (i *fastPathIndex) existsAt(scope string) *sroar.Bitmap {
	if scope == pathRoot {
		return i.docUniverse
	}
	return i.exists[scope]
}

// qualify converts a property-relative path (as emitted by AssignPositions)
// to the logical path used throughout these tests.
func (i *fastPathIndex) qualify(path string) string {
	if path == "" {
		return i.propName
	}
	return i.propName + "." + path
}

func (i *fastPathIndex) addDoc(t *testing.T, prop *models.Property, docID uint64, value any) {
	t.Helper()
	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	for _, v := range result.Values {
		path := i.qualify(v.Path)
		if i.values[path] == nil {
			i.values[path] = map[any]*sroar.Bitmap{}
		}
		if v.Tokenization != "" {
			i.tokenization[path] = v.Tokenization
		}
		// tokenizeStoredValue mirrors the analyzer's call to
		// tokenizer.Tokenize after AssignPositions: text and text[] leaves
		// fan out into one bucket per token; every token shares the
		// PositionedValue's positions (so multi-token Equal needs both
		// tokens at the same Scope element). Non-text leaves and untokenized
		// values bucket on the raw value.
		for _, token := range tokenizeStoredValue(v) {
			bm, ok := i.values[path][token]
			if !ok {
				bm = sroar.NewBitmap()
				i.values[path][token] = bm
			}
			bm.SetMany(OrDocID(v.Positions, docID))
		}
	}
	for _, e := range result.Exists {
		path := i.qualify(e.Path)
		bm, ok := i.exists[path]
		if !ok {
			bm = sroar.NewBitmap()
			i.exists[path] = bm
		}
		bm.SetMany(OrDocID(e.Positions, docID))
	}
	for _, x := range result.Idx {
		path := i.qualify(x.Path)
		if i.idx[path] == nil {
			i.idx[path] = map[int]*sroar.Bitmap{}
		}
		bm, ok := i.idx[path][x.Index]
		if !ok {
			bm = sroar.NewBitmap()
			i.idx[path][x.Index] = bm
		}
		bm.SetMany(OrDocID(x.Positions, docID))
	}
	for _, a := range result.Anchors {
		path := i.qualify(a.Path)
		bm, ok := i.anchor[path]
		if !ok {
			bm = sroar.NewBitmap()
			i.anchor[path] = bm
		}
		bm.SetMany(OrDocID(a.Positions, docID))
	}
	// Doc-level universe: one entry per ingested doc. Under the position
	// layout Encode(0, 0, docID) == docID, so the bit is set directly.
	// Used by pinned-root results to satisfy the trustworthy-scope
	// invariant `Raw ∩ anchorAt(Scope) == Witnesses` uniformly, and
	// by liftToScope when targetScope == pathRoot. Kept out of
	// idx.anchor / idx.exists because those maps mirror meta buckets;
	// the universe will be supplied separately at runtime. anchorAt /
	// existsAt route pathRoot reads here.
	i.docUniverse.Set(docID)
}

// ---------------------------------------------------------------------------
// Result model
// ---------------------------------------------------------------------------

// fastPathResult is the four-tuple every leaf builder and merge helper
// produces. Tests build instances by hand using the helpers below.
//
//  1. Scope: the scope at which the predicate's natural witnesses live.
//     Used for Witnesses computation, dispatch (commonScope), and as
//     the scope `negate` inverts at.
//
//  2. Raw: position bitmap carrying selfMarkers at Scope plus chain bits
//     above and (sometimes) descendant bits below. The operand input to
//     andLeaves/orLeaves intersection/union.
//
//  3. Witnesses: per-element selfMarkers at Scope = Raw ∩ anchor(Scope).
//     Always trustworthy (per-element), regardless of Ceiling.
//
//  4. Ceiling: the shallowest scope at which Raw is still authentic.
//     - Ceiling == pathRoot: fully authentic — every bit traces to a
//     real matching element's elementPositions encoding (chain +
//     selfMarker + descendantSelves). Earned by positive leaves and
//     OR-of-positives.
//     - Ceiling == some specific path: Raw is authentic only at scopes
//     from Ceiling down to Scope; above Ceiling, owner-level chain
//     bits may leak as ghosts. negate, same-Scope AND, ContainsAll,
//     ContainsNone, and pinned helpers all land here.
//
// Dispatch consequence: ceilingAboveScope() (depth(Ceiling) <
// depth(Scope)) is true iff the operand has authentic descendants below
// Scope — the precondition for the broadcasting branch in andLeaves.
//
// Lift rule (liftToScope): cheap anchor-intersect when targetScope is
// at-or-below the operand's clean range; full LiftToAncestor on
// Witnesses otherwise.
type fastPathResult struct {
	Scope     string
	Raw       *sroar.Bitmap
	Witnesses *sroar.Bitmap
	Ceiling   string
}

// ceilingAboveScope reports whether the ceiling sits strictly above the
// scope — i.e. Raw is authentic at one or more scopes shallower than
// Scope. True for positive leaves and broadcasting-derived compounds;
// false for negate, same-Scope AND, ContainsAll, ContainsNone, and
// other compounds with Ceiling == Scope.
func (r *fastPathResult) ceilingAboveScope() bool {
	return pathDepth(r.Ceiling) < pathDepth(r.Scope)
}

// deepestPath returns whichever scope has the greatest depth (= furthest
// from the property root). All inputs must lie on the same root-to-leaf
// chain so depth comparison is meaningful. Used to combine candidate
// Ceiling values during merge.
func deepestPath(scopes ...string) string {
	best := scopes[0]
	bestDepth := pathDepth(best)
	for _, s := range scopes[1:] {
		if d := pathDepth(s); d > bestDepth {
			best = s
			bestDepth = d
		}
	}
	return best
}

// ---------------------------------------------------------------------------
// Leaf builders
// ---------------------------------------------------------------------------

// tokenizeStoredValue mirrors the production analyzer's call to
// tokenizer.Tokenize after AssignPositions: text and text[] leaves with
// a tokenization strategy fan out to one bucket per token; non-text and
// untokenized leaves bucket on the raw value. Tokens are []any so they
// drop straight into idx.values[path]'s any-keyed bucket map without an
// extra type assertion at the call site.
func tokenizeStoredValue(pv PositionedValue) []any {
	if pv.Tokenization == "" {
		return []any{pv.Value}
	}
	switch schema.DataType(pv.DataType) {
	case schema.DataTypeText:
		s, ok := pv.Value.(string)
		if !ok {
			return []any{pv.Value}
		}
		return tokenizeOne(pv.Tokenization, s)
	case schema.DataTypeTextArray:
		var out []any
		switch arr := pv.Value.(type) {
		case []string:
			for _, s := range arr {
				out = append(out, tokenizeOne(pv.Tokenization, s)...)
			}
		case []any:
			for _, x := range arr {
				if s, ok := x.(string); ok {
					out = append(out, tokenizeOne(pv.Tokenization, s)...)
				}
			}
		default:
			return []any{pv.Value}
		}
		return out
	default:
		return []any{pv.Value}
	}
}

func tokenizeOne(strategy, in string) []any {
	tokens := tokenizer.Tokenize(strategy, in)
	out := make([]any, len(tokens))
	for i, t := range tokens {
		out[i] = t
	}
	return out
}

// tokenizeQueryValue mirrors tokenizeStoredValue for the query side: if
// the recorded leaf tokenization is non-empty and the query value is a
// string, return its tokens; otherwise return the value unchanged.
// Consumed by tokenizedMatchBitmap to build the per-value match bitmap
// shared by every value-leaf helper.
func tokenizeQueryValue(idx *fastPathIndex, path string, value any) []any {
	strategy := idx.tokenization[path]
	if strategy == "" {
		return []any{value}
	}
	s, ok := value.(string)
	if !ok {
		return []any{value}
	}
	return tokenizeOne(strategy, s)
}

// tokenizedMatchBitmap returns the per-value match bitmap Mᵢ for a
// single query value: tokenize the value via the recorded leaf
// tokenization, look up each token's bucket, and AND them together.
// Single-token values collapse to a single bucket lookup (the original
// fast path for non-WORD-tokenized leaves); multi-token values
// intersect the per-token buckets — bitmap positions survive the AND
// only when every token is present on the same Scope-element.
//
// This is the unit every value-leaf helper composes over. Operators
// differ only in how they merge Mᵢ across list values:
//
//   - leafPositive (single value): use M₁ directly.
//   - leafContainsAny: OR all Mᵢ.
//   - leafContainsAll: AND all Mᵢ.
//   - leafContainsNone: AndNot each Mᵢ from anchor(Scope) in turn.
//
// Returns an empty bitmap when the tokenizer drops the input
// (stopword-only) or any token bucket is missing from the index — in
// either case no Scope-element can satisfy that value. The Contains
// operators carry that through correctly via their merge step.
//
// Same-element semantics: this helper does parent-Scope AND for
// multi-token values on text[] — tokens may live in different array
// entries of the same parent element, the lenient interpretation. A
// strict (same-array-entry) rule would require extracting per-element-
// self bits at Scope=path and lifting back; that's deliberately not
// modeled here. See the design note in fast_path_python_port_test.go
// for the rationale.
func tokenizedMatchBitmap(idx *fastPathIndex, path string, value any) *sroar.Bitmap {
	tokens := tokenizeQueryValue(idx, path, value)
	if len(tokens) == 0 {
		return sroar.NewBitmap()
	}
	bm := cloneOrEmpty(idx.values[path][tokens[0]])
	for _, t := range tokens[1:] {
		bm.And(cloneOrEmpty(idx.values[path][t]))
	}
	return bm
}

// leafPositive realizes a positive value leaf `path = value`:
//
//	Scope     = parent(path)
//	Raw       = M  (per-value match bitmap; see tokenizedMatchBitmap)
//	Witnesses = Raw ∩ _anchor(Scope)
//	Ceiling   = pathRoot (fully authentic — every bit traces to a real
//	                       matching element's elementPositions encoding;
//	                       see fastPathResult doc for the convention)
//
// Tokenization handling lives entirely in tokenizedMatchBitmap. Single-
// token queries collapse to today's single-bucket fast path; multi-
// token queries produce the AND of per-token buckets (parent-Scope
// same-element semantic on text[]).
func leafPositive(idx *fastPathIndex, path string, value any) *fastPathResult {
	scope := parentPath(path)
	raw := tokenizedMatchBitmap(idx, path, value)
	return &fastPathResult{
		Scope:     scope,
		Raw:       raw,
		Witnesses: raw.Clone().And(idx.anchorAt(scope)),
		Ceiling:   pathRoot,
	}
}

// leafIsNullFalse realizes `path IS NULL false` (equivalently exists(path)).
// Mirrors leafPositive but reads from _exists instead of a value-keyed bucket
// so the leaf fires for every emission regardless of the recorded value.
//
//	Scope     = parent(path)
//	Raw       = _exists(path)
//	Witnesses = Raw ∩ _anchor(Scope)
//	Ceiling   = pathRoot (same authentic-descendant encoding as positive)
//
// TODO aliszka:nested_filtering — for object / object-array paths,
// Scope=array-scope (= path itself) instead of parent would let
// compound merges with descendant predicates skip a LiftToAncestor.
// Kept symmetric with scalars at Scope=parent so the pairing
// `negate(leafIsNullFalse) ≡ leafIsNullTrue` works without a separate
// IS-NULL-true builder. Revisit if pinnedFromValueSet's lift shows up
// as a measurable cost on profiled real-world queries.
func leafIsNullFalse(idx *fastPathIndex, path string) *fastPathResult {
	scope := parentPath(path)
	raw := cloneOrEmpty(idx.existsAt(path))
	var witnesses *sroar.Bitmap
	if scope == pathRoot {
		// `propName IS NULL false`: the property itself is the
		// object-array at the root, so witnesses are docIDs of every
		// doc that has any data under it. MaskRootLeaf projects
		// propName-self-markers to plain docIDs (= Encode(0, 0, docID),
		// matching docUniverse). OR-ing those bits into Raw keeps the
		// trustworthy-scope invariant `Raw ∩ anchorAt(pathRoot) ==
		// Witnesses` true — the underlying _exists bitmap carries only
		// descendant-scope positions and would intersect to empty.
		witnesses, _ = idx.ops.MaskRootLeaf(raw)
		raw = raw.Or(witnesses)
	} else {
		witnesses = raw.Clone().And(idx.anchorAt(scope))
	}
	return &fastPathResult{
		Scope:     scope,
		Raw:       raw,
		Witnesses: witnesses,
		Ceiling:   pathRoot,
	}
}

// negate produces the witnesses-first negation of any positive leaf result.
// The transformation is identical across every NOT-style leaf (unpinned NOT,
// IS NULL true, pinned variants): the positive helper's Witnesses is
// always the canonical positive witnesses at Scope, and the negation is
// `universe \ pos.Witnesses` for both Witnesses (over _anchor) and
// Raw (over _exists).
//
// Ceiling collapses to the negation's own Scope: Raw = _exists(Scope)
// AndNot Witnesses carries chain bits from every doc with any Scope-element
// (so above-Scope chain bits may be ghosts) AND carries descendant markers
// of every Scope-element including matching ones (so below-Scope bits are
// bogus for the negation predicate). Both halves are non-authentic, so the
// non-"" Ceiling correctly signals that the operand isn't broadcast-safe.
//
//	Scope     = pos.Scope
//	Witnesses = _anchor(Scope) ANDNOT pos.Witnesses
//	Raw       = _exists(Scope) ANDNOT pos.Witnesses
//	Ceiling   = Scope (no claim above; not "")
func negate(idx *fastPathIndex, pos *fastPathResult) *fastPathResult {
	scopeAnchor := idx.anchorAt(pos.Scope)
	return &fastPathResult{
		Scope:     pos.Scope,
		Witnesses: cloneOrEmpty(scopeAnchor).AndNot(pos.Witnesses),
		Raw:       cloneOrEmpty(idx.existsAt(pos.Scope)).AndNot(pos.Witnesses),
		Ceiling:   pos.Scope,
	}
}

// leafIsNullTrue realizes `path IS NULL true` — universe-subtract of
// leafIsNullFalse's positive witnesses.
func leafIsNullTrue(idx *fastPathIndex, path string) *fastPathResult {
	return negate(idx, leafIsNullFalse(idx, path))
}

// leafNot realizes `NOT path = value` — universe-subtract of leafPositive's
// positive witnesses. Empty array scope (no elements) does NOT count as a
// witness — that falls out because pos.Witnesses already requires at
// least one Scope-element to exist.
func leafNot(idx *fastPathIndex, path string, value any) *fastPathResult {
	return negate(idx, leafPositive(idx, path, value))
}

// leafPinnedIsNullFalse realizes `<pinned path>.x IS NULL false` using the
// same unified pipeline as leafPinnedPositive, just reading from _exists
// instead of a value-keyed bucket. Works for any pin layout.
//
//	A              = _exists(valuePath) ∩ _idx(pin_0) ∩ … ∩ _idx(pin_N-1)
//	mPin           = A ∩ _anchor(parent(valuePath))
//	Scope          = parent(pins[0].path)
//	narrowedAnchor = _anchor(Scope) ∩ A
//	Witnesses      = LiftToAncestor(mPin, narrowedAnchor)
//	Raw            = (A ANDNOT _anchor(Scope)) ∪ Witnesses
//	Ceiling        = Scope
//
// TODO aliszka:nested_filtering — make the lift lazy when downstream only
// consumes doc-level output; MaskRootLeaf(mPin) suffices in that case.
//
// TODO aliszka:nested_filtering — when valuePath denotes an object /
// object-array (not a scalar leaf), the array-scope Scope alternative
// described in leafIsNullFalse's comment applies here too, and is where
// the BIGGEST practical win lives (eliminates the LiftToAncestor inside
// pinnedFromValueSet for every pinned object/object[] IS NULL false). See
// the rationale and deferral reasons documented at leafIsNullFalse.
func leafPinnedIsNullFalse(idx *fastPathIndex, valuePath string, pins []pinSpec) *fastPathResult {
	if len(pins) == 0 {
		panic("pinned IS NULL false needs at least one pin")
	}
	a := cloneOrEmpty(idx.existsAt(valuePath))
	for _, pin := range pins {
		a.And(idx.idx[pin.path][pin.index])
	}
	return pinnedFromValueSet(idx, a, valuePath, pins)
}

// leafPinnedIsNullTrue realizes `<pinned path>.x IS NULL true`. Dispatches
// internally based on whether the pin chain has a gap between Scope
// and the value path's element scope: //
//
//   - No gap (fully pinned at every array level): universe-subtract of
//     leafPinnedIsNullFalse. Owner-level coincides with per-element here
//     because the pinned slot is uniquely identified per Scope-element.
//
//   - Gap present (intermediate pin — some array level between Scope and
//     ElementScope is unpinned): owner-level subtraction would diverge
//     from the natural per-element reading (e.g. `g[1].cars.year IS NULL
//     true` should pass any car under g[1] missing year, not require ALL
//     cars under g[1] to be missing year). Use per-element computation:
//     elementWitnesses ∪ scopeMissingPin lifted to Scope.
//
// Missing pinned slot still counts as success in both branches — covered
// by scopeMissingPin in the per-element formula and by universe-subtract
// in the no-gap branch.
func leafPinnedIsNullTrue(idx *fastPathIndex, valuePath string, pins []pinSpec) *fastPathResult {
	if !hasPinGap(pins, valuePath) {
		return negate(idx, leafPinnedIsNullFalse(idx, valuePath, pins))
	}
	return perElementNotExists(idx, valuePath, pins)
}

// pinSpec describes one pin in a multi-pin path, e.g.,
// `garages[1].cars[2].year=2020` carries two pins: `{path:"countries.garages",
// index:1}` and `{path:"countries.garages.cars", index:2}`.
type pinSpec struct {
	path  string // qualified path of the pinned array
	index int    // pinned index within that array
}

// leafPinnedPositive realizes a multi-pin positive value leaf using a
// unified pipeline that works for any pin layout (innermost-pinned,
// intermediate-pinned, all-pinned multi-level) with a single LiftToAncestor
// regardless of pin count.
//
//	A              = value(valuePath, value) ∩ _idx(pin_0) ∩ … ∩ _idx(pin_N-1)
//	mPin           = A ∩ _anchor(parent(valuePath))   // drops chain bits
//	Scope          = parent(pins[0].path)
//	narrowedAnchor = _anchor(Scope) ∩ A               // optimization
//	Witnesses      = LiftToAncestor(mPin, narrowedAnchor)
//	Raw            = (A ANDNOT _anchor(Scope)) ∪ Witnesses
//	Ceiling        = Scope
//
// A carries chain bits inside the pinned subtree. Some are authentic
// ancestors of real matches; others are ghost bits left over when the
// value matches at a sibling slot of a pin. mPin drops every chain bit
// (both authentic and ghost), leaving only leaf-scope selves where every
// pin constraint holds at a single concrete element — the canonical
// Witnesses once lifted to Scope.
//
// narrowedAnchor: every authentic Scope ancestor of an mPin element is
// already in A (chain bit of the same emission), so pre-intersecting the
// parent anchor with A doesn't drop any required predecessor and shrinks
// the LiftToAncestor scan to buckets that survived the pin intersections.
//
// Raw preserves chain structure within the pinned subtree by stripping
// only Scope-level bits before OR-ing Witnesses back. Intermediate-level
// ghosts can persist below Scope; Ceiling=Scope marks that downstream
// composition above Scope via anchor is unsafe.
//
// Root-level pin (outermost pin at the property root array, e.g.
// `cars[1].year=2020` at L0) is supported via pinnedFromValueSet's
// MaskRootLeaf branch — see that helper for the doc-level lift
// rationale.
//
// TODO aliszka:nested_filtering — make the non-root lift lazy when
// downstream only consumes doc-level output; MaskRootLeaf(mPin) suffices
// in that case (the same shortcut pinned-root already uses).
func leafPinnedPositive(idx *fastPathIndex, valuePath string, value any, pins []pinSpec) *fastPathResult {
	if len(pins) == 0 {
		panic("multi-pin needs at least one pin")
	}
	a := cloneOrEmpty(idx.values[valuePath][value])
	for _, pin := range pins {
		a.And(idx.idx[pin.path][pin.index])
	}
	return pinnedFromValueSet(idx, a, valuePath, pins)
}

// pinnedFromValueSet runs the shared pin-lift / Raw-reconstruction
// stage of every value-based pinned helper (leafPinnedPositive plus
// the pinned ContainsAny / ContainsAll variants below). Callers
// supply the already-aggregated value raw `a` — single bucket,
// union of buckets, or intersection of buckets — narrowed by every
// pin's _idx raw. The function takes care of mPin, the narrowed-
// anchor lift, and Raw = (a ANDNOT scopeAnchor) ∪ Witnesses.
//
// LOAD-BEARING CALLER CONTRACT: `a` MUST already be intersected with
// every pin's _idx raw before the call. The pin-narrowing is what
// guarantees that any bit in `a` lies inside the pinned subtree —
// helpers like leafPinnedContainsAny that build `a` via union-then-pin
// rely on this to keep the result owner-correct (cross-element bits
// from values[v] survive the union but get filtered out by the
// per-pin ∩ idx[pin]). Skipping the narrowing for any pin would let
// out-of-subtree bits flow through to Witnesses; future helpers that
// touch this function must preserve the contract.
//
// Result shape matches leafPinnedPositive: Scope at the outermost pin's
// parent, Witnesses at Scope, Raw preserving chain bits inside the
// pinned subtree, Ceiling = Scope (no claim above — intermediate-
// level ghosts below Scope can persist, and the chain bits stripped /
// re-added by the formula don't preserve above-Scope authenticity).
//
// When the outermost pin is at the property root (parent = pathRoot),
// the standard LiftToAncestor predecessor scan can't run: doc-level
// markers live at rootIdx=0 while value selfMarkers carry the value's
// rootIdx, putting them in different (rootIdx, docID) buckets. The
// doc-level lift is instead computed via MaskRootLeaf, which zeroes
// root+leaf bits and yields the docIDs directly. Equivalent because
// Encode(0, 0, docID) == docID under the position layout, so the
// docUniverse's positions match MaskRootLeaf's output exactly — the
// trustworthy-scope invariant Raw ∩ anchorAt(pathRoot) == Witnesses
// holds without a real predecessor scan.
func pinnedFromValueSet(idx *fastPathIndex, a *sroar.Bitmap, valuePath string, pins []pinSpec) *fastPathResult {
	innerAnchor := idx.anchorAt(parentPath(valuePath))
	scope := parentPath(pins[0].path)

	mPin := a.Clone().And(innerAnchor)
	var witnesses, raw *sroar.Bitmap
	if scope == pathRoot {
		// MaskRootLeaf strips root+leaf bits, projecting selfMarkers to
		// plain docIDs (= Encode(0, 0, docID), matching docUniverse).
		witnesses, _ = idx.ops.MaskRootLeaf(mPin)
		// Raw preserves the pinned-subtree chain bits from `a` and
		// OR-s in the doc-level positions so the trustworthy-scope
		// invariant holds (docUniverse ∩ a is empty — `a` carries no
		// doc-level markers — so adding Witnesses is required).
		raw = a.Or(witnesses)
	} else {
		scopeAnchor := idx.anchorAt(scope)
		narrowedAnchor := cloneOrEmpty(scopeAnchor).And(a)
		witnesses, _ = idx.ops.LiftToAncestor(mPin, narrowedAnchor)
		raw = a.AndNot(scopeAnchor).Or(witnesses)
	}

	return &fastPathResult{
		Scope:     scope,
		Raw:       raw,
		Witnesses: witnesses,
		Ceiling:   scope,
	}
}

// leafPinnedNot realizes `NOT <pinned path>.x = value`. Dispatches
// internally based on the pin chain layout — same shape as
// leafPinnedIsNullTrue: //
//
//   - No gap (fully pinned at every array level between Scope and the value
//     path's element scope): universe-subtract of leafPinnedPositive.
//     The pinned slot is unique per Scope-element, so owner-level NOT
//     coincides with per-element NOT.
//
//   - Gap present (intermediate pin): use the per-element NOT formula —
//     witnesses are the elements under the pinned subtree that fail the
//     predicate, lifted to Scope, unioned with Scope-elements that lack the
//     outermost pin slot entirely.
//
// Missing pinned slot counts as success in both branches.
func leafPinnedNot(idx *fastPathIndex, valuePath string, value any, pins []pinSpec) *fastPathResult {
	if !hasPinGap(pins, valuePath) {
		return negate(idx, leafPinnedPositive(idx, valuePath, value, pins))
	}
	return perElementNotValue(idx, valuePath, value, pins)
}

// hasPinGap reports whether the pin chain has an unpinned array level
// between the Scope and the value path's element scope. The pin
// count must equal the number of array levels descending from Scope+1
// down to ElementScope; any mismatch means an intermediate level is
// unpinned. Used to choose between owner-level negation (no gap) and
// per-element negation (gap present).
func hasPinGap(pins []pinSpec, valuePath string) bool {
	if len(pins) == 0 {
		return false
	}
	elementScope := parentPath(valuePath)
	scope := parentPath(pins[0].path)
	levelsBetween := pathDepth(elementScope) - pathDepth(scope)
	return len(pins) != levelsBetween
}

// perElementNotValue computes `NOT <pinned path>.x = value` directly at
// the per-element scope when the pin chain has a gap. Wraps
// perElementNotFromSubtractands with the value-keyed bucket as the
// subtractand: witnesses are the elements that don't match the given
// value within the pinned subtree.
//
// Worked example: `NOT garages[1].cars.make=bmw` on the L2 schema —
// ElementScope=cars (unpinned), pins=[(garages, 1)], Scope=countries.
// A country passes if it contains any car under garages[1] whose make is
// not bmw, OR if it has no garages[1] slot at all.
func perElementNotValue(idx *fastPathIndex, valuePath string, value any, pins []pinSpec) *fastPathResult {
	return perElementNotFromSubtractands(idx, valuePath, pins, idx.values[valuePath][value])
}

// perElementNotExists is the IS NULL true analogue of perElementNotValue: // witnesses are elements within the pinned subtree whose value-path
// leaf is absent. The subtractand is the _exists bucket of the value
// path instead of a specific value bucket.
func perElementNotExists(idx *fastPathIndex, valuePath string, pins []pinSpec) *fastPathResult {
	return perElementNotFromSubtractands(idx, valuePath, pins, idx.existsAt(valuePath))
}

// perElementNotFromSubtractands is the shared body of perElementNotValue,
// perElementNotExists, and leafPinnedContainsNone. Callers differ in how
// many bitmaps they subtract from the pin-narrowed element universe: // single-value NOT passes one, IS NULL true passes one, ContainsNone
// passes one per listed value.
//
// Raw algebra: //
//
//	witnesses      := ⋂_i _idx[pins[i]] ∩ _anchor(ElementScope) ANDNOT ⋃ subtractands
//	                  // candidates inside the pinned subtree that the
//	                  // caller wants to negate (don't match any value,
//	                  // or lack the value-path leaf)
//	witnessesAtScope  := LiftToAncestor(witnesses, _anchor(Scope))
//	                  // Scope-elements with at least one such candidate
//	scopeMissingPin   := _anchor(Scope) ANDNOT _idx[outermost pin]
//	                  // Scope-elements that lack the outermost pin slot
//	Witnesses   := witnessesAtScope ∪ scopeMissingPin
//	Raw           := (_exists(Scope) ANDNOT _anchor(Scope)) ∪ Witnesses
//	                  // satisfies trustworthy-scope invariant
//	                  // Raw ∩ _anchor(Scope) == Witnesses
//
// The union of subtractands is computed implicitly via chained AndNot: // A − (B ∪ C) = (A − B) − C, so we never materialise the union.
//
// Algebraic shortcuts (vs. the textbook formula): //
//
//   - Skipping elementPositive: `(pinNarrow ∩ _anchor) ANDNOT
//     elementPositive` simplifies to `(pinNarrow ∩ _anchor) ANDNOT
//     subtractand` because the subtractand's `pinNarrow ∩ _anchor`
//     factors are already present in the minuend.
//
//   - Skipping tsWithPin: `A ANDNOT (A ∩ B) = A ANDNOT B`, so
//     `scopeMissingPin = _anchor(Scope) ANDNOT _idx[outermost]` directly.
//
//   - Skipping the second LiftToAncestor: the outermost pin's _idx
//     raw already carries chain bits through Scope, so the anchor
//     intersection is equivalent and cheaper than a full lift.
//
// Net: one LiftToAncestor and three clones, no per-element correlation
// trade-off.
func perElementNotFromSubtractands(idx *fastPathIndex, valuePath string, pins []pinSpec, subtractands ...*sroar.Bitmap) *fastPathResult {
	elementScope := parentPath(valuePath)
	scope := parentPath(pins[0].path)

	// elementWitnesses = ⋂_i _idx[pins[i]] ∩ _anchor(ElementScope) ANDNOT ⋃ subtractands
	elementWitnesses := cloneOrEmpty(idx.idx[pins[0].path][pins[0].index])
	for _, p := range pins[1:] {
		elementWitnesses.And(idx.idx[p.path][p.index])
	}
	elementWitnesses.And(idx.anchorAt(elementScope))
	for _, sub := range subtractands {
		if sub != nil {
			elementWitnesses.AndNot(sub)
		}
	}

	outermost := pins[0]
	var witnessesAtScope, scopeMissingPin *sroar.Bitmap
	if scope == pathRoot {
		// Doc-level lift: predecessor scan would scan disjoint buckets
		// (elementWitnesses carry rootIdx=pin-slot while docUniverse
		// carries rootIdx=0). MaskRootLeaf projects per-element
		// non-matches straight down to plain docIDs. scopeMissingPin
		// compares like-to-like at the doc level: the outermost pin's
		// _idx raw also gets MaskRootLeaf-ed so the AndNot against
		// docUniverse actually subtracts (without this, the two
		// operands live in disjoint position spaces and the AndNot is a
		// no-op — the original bug had `cars[1] doesn't exist` claim
		// all docs).
		witnessesAtScope, _ = idx.ops.MaskRootLeaf(elementWitnesses)
		pinDocs, _ := idx.ops.MaskRootLeaf(idx.idx[outermost.path][outermost.index])
		scopeMissingPin = cloneOrEmpty(idx.docUniverse).AndNot(pinDocs)
	} else {
		scopeAnchor := idx.anchorAt(scope)
		// witnessesAtScope = LiftToAncestor(elementWitnesses, _anchor(Scope))
		witnessesAtScope, _ = idx.ops.LiftToAncestor(elementWitnesses, scopeAnchor)
		// scopeMissingPin = _anchor(Scope) ANDNOT _idx[outermost pin]
		scopeMissingPin = cloneOrEmpty(scopeAnchor).AndNot(idx.idx[outermost.path][outermost.index])
	}

	// witnesses = witnessesAtScope ∪ scopeMissingPin (mutates witnessesAtScope).
	witnesses := witnessesAtScope.Or(scopeMissingPin)
	// Raw = (existsAt(Scope) ANDNOT anchorAt(Scope)) ∪ Witnesses. At
	// pathRoot both reduce to docUniverse so the AndNot zeroes and
	// raw == witnesses — correct because there's no scope above
	// pathRoot to carry chain bits.
	raw := cloneOrEmpty(idx.existsAt(scope)).AndNot(idx.anchorAt(scope)).Or(witnesses)

	return &fastPathResult{
		Scope:     scope,
		Raw:       raw,
		Witnesses: witnesses,
		Ceiling:   scope,
	}
}

// leafContainsAny realizes `path ContainsAny [values]` as the union of
// per-value match bitmaps. Each list value is tokenized and reduced to
// its per-value Mᵢ via tokenizedMatchBitmap (single-token values
// collapse to a single bucket; multi-token values become the AND of
// per-token buckets). The outer OR then unions those Mᵢ together —
// production's extractContains → extractTokenizableProp does the
// same composition at the wire layer.
//
// Semantics: a Scope-element satisfies ContainsAny if it satisfies at
// least one list value as a whole — including multi-token values
// where ALL tokens of THAT value must be present (parent-Scope same
// element).
//
// Result shape unchanged from the previous untokenized version:
// Scope=parent(path), Witnesses=Raw ∩ _anchor(Scope), Ceiling=pathRoot.
//
// Panics on an empty value list.
func leafContainsAny(idx *fastPathIndex, path string, values ...any) *fastPathResult {
	if len(values) == 0 {
		panic("leafContainsAny requires at least one value")
	}
	scope := parentPath(path)
	raw := sroar.NewBitmap()
	for _, v := range values {
		raw.Or(tokenizedMatchBitmap(idx, path, v))
	}
	return &fastPathResult{
		Scope:     scope,
		Raw:       raw,
		Witnesses: raw.Clone().And(idx.anchorAt(scope)),
		Ceiling:   pathRoot,
	}
}

// leafContainsAll realizes `path ContainsAll [values]` as the
// intersection of per-value match bitmaps. Each list value reduces to
// its per-value Mᵢ via tokenizedMatchBitmap; the outer AND then
// intersects them. Associativity makes this equivalent to flattening
// every token across every value into one big AND (multi-token values
// just contribute extra AND terms), but the per-value structure keeps
// ContainsAll readable as ContainsAll.
//
// Semantics: a Scope-element satisfies ContainsAll if it satisfies
// every list value. For tokenized multi-token values, "satisfies"
// already means "all tokens of that value present" via Mᵢ.
//
// Result shape unchanged: Scope=parent(path), Witnesses=Raw ∩
// _anchor(Scope), Ceiling=Scope (same-Scope AND can't claim cleanness
// above).
//
// Panics on an empty value list.
func leafContainsAll(idx *fastPathIndex, path string, values ...any) *fastPathResult {
	if len(values) == 0 {
		panic("leafContainsAll requires at least one value")
	}
	scope := parentPath(path)
	raw := tokenizedMatchBitmap(idx, path, values[0])
	for _, v := range values[1:] {
		raw.And(tokenizedMatchBitmap(idx, path, v))
	}
	return &fastPathResult{
		Scope:     scope,
		Raw:       raw,
		Witnesses: raw.Clone().And(idx.anchorAt(scope)),
		Ceiling:   scope,
	}
}

// leafPinnedContainsAny realizes `<pinned path>.x ContainsAny [values]`
// — for example `garages[1].cars[2].colors ContainsAny [red, blue]`.
// Mirrors the optimisation used by the unpinned leafContainsAny: // compute the union of per-value buckets directly, narrow with every
// pin's _idx raw, then hand off to the shared pin-lift stage.
//
// Panics on an empty pin list (the function would have no Scope)
// or an empty value list (no positive witnesses possible).
func leafPinnedContainsAny(idx *fastPathIndex, valuePath string, pins []pinSpec, values ...any) *fastPathResult {
	if len(pins) == 0 {
		panic("pinned ContainsAny needs at least one pin")
	}
	if len(values) == 0 {
		panic("pinned ContainsAny requires at least one value")
	}
	a := sroar.NewBitmap()
	for _, v := range values {
		if bm := idx.values[valuePath][v]; bm != nil {
			a.Or(bm)
		}
	}
	for _, pin := range pins {
		a.And(idx.idx[pin.path][pin.index])
	}
	return pinnedFromValueSet(idx, a, valuePath, pins)
}

// leafPinnedContainsAll realizes `<pinned path>.x ContainsAll [values]`
// — for example `cars[1].colors ContainsAll [red, blue]`, "cars[1]'s
// colors array contains both red and blue."
//
// Computes the intersection of per-value buckets directly, then
// narrows with every pin's _idx raw before the pin-lift stage.
// Bails out early when any value is absent from the index — the
// intersection is already empty.
//
// Panics on an empty pin list or empty value list.
func leafPinnedContainsAll(idx *fastPathIndex, valuePath string, pins []pinSpec, values ...any) *fastPathResult {
	if len(pins) == 0 {
		panic("pinned ContainsAll needs at least one pin")
	}
	if len(values) == 0 {
		panic("pinned ContainsAll requires at least one value")
	}
	a := cloneOrEmpty(idx.values[valuePath][values[0]])
	for _, v := range values[1:] {
		bm := idx.values[valuePath][v]
		if bm == nil {
			a = sroar.NewBitmap()
			break
		}
		a.And(bm)
	}
	for _, pin := range pins {
		a.And(idx.idx[pin.path][pin.index])
	}
	return pinnedFromValueSet(idx, a, valuePath, pins)
}

// leafContainsNone realizes `path ContainsNone [values]` as the
// universe (anchor at Scope) minus the union of per-value match
// bitmaps. Each list value reduces to its per-value Mᵢ via
// tokenizedMatchBitmap; chained AndNot then subtracts each Mᵢ in
// turn — algebraically equivalent to `anchor AndNot ⋃ Mᵢ`.
//
// Algebra:
//
//	Witnesses = _anchor(Scope) ANDNOT ⋃_v Mᵥ
//	             = ((_anchor(Scope) ANDNOT M_1) ANDNOT M_2) ... ANDNOT M_N
//	Raw       = (_exists(Scope) ANDNOT _anchor(Scope)) ∪ Witnesses
//	             (satisfies Raw ∩ _anchor(Scope) == Witnesses)
//
// Correctness note for multi-token list values: the per-value AND
// MUST be materialized inside tokenizedMatchBitmap and only then
// subtracted. Folding individual token buckets directly into the
// outer AndNot loop would compute `anchor AndNot ⋃ tokens` instead
// of `anchor AndNot ⋃ (per-value AND of tokens)` — semantically
// stricter (closer to "doc has none of the tokens individually"
// rather than "doc has none of the values").
//
// Semantics: a Scope-element satisfies ContainsNone if it doesn't
// satisfy any list value as a whole. For tokenized text[] the
// per-element AND inside Mᵢ is parent-Scope (same lenient rule as
// ContainsAny / ContainsAll).
//
// Result shape unchanged: Ceiling=Scope (owner-level negation can't
// claim cleanness above).
//
// Panics on an empty value list.
func leafContainsNone(idx *fastPathIndex, path string, values ...any) *fastPathResult {
	if len(values) == 0 {
		panic("leafContainsNone requires at least one value")
	}
	scope := parentPath(path)
	scopeAnchor := idx.anchorAt(scope)
	witnesses := cloneOrEmpty(scopeAnchor)
	for _, v := range values {
		witnesses.AndNot(tokenizedMatchBitmap(idx, path, v))
	}
	raw := cloneOrEmpty(idx.existsAt(scope)).AndNot(scopeAnchor).Or(witnesses)
	return &fastPathResult{
		Scope:     scope,
		Raw:       raw,
		Witnesses: witnesses,
		Ceiling:   scope,
	}
}

// leafPinnedContainsNone realizes `<pinned path>.x ContainsNone [values]`
// as owner-level negation of pinned ContainsAny: a Scope-element passes
// only if NO descendant under the pinned subtree matches any of the
// listed values.
//
// Unlike leafPinnedNot / leafPinnedIsNullTrue, this helper does NOT
// dispatch on hasPinGap. The operator name "Contains None" is a
// universal claim about the entire array — every element must not
// match — which is the owner-level reading. A per-element dispatch
// would let a single non-matching element witness pass the predicate
// even when other elements match, contradicting the "none" semantic.
//
// There's no escape hatch at this layer for the per-element-existential
// reading ("some element under the pin doesn't match any value"). For a
// single value, users can write the equivalent `NOT pinned X=v` — at
// intermediate pin that routes through perElementNotFromSubtractands and
// produces the per-element result. For multiple values the only
// expression today is `NOT pinned X=a AND NOT pinned X=b`, which at
// intermediate pin yields owner-level (the AND lands at the pin's
// parent scope without per-element correlation). A first-class
// per-element-multi-value operator would need new planner syntax; until
// then ContainsNone is owner-level, full stop.
//
// Panics on an empty pin list or empty value list.
func leafPinnedContainsNone(idx *fastPathIndex, valuePath string, pins []pinSpec, values ...any) *fastPathResult {
	if len(pins) == 0 {
		panic("pinned ContainsNone needs at least one pin")
	}
	if len(values) == 0 {
		panic("pinned ContainsNone requires at least one value")
	}
	return negate(idx, leafPinnedContainsAny(idx, valuePath, pins, values...))
}

// ---------------------------------------------------------------------------
// Merge builders
// ---------------------------------------------------------------------------

// commonScope returns the longest common ancestor of two qualified path
// strings (segments delimited by "."). For schemas with linear hierarchy
// it's either a prefix of both inputs or equal to one of them. When one
// input is pathRoot, or the inputs share no propName-level prefix, the
// LCA is pathRoot — the doc-level sentinel above every real path.
func commonScope(a, b string) string {
	if a == b {
		return a
	}
	if a == pathRoot || b == pathRoot {
		return pathRoot
	}
	aParts := strings.Split(a, ".")
	bParts := strings.Split(b, ".")
	n := min(len(bParts), len(aParts))
	i := 0
	for i < n && aParts[i] == bParts[i] {
		i++
	}
	if i == 0 {
		return pathRoot
	}
	return strings.Join(aParts[:i], ".")
}

// andLeaves performs an AND merge of two leaf results. See the AND merge
// dispatch matrix near the top of this file for the full table of cases;
// inline comments below mark each branch.
func andLeaves(idx *fastPathIndex, left, right *fastPathResult) *fastPathResult {
	if left.Scope == right.Scope {
		return andAtScope(idx, left, right, left.Scope, left.Scope)
	}

	common := commonScope(left.Scope, right.Scope)

	// Siblings (LCA is above both) → lift both up, recurse as same-scope.
	// Cheap lift preserves each operand's Ceiling (typically "" for
	// positive leaves). The subsequent same-Scope AND sets ceiling=Scope,
	// so the recursive result naturally has Ceiling == Scope and
	// ceilingAboveScope() returns false — preventing the dispatch from
	// later incorrectly broadcasting into a sibling subtree the inner
	// AND never populated.
	if common != left.Scope && common != right.Scope {
		return andLeaves(idx,
			liftToScope(idx, left, common),
			liftToScope(idx, right, common))
	}

	// One is ancestor of the other.
	var ancestor, child *fastPathResult
	if common == left.Scope {
		ancestor, child = left, right
	} else {
		ancestor, child = right, left
	}

	if ancestor.ceilingAboveScope() {
		// A's authenticity range extends strictly above its own Scope,
		// which (by the Ceiling convention) means A was constructed
		// with the elementPositions encoding — chain + selfMarker +
		// descendantSelves. So A's Raw broadcasts authentic descendant
		// bits down through child.Scope. Merge at child.Scope.
		//
		// The cleanness CEILING for the AND is ancestor.Scope (not the
		// merge scope child.Scope): at scopes in [child.Scope, A.Scope]
		// the chain bits in either operand are per-element selfMarkers
		// and the intersection is genuine same-element correlation;
		// strictly above A.Scope the chain bits are shared and the AND
		// can produce owner-level ghosts. The child may carry its own
		// deeper Ceiling (e.g. a compound child whose lift bottomed
		// out below A.Scope) — take the deeper of the two so the AND
		// inherits the most restrictive cleanness limit. ancestor.Ceiling
		// can't participate: it's bounded by ancestor.Scope (Ceiling
		// ≤ Scope by the trustworthy-scope invariant), so it can never
		// be deeper than the ceiling we're already starting from.
		ceiling := deepestPath(ancestor.Scope, child.Ceiling)
		return andAtScope(idx, child, ancestor, child.Scope, ceiling)
	}
	if ancestor.Scope == pathRoot {
		// pathRoot is a virtual scope above the property root — descendant
		// Bitmaps carry no Encode(0, 0, docID) bits at all, so the cheap-
		// merge check below (which assumes child.Raw is trustworthy at
		// A.Scope when child.Ceiling ≤ A.Scope) would silently AND
		// against an empty intersection. Lift child explicitly so its
		// doc-level bits are present before merging.
		lifted := liftToScope(idx, child, pathRoot)
		return andAtScope(idx, ancestor, lifted, pathRoot, pathRoot)
	}
	if pathDepth(child.Ceiling) <= pathDepth(ancestor.Scope) {
		// C's clean range reaches A.Scope — its chain bits at A.Scope are
		// already authentic. Intersect directly at A.Scope, no lift needed.
		// A.Raw is trustworthy at A.Scope by the scope invariant. Ceiling =
		// merge scope (= A.Scope): above A.Scope, A's own ghosts persist and the
		// merge can't recover them.
		//
		// Distinct from the simpler `child.ceilingAboveScope()` test because we
		// need C to be clean SPECIFICALLY AT A.Scope, not just somewhere
		// between C.Scope and root. A compound C (e.g. the result of an
		// ancestor+child AND with Ceiling=somewhere-between) may be
		// ceilingAboveScope yet still have ghosts at A.Scope — in which case we
		// must lift.
		return andAtScope(idx, ancestor, child, ancestor.Scope, ancestor.Scope)
	}
	// C's clean range stops below A.Scope (or C is fully unclean above Scope).
	// C's bits at A.Scope may include ghosts; ANDing directly would let
	// those coincide with A's authentic Witnesses and leak. Lift C up to A.Scope
	// so its A.Scope bits are rebuilt from authentic Witnesses
	// (LiftToAncestor wipes ghost bits), then merge same-scope at A.Scope.
	lifted := liftToScope(idx, child, ancestor.Scope)
	return andAtScope(idx, ancestor, lifted, ancestor.Scope, ancestor.Scope)
}

// andAtScope is the shared core merge formula used by every branch of
// andLeaves once the merge scope and cleanness ceiling are known.
// `scope` is the merge scope (= result.Scope). `ceiling` is what
// result.Ceiling will be set to — the shallowest scope at which the
// AND's raw intersection is still per-element clean.
//
// For every same-Scope-or-LCA caller (5 of the 6 call sites)
// ceiling == scope: the operands' Scopes equal scope by dispatch and
// their Ceiling fields are bounded by their Scopes, so the cleanness
// ceiling can never rise above scope. For the broadcasting branch
// (1 call site) ceiling is computed at the caller via
// deepestPath(ancestor.Scope, child.Ceiling) because there the
// child's Ceiling can sit between ancestor.Scope and child.Scope
// and we want the deepest restriction to win.
//
// the intersection of two authentic-descendant bitmaps is authentic at
// every scope below scope, but if either operand carries bogus
// descendants the intersection may pick them up.
func andAtScope(idx *fastPathIndex, left, right *fastPathResult, scope, ceiling string) *fastPathResult {
	raw := left.Raw.Clone().And(right.Raw)
	// anchorAt: scope is pathRoot when both operands sit at the doc level
	// (pinned-root same-Scope AND, or any AND lifted to the pathRoot LCA).
	witnesses := raw.Clone().And(idx.anchorAt(scope))
	return &fastPathResult{
		Scope:     scope,
		Raw:       raw,
		Witnesses: witnesses,
		Ceiling:   ceiling,
	}
}

// liftToScope re-wraps a leaf result at a higher (ancestor) Scope.
// Useful when sibling leaves with different natural witness scopes share a
// common ancestor where the merge should land (e.g. `cars.accessories.type`
// at Scope=accessories and `cars.tires.width` at Scope=tires both
// merge at cars).
//
// The lift method depends on whether targetScope falls within the operand's
// clean range: //
//
//   - depth(leaf.Ceiling) <= depth(targetScope) (= targetScope is at-or-
//     below leaf.Ceiling, i.e. inside the clean range): Raw is already
//     authentic at targetScope, so anchor intersection suffices — no ghost
//     ancestors to worry about. Raw is reused unchanged. The lifted
//     result KEEPS the operand's higher Ceiling: Raw is byte-for-byte
//     the same raw, so every cleanness claim the original made still
//     holds. Only Scope changes.
//
//   - depth(leaf.Ceiling) > depth(targetScope) (= targetScope is strictly
//     above leaf.Ceiling, outside the clean range): Raw carries ghosts
//     at intermediate scopes. Use a real predecessor lift (LiftToAncestor)
//     on Witnesses to project to targetScope, then reconstruct Raw at
//     the new scope by stripping target-scope ghost bits and OR-ing back
//     the authentic lifted Witnesses. The lift only rebuilds the targetScope-level
//     bits; scopes strictly above targetScope keep whatever ghosts the
//     original Raw carried. The lifted result claims cleanness only at
//     targetScope itself — Ceiling = targetScope.
//
// targetScope must be an ancestor of leaf.Scope in the schema (or
// equal, in which case the input is returned unchanged). Caller is
// responsible for ensuring this — no schema lookup happens here.
func liftToScope(idx *fastPathIndex, leaf *fastPathResult, targetScope string) *fastPathResult {
	if leaf.Scope == targetScope {
		return leaf
	}
	if targetScope == pathRoot {
		// Doc-level lift: predecessor scan doesn't apply (docUniverse's
		// rootIdx=0 puts its positions in a different (rootIdx, docID)
		// bucket than any descendant selfMarker). MaskRootLeaf projects
		// directly to plain docIDs, which equal Encode(0, 0, docID)
		// under the position layout and match docUniverse entries
		// verbatim — see pinnedFromValueSet for the equivalence reasoning.
		//
		// Ceiling becomes pathRoot, NOT the operand's original CA.
		// AssignPositions emits only positions with rootIdx ≥ 1 and
		// leafIdx ≥ 1 (both are 1-based), so leaf.Raw never carries
		// Encode(0, 0, d) bits. OR-ing in witnesses introduces authentic
		// doc-level bits without polluting anything that was already
		// there. The lifted Raw is therefore authentic at pathRoot
		// (newly, via witnesses) and remains authentic at every scope the
		// operand was originally clean at — the shallowest scope in the
		// new clean range is pathRoot.
		witnesses, _ := idx.ops.MaskRootLeaf(leaf.Witnesses)
		raw := leaf.Raw.Clone().Or(witnesses)
		return &fastPathResult{
			Scope:     pathRoot,
			Raw:       raw,
			Witnesses: witnesses,
			Ceiling:   pathRoot,
		}
	}
	targetAnchor := idx.anchorAt(targetScope)
	var raw, witnesses *sroar.Bitmap
	var ceiling string
	if pathDepth(leaf.Ceiling) <= pathDepth(targetScope) {
		// Cheap lift: Raw is unchanged, so it stays authentic at every
		// scope the operand was originally clean at (down to leaf.Ceiling,
		// which is at-or-shallower-than targetScope). Preserving Ceiling
		// matters for OR: keeping a shallower Ceiling here lets the OR
		// of a deep operand with a shallow Ceiling=pathRoot operand retain its
		// higher cleanness reach.
		raw = leaf.Raw
		witnesses = leaf.Raw.Clone().And(targetAnchor)
		ceiling = leaf.Ceiling
	} else {
		// Real lift: targetScope is above the operand's clean range. Only
		// the targetScope-level bits get rebuilt; everything above remains
		// ghost-prone. The lifted result can only claim cleanness at
		// targetScope.
		witnesses, _ = idx.ops.LiftToAncestor(leaf.Witnesses, targetAnchor)
		raw = leaf.Raw.Clone().AndNot(targetAnchor).Or(witnesses)
		ceiling = targetScope
	}
	return &fastPathResult{
		Scope:     targetScope,
		Raw:       raw,
		Witnesses: witnesses,
		Ceiling:   ceiling,
	}
}

// orLeaves performs an OR merge of two leaf results. See the OR merge
// dispatch matrix near the top of this file for the full table of cases.
// The implementation is a single rule: lift both operands to their LCA
// (no-op when already there; cheap anchor-intersect when target is within
// the operand's clean range; full LiftToAncestor otherwise) and union
// them. The clean-range shortcut is entirely encoded inside liftToScope.
//
// result.Ceiling = deepest of the two lifted operands' CleanScopes.
// Because the cheap-lift path preserves the operand's original (higher)
// Ceiling, an OR of two Ceiling=pathRoot-equivalent operands keeps cleanness
// up to the property root — the union of authentic bits at every scope
// is itself authentic. An OR where one operand has Ceiling below LCA
// (e.g. the result of a deeper AND) gets the deeper of the two as its
// own ceiling.
func orLeaves(idx *fastPathIndex, left, right *fastPathResult) *fastPathResult {
	lca := commonScope(left.Scope, right.Scope)
	l := liftToScope(idx, left, lca)
	r := liftToScope(idx, right, lca)
	return &fastPathResult{
		Scope:     lca,
		Raw:       l.Raw.Clone().Or(r.Raw),
		Witnesses: l.Witnesses.Clone().Or(r.Witnesses),
		Ceiling:   deepestPath(l.Ceiling, r.Ceiling),
	}
}

// andN folds N operands via andLeaves, sorted by Scope depth (deepest
// first) with Ceiling=pathRoot breaking ties before Ceiling=Scope. The
// pairwise dispatch is order-invariant for correctness; the sort
// minimises intermediate lifts:
//
//   - Deepest-first keeps consecutive same-Scope operands adjacent so
//     the same-Scope branch folds them with no lifts. The accumulator
//     stays at the deepest Scope as long as subsequent shallower
//     operands have Ceiling=pathRoot (broadcasting branch).
//
//   - Ceiling=pathRoot-first within a same-Scope run keeps the running
//     accumulator clean for as long as possible across ancestor-child
//     merges, favouring the broadcasting branch.
//
// Panics on empty input. A single-operand call returns the operand.
//
// TODO aliszka:nested_filtering — bucket Ceiling=Scope operands by Scope
// and fold each bucket locally (no lift) before lifting per-bucket
// results to the LCA. Caps the total LiftToAncestor count at the number
// of buckets instead of the number of Ceiling=Scope operands.
func andN(idx *fastPathIndex, operands ...*fastPathResult) *fastPathResult {
	if len(operands) == 0 {
		panic("andN requires at least one operand")
	}
	if len(operands) == 1 {
		return operands[0]
	}
	sorted := make([]*fastPathResult, len(operands))
	copy(sorted, operands)
	sort.SliceStable(sorted, func(i, j int) bool {
		di := pathDepth(sorted[i].Scope)
		dj := pathDepth(sorted[j].Scope)
		if di != dj {
			return di > dj
		}
		// Within a same-Scope run, prefer operands whose Raw is clean at more
		// scopes above Scope (shallower Ceiling). Mirrors the old
		// Ceiling=pathRoot-first tiebreaker: cleaner operands stay at the front
		// so the running accumulator keeps its cleanest reach for as long
		// as possible across subsequent ancestor merges.
		return pathDepth(sorted[i].Ceiling) < pathDepth(sorted[j].Ceiling)
	})
	result := sorted[0]
	for i := 1; i < len(sorted); i++ {
		result = andLeaves(idx, result, sorted[i])
	}
	return result
}

// orN folds N operands via orLeaves, sorted by Scope depth (deepest
// first) with Ceiling=pathRoot breaking ties before Ceiling=Scope.
// Order-invariant for correctness; the sort minimises intermediate lifts.
//
// Unlike andN, OR always lands at the LCA — merging below would lose
// each operand's "container exists" contribution at the higher scope.
// The sort still helps:
//
//   - Deepest-first clusters same-Scope operands at the front so the
//     same-Scope OR is a pure union with no lift.
//
//   - Ceiling=pathRoot-first lets the accumulator promote via cheap
//     anchor-intersect lift when subsequent shallower operands push it
//     to a higher LCA.
//
// Panics on empty input. A single-operand call returns the operand.
//
// TODO aliszka:nested_filtering — see andN's TODO. Same grouping
// optimisation caps the LiftToAncestor count at the number of
// Ceiling=Scope-bucket lifts instead of per-operand.
func orN(idx *fastPathIndex, operands ...*fastPathResult) *fastPathResult {
	if len(operands) == 0 {
		panic("orN requires at least one operand")
	}
	if len(operands) == 1 {
		return operands[0]
	}
	sorted := make([]*fastPathResult, len(operands))
	copy(sorted, operands)
	sort.SliceStable(sorted, func(i, j int) bool {
		di := pathDepth(sorted[i].Scope)
		dj := pathDepth(sorted[j].Scope)
		if di != dj {
			return di > dj
		}
		// Within a same-Scope run, prefer operands whose Raw is clean at more
		// scopes above Scope (shallower Ceiling). Mirrors the old
		// Ceiling=pathRoot-first tiebreaker: cleaner operands stay at the front
		// so the running accumulator keeps its cleanest reach for as long
		// as possible across subsequent ancestor merges.
		return pathDepth(sorted[i].Ceiling) < pathDepth(sorted[j].Ceiling)
	})
	result := sorted[0]
	for i := 1; i < len(sorted); i++ {
		result = orLeaves(idx, result, sorted[i])
	}
	return result
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func parentPath(path string) string {
	if path == pathRoot {
		return pathRoot
	}
	if i := strings.LastIndex(path, "."); i >= 0 {
		return path[:i]
	}
	return pathRoot
}

// pathDepth returns the number of segments in a dot-separated path.
// pathRoot has depth 0; "countries" has depth 1;
// "countries.garages.cars" has depth 3.
func pathDepth(path string) int {
	if path == pathRoot {
		return 0
	}
	return strings.Count(path, ".") + 1
}

func cloneOrEmpty(bm *sroar.Bitmap) *sroar.Bitmap {
	if bm == nil {
		return sroar.NewBitmap()
	}
	return bm.Clone()
}

// docIDs runs Witnesses through the real BitmapOps.MaskRootLeaf so the
// projection path itself is under test.
func (i *fastPathIndex) docIDs(r *fastPathResult) []uint64 {
	if r.Witnesses == nil || r.Witnesses.IsEmpty() {
		return nil
	}
	doc, _ := i.ops.MaskRootLeaf(r.Witnesses)
	return doc.ToArray()
}

// ---------------------------------------------------------------------------
// L2 schema and fixtures (countries.garages.cars)
// ---------------------------------------------------------------------------

func l2Schema() *models.Property {
	tx := func(name string) *models.NestedProperty {
		return &models.NestedProperty{
			Name: name, DataType: []string{string(schema.DataTypeText)},
			Tokenization: models.PropertyTokenizationField,
		}
	}
	in := func(name string) *models.NestedProperty {
		return &models.NestedProperty{Name: name, DataType: []string{string(schema.DataTypeInt)}}
	}
	txArr := func(name string) *models.NestedProperty {
		return &models.NestedProperty{
			Name: name, DataType: []string{string(schema.DataTypeTextArray)},
			Tokenization: models.PropertyTokenizationField,
		}
	}
	txWord := func(name string) *models.NestedProperty {
		return &models.NestedProperty{
			Name: name, DataType: []string{string(schema.DataTypeText)},
			Tokenization: models.PropertyTokenizationWord,
		}
	}
	txArrWord := func(name string) *models.NestedProperty {
		return &models.NestedProperty{
			Name: name, DataType: []string{string(schema.DataTypeTextArray)},
			Tokenization: models.PropertyTokenizationWord,
		}
	}
	objArr := func(name string, nested ...*models.NestedProperty) *models.NestedProperty {
		return &models.NestedProperty{
			Name:             name,
			DataType:         []string{string(schema.DataTypeObjectArray)},
			NestedProperties: nested,
		}
	}
	return &models.Property{
		Name:     "countries",
		DataType: []string{string(schema.DataTypeObjectArray)},
		NestedProperties: []*models.NestedProperty{
			objArr("garages",
				tx("city"),
				objArr("cars",
					in("year"),
					tx("make"),
					tx("model"),
					tx("name"),
					txArr("colors"),
					txWord("description"),
					txArrWord("tags"),
					objArr("accessories", tx("type")),
					objArr("tires", in("width")),
					objArr("doors", in("count")),
				),
			),
		},
	}
}

// l2ObjectSchema mirrors l2Schema except the root property is a single
// OBJECT (`country`) instead of an OBJECT_ARRAY (`countries`). Used by
// the Python-port L2_object variant tests. AssignPositions treats a
// single OBJECT as a 1-element array internally — same dispatch path
// as L2 with one-country-per-doc fixtures — so this schema mostly
// confirms the OBJECT-vs-OBJECT_ARRAY encoding branch in walkNestedArray.
func l2ObjectSchema() *models.Property {
	tx := func(name string) *models.NestedProperty {
		return &models.NestedProperty{
			Name: name, DataType: []string{string(schema.DataTypeText)},
			Tokenization: models.PropertyTokenizationField,
		}
	}
	in := func(name string) *models.NestedProperty {
		return &models.NestedProperty{Name: name, DataType: []string{string(schema.DataTypeInt)}}
	}
	txArr := func(name string) *models.NestedProperty {
		return &models.NestedProperty{
			Name: name, DataType: []string{string(schema.DataTypeTextArray)},
			Tokenization: models.PropertyTokenizationField,
		}
	}
	txWord := func(name string) *models.NestedProperty {
		return &models.NestedProperty{
			Name: name, DataType: []string{string(schema.DataTypeText)},
			Tokenization: models.PropertyTokenizationWord,
		}
	}
	txArrWord := func(name string) *models.NestedProperty {
		return &models.NestedProperty{
			Name: name, DataType: []string{string(schema.DataTypeTextArray)},
			Tokenization: models.PropertyTokenizationWord,
		}
	}
	objArr := func(name string, nested ...*models.NestedProperty) *models.NestedProperty {
		return &models.NestedProperty{
			Name:             name,
			DataType:         []string{string(schema.DataTypeObjectArray)},
			NestedProperties: nested,
		}
	}
	return &models.Property{
		Name:     "country",
		DataType: []string{string(schema.DataTypeObject)},
		NestedProperties: []*models.NestedProperty{
			objArr("garages",
				tx("city"),
				objArr("cars",
					in("year"),
					tx("make"),
					tx("model"),
					tx("name"),
					txArr("colors"),
					txWord("description"),
					txArrWord("tags"),
					objArr("accessories", tx("type")),
					objArr("tires", in("width")),
					objArr("doors", in("count")),
				),
			),
		},
	}
}

// l2Docs returns the L2 fixtures. 100–600 are the core single-leaf fixtures.
// 700 carries an empty-cars garage alongside a non-empty one — needed for
// owner-level `cars IS NULL true|false` on object[]. Docs 800/900 will be
// added when sibling-merge / ghost-recovery tests land.
func l2Docs() map[uint64]any {
	return map[uint64]any{
		100: []any{ // single country, single garage, three cars
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2020, "make": "toyota", "colors": []any{"blue"}, "accessories": []any{map[string]any{"type": "spoiler"}}, "tires": []any{map[string]any{"width": 205}}},
					map[string]any{"year": 2018, "make": "honda", "colors": []any{"blue", "red"}, "accessories": []any{map[string]any{"type": "radio"}}, "tires": []any{map[string]any{"width": 195}}},
					map[string]any{"year": 2017, "name": "honda"},
				}},
			}},
		},
		200: []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2015, "make": "honda", "colors": []any{"red"}, "accessories": []any{map[string]any{"type": "spoiler"}}, "tires": []any{map[string]any{"width": 205}}},
					map[string]any{"year": 2020, "make": "honda", "colors": []any{"blue"}, "accessories": []any{map[string]any{"type": "spoiler"}}, "tires": []any{map[string]any{"width": 205}}},
					map[string]any{"name": "ford"},
				}},
			}},
		},
		300: []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2011, "make": "ford", "colors": []any{"green"}},
					map[string]any{"make": "honda", "colors": []any{"red"}}, // cars[1]: no year
				}},
			}},
		},
		400: []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2012, "make": "ford", "colors": []any{"yellow"}},
				}},
			}},
		},
		500: []any{ // single country, two garages
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2018, "make": "mazda"},
					map[string]any{"year": 2019, "make": "ford"},
				}},
				map[string]any{"cars": []any{
					map[string]any{"year": 2022, "make": "bmw"},
					map[string]any{"year": 2016, "name": "honda"},
					map[string]any{"name": "honda"},
				}},
			}},
		},
		600: []any{ // two countries, one garage each
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2010, "make": "ford"},
					map[string]any{"year": 2018, "make": "mazda"},
					map[string]any{"name": "ford"},
				}},
			}},
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2011, "make": "bmw"},
					map[string]any{"year": 2020, "make": "toyota"},
					map[string]any{"name": "honda"},
				}},
			}},
		},
		700: []any{ // single country, two garages: one empty cars, one non-empty
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{}},
				map[string]any{"cars": []any{
					map[string]any{"year": 2014, "make": "ford"},
				}},
			}},
		},
		// 810: every car matches `year=2020`. Forces FAIL paths for NOT and
		// IS NULL true to actually fire (without this, every fixture had at
		// least one car with year != 2020 or missing year, so the unpinned
		// negation helpers would pass even if the positive subtraction were
		// a no-op).
		810: []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2020, "make": "honda"},
					map[string]any{"year": 2020, "make": "toyota"},
				}},
			}},
		},
		// 820: two countries each with cars[1].year=2020. Forces NOT
		// cars[1].year=2020 to FAIL at doc level (every root has the pinned
		// match) — exercises the multi-root all-match case that doc 600
		// only half-covered (in doc 600 only one country matches).
		820: []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2018, "make": "ford"},
					map[string]any{"year": 2020, "make": "honda"},
				}},
			}},
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2019, "make": "kia"},
					map[string]any{"year": 2020, "make": "mazda"},
				}},
			}},
		},
		// 830: car with multiple accessories — spoiler at index 1, not 0.
		// Verifies walker positions for non-first array elements at depth 4.
		// Car has no year/colors/make so its contribution to year/color tests
		// is limited to "missing scalar" witnesses (E, F, G, H, (5b)).
		830: []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"accessories": []any{
						map[string]any{"type": "radio"},
						map[string]any{"type": "spoiler"},
					}},
				}},
			}},
		},
		// 800: positive fixture for ghost recovery. One garage (warsaw)
		// contains a single car that satisfies spoiler+205+door=4. The
		// other garage (krakow) splits spoiler and 205 across two cars —
		// produces a ghost ancestor at the krakow garage when (spoiler AND
		// 205) is merged at cars. An ancestor constraint city=warsaw filters
		// the krakow ghost; a same-scope sibling like doors.count=4 doesn't.
		800: []any{
			map[string]any{"garages": []any{
				map[string]any{"city": "warsaw", "cars": []any{
					map[string]any{
						"accessories": []any{map[string]any{"type": "spoiler"}},
						"tires":       []any{map[string]any{"width": 205}},
						"doors":       []any{map[string]any{"count": 4}},
					},
				}},
				map[string]any{"city": "krakow", "cars": []any{
					map[string]any{
						"accessories": []any{map[string]any{"type": "spoiler"}},
						"doors":       []any{map[string]any{"count": 4}},
					},
					map[string]any{
						"tires": []any{map[string]any{"width": 205}},
						"doors": []any{map[string]any{"count": 4}},
					},
				}},
			}},
		},
		// 900: negative fixture for ghost recovery. Same krakow split as
		// 800, but the warsaw garage has no spoiler+205 witness —
		// only doors. With city=warsaw the result is empty (ghost in krakow
		// is filtered, no witness in warsaw). With doors.count=4 it stays
		// empty (sibling matches in krakow too, ghost survives but no real
		// car witness anywhere).
		900: []any{
			map[string]any{"garages": []any{
				map[string]any{"city": "warsaw", "cars": []any{
					map[string]any{
						"doors": []any{map[string]any{"count": 4}},
					},
				}},
				map[string]any{"city": "krakow", "cars": []any{
					map[string]any{
						"accessories": []any{map[string]any{"type": "spoiler"}},
						"doors":       []any{map[string]any{"count": 4}},
					},
					map[string]any{
						"tires": []any{map[string]any{"width": 205}},
						"doors": []any{map[string]any{"count": 4}},
					},
				}},
			}},
		},
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// buildL2 ingests every L2 fixture into a fresh index.
func buildL2(t *testing.T) *fastPathIndex {
	t.Helper()
	prop := l2Schema()
	idx := newFastPathIndex("countries")
	for docID, value := range l2Docs() {
		idx.addDoc(t, prop, docID, value)
	}
	return idx
}

// fastPathTestCase is the table row used by both single-leaf and merge tests.
// build constructs the result lazily so each subtest captures its own
// expression — easier to scan than a slice of *fastPathResult.
//
// wantCeiling names the SHALLOWEST scope at which the result's Raw is
// still ghost-free (Raw ∩ _anchor(S) yields exactly wantDocs after
// MaskRootLeaf, for every S between Scope and Ceiling inclusive).
// Above Ceiling, Raw may carry owner-level ghosts — no assertion is
// made there.
type fastPathTestCase struct {
	name        string
	build       func(*fastPathIndex) *fastPathResult
	wantScope   string
	wantCeiling string
	wantDocs    []uint64 // after the real MaskRootLeaf projection
}

// runFastPathCases drives the standard assertion suite for any
// fastPathTestCase table: scope/Ceiling equality, Witnesses non-empty iff
// wantDocs non-empty, trustworthy-scope invariant, doc-id equality, and
// the no-ghost contract over [Scope, Ceiling].
func runFastPathCases(t *testing.T, idx *fastPathIndex, cases []fastPathTestCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.build(idx)
			assert.Equal(t, tc.wantScope, r.Scope)
			assert.Equal(t, tc.wantCeiling, r.Ceiling)
			// Witnesses mirrors wantDocs: non-empty iff wantDocs non-empty.
			// Raw is non-empty in the populated case; for empty-result we
			// skip the Raw check because some negation shapes can leave
			// ancestor chain bits in Raw even when Witnesses is empty.
			if len(tc.wantDocs) == 0 {
				assert.True(t, r.Witnesses.IsEmpty(), "Witnesses must be empty when wantDocs is empty")
			} else {
				assert.False(t, r.Raw.IsEmpty(), "Raw must not be empty")
				assert.False(t, r.Witnesses.IsEmpty(), "Witnesses must not be empty")
			}
			// Trustworthy-scope invariant: Raw ∩ _anchor(Scope) must
			// equal Witnesses. Holds regardless of Ceiling — Scope is
			// always inside the clean range and the equality is a structural
			// property of how Witnesses is built from Raw. anchorAt
			// routes pathRoot lookups to docUniverse (pinned-root results).
			richAtTruth := r.Raw.Clone().And(idx.anchorAt(r.Scope))
			assert.Equal(t, r.Witnesses.ToArray(), richAtTruth.ToArray(),
				"Raw ∩ _anchor(Scope) must equal Witnesses")
			assert.ElementsMatch(t, tc.wantDocs, idx.docIDs(r))

			// Ghost-free contract for the entire clean range: walk from
			// Scope up to and including Ceiling. At each scope,
			// projecting Raw via anchor and masking to docIDs must yield
			// exactly wantDocs. The loop stops before going above
			// Ceiling — that's where owner-level ghosts are allowed to
			// live, and the result explicitly disclaims cleanness there.
			//
			// Loop exits when scope would become parentPath(Ceiling) —
			// i.e., one step above Ceiling. Naturally handles
			// Ceiling == pathRoot (parentPath(pathRoot) == pathRoot,
			// stop == pathRoot, body never executes at the doc level —
			// the trustworthy-scope check above is the doc-level witness).
			stop := parentPath(r.Ceiling)
			for scope := r.Scope; scope != stop; scope = parentPath(scope) {
				richAtScope := r.Raw.Clone().And(idx.anchorAt(scope))
				docs, _ := idx.ops.MaskRootLeaf(richAtScope)
				assert.ElementsMatch(t, tc.wantDocs, docs.ToArray(),
					"clean range [Scope, Ceiling]: Raw ∩ _anchor(%q) must yield wantDocs", scope)
			}
		})
	}
}

// TestFastPathL2_SingleLeaf exercises the single-leaf shapes against the L2
// schema. Each subtest verifies the full result quadruple (Scope, Raw,
// Witnesses, Ceiling) plus the masked doc IDs from
// BitmapOps.MaskRootLeaf.
func TestFastPathL2_SingleLeaf(t *testing.T) {
	idx := buildL2(t)

	cases := []fastPathTestCase{
		{
			name: "cars.year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "countries.garages.cars.year", 2020)
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: pathRoot,
			wantDocs:    []uint64{100, 200, 600, 810, 820},
		},
		{
			name: "cars[1].year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedPositive(idx,
					"countries.garages.cars.year",
					2020,
					[]pinSpec{{"countries.garages.cars", 1}})
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			wantDocs:    []uint64{200, 600, 810, 820},
		},
		{
			name: "cars.year IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullFalse(idx, "countries.garages.cars.year")
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: pathRoot,
			wantDocs:    []uint64{100, 200, 300, 400, 500, 600, 700, 810, 820},
		},
		{
			name: "cars[1].year IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullFalse(idx,
					"countries.garages.cars.year",
					[]pinSpec{{"countries.garages.cars", 1}})
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			wantDocs:    []uint64{100, 200, 500, 600, 810, 820},
		},
		{
			name: "cars.year IS NULL true",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullTrue(idx, "countries.garages.cars.year")
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			wantDocs:    []uint64{200, 300, 500, 600, 800, 830, 900},
		},
		{
			name: "cars[1].year IS NULL true",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullTrue(idx,
					"countries.garages.cars.year",
					[]pinSpec{{"countries.garages.cars", 1}})
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			wantDocs:    []uint64{300, 400, 700, 800, 830, 900},
		},
		{
			name: "NOT cars.year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafNot(idx, "countries.garages.cars.year", 2020)
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// 810 FAILS: every car has year=2020. 820 PASSES: each country
			// has cars[0] with year != 2020 (a per-element witness). 830
			// PASSES: car has no year, which counts as "not 2020". 800/900
			// PASS: cars in those docs have no year.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 800, 820, 830, 900},
		},
		{
			name: "NOT cars[1].year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedNot(idx,
					"countries.garages.cars.year",
					2020,
					[]pinSpec{{"countries.garages.cars", 1}})
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			// 810 FAILS: cars[1].year=2020 in the only garage. 820 FAILS: // every country's cars[1] has year=2020 — no garage witness anywhere.
			// 830 PASSES: no cars[1] → missing pinned slot satisfies NOT.
			// 800/900 PASS: cars[1] missing or has no year.
			wantDocs: []uint64{100, 300, 400, 500, 600, 700, 800, 830, 900},
		},
		{
			name: "cars IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullFalse(idx, "countries.garages.cars")
			},
			wantScope:   "countries.garages",
			wantCeiling: pathRoot,
			wantDocs:    []uint64{100, 200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		{
			name: "cars IS NULL true",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullTrue(idx, "countries.garages.cars")
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			wantDocs:    []uint64{700},
		},
		// Scalar array (text[]) — verifies chain propagation through
		// walkScalarArray and that leafPositive / leafNot work over
		// per-element value/anchor entries without modification.
		{
			name: "cars.colors=red",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "countries.garages.cars.colors", "red")
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: pathRoot,
			wantDocs:    []uint64{100, 200, 300},
		},
		{
			name: "NOT cars.colors=red",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafNot(idx, "countries.garages.cars.colors", "red")
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// Every doc has at least one car lacking "red" (most fixtures
			// have no colors property at all — cars stay in _anchor(cars)
			// but contribute no posExact). All docs pass.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		// Depth-4 nested scalar (cars → accessories → type) — verifies chain
		// propagation through an extra level of walkNestedArray.
		{
			name: "cars.accessories.type=spoiler",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler")
			},
			wantScope:   "countries.garages.cars.accessories",
			wantCeiling: pathRoot,
			// 830 PASSES via accessories[1] — verifies walker positions for
			// non-first array elements at depth 4. 800 PASSES via spoiler
			// accessories in both g[0].cars[0] and g[1].cars[0]; 900 PASSES
			// via spoiler in g[1].cars[0].
			wantDocs: []uint64{100, 200, 800, 830, 900},
		},
		{
			name: "NOT cars.accessories.type=spoiler",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafNot(idx, "countries.garages.cars.accessories.type", "spoiler")
			},
			wantScope:   "countries.garages.cars.accessories",
			wantCeiling: "countries.garages.cars.accessories",
			// doc 100: cars[1]'s radio is non-spoiler witness.
			// doc 200: all spoilers → no witness → FAIL.
			// 300-820: no accessories → empty scope, no witness possible → FAIL.
			// 830: accessories[0] is radio → witness.
			wantDocs: []uint64{100, 830},
		},
		// Empty-result edge case — value present in the schema but no doc
		// has it. Exercises the no-match path through every helper
		// invariant (Raw/Witnesses empty, MaskRootLeaf on empty input).
		{
			name: "cars.year=9999 (empty result)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "countries.garages.cars.year", 9999)
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: pathRoot,
			wantDocs:    nil,
		},
		// Multi-pin path — two pins (garages[1] + cars[2]). Verifies the
		// narrow+lift chain across multiple array levels. Single-pin
		// equivalent `cars[2].name=honda` would also match docs 100 and 600;
		// the garages[1] pin filters them out, leaving only doc 500 (the
		// only fixture with garages[1].cars[2].name=honda).
		{
			name: "garages[1].cars[2].name=honda",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedPositive(idx,
					"countries.garages.cars.name",
					"honda",
					[]pinSpec{
						{"countries.garages", 1},
						{"countries.garages.cars", 2},
					})
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			wantDocs:    []uint64{500},
		},
		{
			name: "garages[1].cars[2].name IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullFalse(idx,
					"countries.garages.cars.name",
					[]pinSpec{
						{"countries.garages", 1},
						{"countries.garages.cars", 2},
					})
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			// Only doc 500 has the full pinned path with a name field.
			wantDocs: []uint64{500},
		},
		{
			name: "garages[1].cars[2].name IS NULL true",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullTrue(idx,
					"countries.garages.cars.name",
					[]pinSpec{
						{"countries.garages", 1},
						{"countries.garages.cars", 2},
					})
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			// All countries pass except doc 500's (which has the full
			// pinned path with a name). Missing-pin cases (no garages[1] or
			// no cars[2]) all count as witnesses — a missing pinned slot
			// satisfies IS NULL true.
			// 800/900 PASS: g[1] exists but cars[2] missing.
			wantDocs: []uint64{100, 200, 300, 400, 600, 700, 800, 810, 820, 830, 900},
		},
		{
			name: "NOT garages[1].cars[2].name=honda",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedNot(idx,
					"countries.garages.cars.name",
					"honda",
					[]pinSpec{
						{"countries.garages", 1},
						{"countries.garages.cars", 2},
					})
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			// Same set as the IS NULL true case — the only doc where the
			// full pinned path has name=honda is doc 500. Every other doc
			// passes either via missing pin or via name != honda.
			wantDocs: []uint64{100, 200, 300, 400, 600, 700, 800, 810, 820, 830, 900},
		},
		// Intermediate-pin: pin at garages[1] with cars unpinned. Discriminates
		// from single-pin `cars.make=bmw` (which would also include doc 600).
		// Only doc 500 has garages[1] containing a car with make=bmw.
		// NOT and IS NULL true variants use a per-element formula because
		// the pin chain has a gap (cars is unpinned between Scope=countries
		// and ElementScope=cars). leafPinnedNot and leafPinnedIsNullTrue
		// dispatch internally on hasPinGap and call perElementNotFromSubtractand.
		{
			name: "garages[1].cars.make=bmw",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedPositive(idx,
					"countries.garages.cars.make",
					"bmw",
					[]pinSpec{{"countries.garages", 1}})
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			wantDocs:    []uint64{500},
		},
		{
			name: "garages[1].cars.make IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullFalse(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}})
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			// doc 500: g[1].cars[0].make=bmw. doc 700: g[1].cars[0].make=ford.
			// All other docs lack garages[1] or have no make on any car in g[1].
			wantDocs: []uint64{500, 700},
		},
		// Per-element NOT on intermediate-pin path. Every doc passes
		// either via a per-element witness (some car under g[1] with
		// make!=bmw, or with no make field) or via missing-pin success
		// (the country has no g[1] slot at all).
		{
			name: "NOT garages[1].cars.make=bmw",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedNot(idx,
					"countries.garages.cars.make",
					"bmw",
					[]pinSpec{{"countries.garages", 1}})
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			// Missing-pin: docs 100-400, 600 (both countries lack g[1]),
			// 810, 820 (both countries lack g[1]), 830 — all pass via
			// scopeMissingPin.
			// Per-element witnesses: // - doc 500 g[1]: cars[0]=bmw is positive but cars[1]/[2]
			//   have name=honda with no make field — both are non-bmw
			//   witnesses.
			// - doc 700 g[1].cars[0]=ford — non-bmw witness.
			// - docs 800/900 g[1] cars have no make field at all — every
			//   car is a non-bmw witness.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		// Per-element IS NULL true on intermediate-pin path. Passes any
		// doc with at least one car under g[1] missing the make field,
		// OR with no g[1] at all. Owner-level would have excluded doc
		// 500 (g[1] has cars[0] with a make field); per-element correctly
		// includes 500 via cars[1] and cars[2].
		{
			name: "garages[1].cars.make IS NULL true",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullTrue(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}})
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			// Missing-pin: docs 100-400, 600, 810, 820, 830 (no g[1]).
			// Per-element witnesses: // - doc 500: g[1].cars[1] and cars[2] have name=honda but no
			//   make field — both are IS NULL true witnesses.
			// - docs 800/900: g[1] cars have no make at all — every car
			//   is a witness.
			// doc 700 FAILS: g[1].cars[0]=ford has a make field, and it's
			// the only car under g[1]. No per-element witness; pin is
			// present so no missing-pin contribution either.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 800, 810, 820, 830, 900},
		},
	}

	runFastPathCases(t, idx, cases)
}

// TestFastPathL2_Merge exercises compound (AND/OR) shapes against the L2
// schema, building on the single-leaf helpers. Each case constructs a
// compound via merge builders (andLeaves, …) operating on leaf results.
func TestFastPathL2_Merge(t *testing.T) {
	idx := buildL2(t)

	cases := []fastPathTestCase{
		// Positive AND at the same scope. Same-element semantics fall out
		// from intersecting chain-bearing Riches: only cars that satisfy
		// BOTH predicates simultaneously survive at the car self level.
		// Ceiling=Scope: Raw at parent (garages) keeps chain bits for
		// docs where different cars in the same garage satisfy each leaf
		// (e.g. doc 100 cars[0]=2020/toyota + cars[1]=2018/honda) — that
		// would falsely project to garage level.
		{
			name: "cars.make=honda AND cars.year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.year", 2020))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// doc 200: cars[1] has both year=2020 and make=honda.
			// doc 810: cars[0] has both year=2020 and make=honda.
			// doc 820 country 0: cars[1] has both. (Country 1 has neither in
			// the same car — but doc passes existentially via country 0.)
			// All other docs have year=2020 and make=honda in different cars
			// or not at all.
			wantDocs: []uint64{200, 810, 820},
		},
		// Positive OR at the same scope. Each leaf contributes its own
		// matching cars; the merged result is "exists a car matching either
		// predicate". Ceiling=pathRoot is preserved because Raw chain bits at
		// parent legitimately indicate "garages with at least one matching
		// car for either leaf" — the existential OR semantic at parent.
		{
			name: "cars.make=honda OR cars.year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.year", 2020))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: pathRoot,
			// 100: cars[0].year=2020. 200: cars[0].make=honda. 300: // cars[1].make=honda. 600 country 1: cars[1].year=2020.
			// 810: cars[0] year=2020 (and make=honda). 820 country 0: // cars[1] year=2020 (and make=honda). 400/500/700/830 lack both.
			wantDocs: []uint64{100, 200, 300, 600, 810, 820},
		},
		// Same property, different values — AND requires the same car's
		// scalar-array (colors) to contain BOTH values. Same shape as the
		// scalar-AND case above but on text[]: each colors element emits at
		// its own leaf position while sharing the car's chain. Intersection
		// at car self level survives only when a single car has both colors.
		{
			name: "cars.colors=blue AND cars.colors=red",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "countries.garages.cars.colors", "blue"),
					leafPositive(idx, "countries.garages.cars.colors", "red"))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// doc 100: cars[1].colors=["blue","red"]. Other docs either lack
			// colors entirely (500/600/700/810/820/830), have only one of the
			// values (200 has blue and red split across cars[0]/cars[1]; 300
			// has only red; 400 has only "yellow"), or have neither.
			wantDocs: []uint64{100},
		},
		// Sibling branches under a shared owner. Each leaf's natural
		// Scope is its own array element (accessories vs tires); the
		// merge must land at the common ancestor (cars). andLeaves detects
		// the sibling relationship and lifts both operands to the LCA before
		// merging.
		{
			name: "cars.accessories.type=spoiler AND cars.tires.width=205",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// doc 100: cars[0] has both spoiler-accessory and tires.width=205.
			// doc 200: cars[0] and cars[1] both have both. doc 800 g[0].cars[0]
			// has both. Other docs lack either accessories or tires (or both);
			// doc 830 has a spoiler accessory on cars[0] but no tires.
			// doc 900 has spoiler in g[1].cars[0] and tires in g[1].cars[1] —
			// no single car has both, FAIL.
			wantDocs: []uint64{100, 200, 800},
		},
		// Pinned+pinned merge at owner. Both leaves naturally have
		// Scope=garages after pin consumption, so andLeaves works
		// without lift. Same-garage semantic: a single garage must satisfy
		// both NOT cars[1].year=2020 (cars[1] missing or year ≠ 2020) AND
		// cars[2].name=honda. doc 600 deliberately FAILs because its two
		// countries each satisfy one half but no garage satisfies both —
		// this test pins the no-cross-country-mixing property.
		{
			name: "NOT cars[1].year=2020 AND cars[2].name=honda",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPinnedNot(idx,
						"countries.garages.cars.year", 2020,
						[]pinSpec{{"countries.garages.cars", 1}}),
					leafPinnedPositive(idx,
						"countries.garages.cars.name", "honda",
						[]pinSpec{{"countries.garages.cars", 2}}))
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			// doc 100: g[0] satisfies both. doc 500: g[1] satisfies both.
			// doc 600: country 0 g[0] satisfies NOT but cars[2].name=ford;
			// country 1 g[0] cars[1].year=2020 fails NOT. No same-garage
			// witness anywhere.
			wantDocs: []uint64{100, 500},
		},
		// Mixed Ceiling AND: pinned negation (Scope=garages, Ceiling=Scope)
		// ANDed with unpinned positive (Scope=cars, Ceiling=pathRoot). Ancestor.Ceiling=Scope
		// + child.Ceiling=pathRoot → direct merge at ancestor.Scope=garages (child's
		// chain bits at garages are already clean since child.Ceiling=pathRoot).
		// Result Ceiling=Scope (ancestor's ghosts above garages remain).
		{
			name: "NOT cars[1].year=2020 AND cars.make=honda",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPinnedNot(idx,
						"countries.garages.cars.year", 2020,
						[]pinSpec{{"countries.garages.cars", 1}}),
					leafPositive(idx, "countries.garages.cars.make", "honda"))
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			// doc 100: g[0] cars[1]=2018 satisfies NOT, cars[1].make=honda.
			// doc 300: g[0] cars[1] missing year satisfies NOT (missing
			// pinned slot is a witness), cars[1].make=honda.
			// 200/810/820: cars[1].year=2020 fails NOT. 400/500/600/700/830: // no make=honda or no satisfying garage.
			wantDocs: []uint64{100, 300},
		},
		// Ghost mitigation via ancestor constraint. The inner sibling-AND
		// at cars has Ceiling=Scope because chain bits at garage level can
		// survive even when no single car satisfies both predicates (a
		// "ghost ancestor"). AND-ing with garages.city=warsaw — a positive
		// ancestor at garages, Ceiling=pathRoot — masks ghosts that don't overlap
		// with warsaw garages, but cannot mask ghosts that coincide with
		// warsaw's authentic positions. Result Ceiling=child.Ceiling=Scope: Witnesses at
		// cars is correct, but parent projection stays unsafe in general.
		//
		// For these specific fixtures the merge happens to produce a clean
		// result at parent — doc 800 g[1] (krakow) ghost is filtered out
		// because krakow ≠ warsaw; doc 900 has no inner witness anywhere.
		// But that's data-dependent; the Ceiling flag stays false because a doc
		// with a warsaw garage carrying its own split-spoiler/tires ghost
		// would survive the filter and leak into a parent projection.
		{
			name: "(cars.accessories.type=spoiler AND cars.tires.width=205) AND garages.city=warsaw",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					andLeaves(idx,
						leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
						leafPositive(idx, "countries.garages.cars.tires.width", 205)),
					leafPositive(idx, "countries.garages.city", "warsaw"))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			wantDocs:    []uint64{800},
		},
		// Ancestor+child AND with both Ceiling=pathRoot. Ancestor garages.city=warsaw
		// (Scope=garages) AND child cars.doors.count=4 (Scope=doors). Merge at
		// child.Scope=doors with Ceiling=child.Ceiling=pathRoot. Ancestor.Raw broadcasts to
		// descendants via the chain encoding, so doors-scope intersection
		// keeps only doors whose garage chain includes a warsaw garage.
		{
			name: "garages.city=warsaw AND cars.doors.count=4",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "countries.garages.city", "warsaw"),
					leafPositive(idx, "countries.garages.cars.doors.count", 4))
			},
			wantScope: "countries.garages.cars.doors",
			// Ceiling = ancestor.Scope (garages). The dispatch matrix's
			// broadcasting-and-clean-down-to-A.Scope branch fires here: // per-element correlation holds at scopes [cars.doors, cars,
			// garages]; strictly above garages (countries) the chain bits
			// are shared and ghosts can survive.
			wantCeiling: "countries.garages",
			// 800 g[0] (warsaw): cars[0].doors=4 → MATCH. 800 g[1] (krakow): // doors=4 but chain is krakow → no warsaw overlap.
			// 900 g[0] (warsaw): cars[0].doors=4 → MATCH. 900 g[1] (krakow): // doors=4 but krakow.
			// No other fixture sets city or doors.
			wantDocs: []uint64{800, 900},
		},
		// Ancestor+child AND with both Ceiling=Scope. Ancestor pinned NOT
		// (Scope=garages) AND child same-scope AND at cars (Scope=cars). Both
		// inputs carry ghosts, so child gets lifted to ancestor.Scope=garages
		// first (LiftToAncestor rebuilds child.Raw at garages from
		// authentic Witnesses, wiping ghost bits), then same-scope AND
		// at garages with Ceiling=Scope.
		{
			name: "NOT cars[1].year=2020 AND (cars.colors=red AND cars.colors=blue)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPinnedNot(idx,
						"countries.garages.cars.year", 2020,
						[]pinSpec{{"countries.garages.cars", 1}}),
					andLeaves(idx,
						leafPositive(idx, "countries.garages.cars.colors", "red"),
						leafPositive(idx, "countries.garages.cars.colors", "blue")))
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			// Inner AND matches only doc 100 cars[1] — the only car with
			// both red and blue colors in any fixture. Outer NOT witness
			// covers doc 100 g[0] (cars[1].year=2018 ≠ 2020). Lifted child
			// at garages = {doc 100 g[0]}; ancestor Witnesses at garages includes
			// that garage. Intersection → doc 100.
			wantDocs: []uint64{100},
		},
		// Contrast to the ancestor-constrained ghost-mitigation case above: // adding a same-scope sibling at cars (doors.count=4) does NOT clean
		// the ghost at the parent garage. The inner sibling-AND
		// (spoiler AND 205) carries chain bits at BOTH the warsaw garage
		// (real witness via cars[0]) and the krakow garage (ghost from
		// cars[0]=spoiler / cars[1]=205 split). doors.count=4 fires in cars
		// under BOTH garages of doc 800, so its chain bits overlap with the
		// ghost at krakow — the same-scope intersection preserves both
		// garages in Raw. Witnesses at cars is still clean (only the
		// warsaw car satisfies all three at car-self level), so wantDocs is
		// {800}. The point of the test is the structural property that
		// Ceiling stays false after another same-scope AND: same-scope
		// siblings cannot recover from a ghost-bearing input, while an
		// ancestor constraint at a different scope sometimes can (data
		// permitting).
		{
			name: "(cars.accessories.type=spoiler AND cars.tires.width=205) AND cars.doors.count=4",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					andLeaves(idx,
						leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
						leafPositive(idx, "countries.garages.cars.tires.width", 205)),
					leafPositive(idx, "countries.garages.cars.doors.count", 4))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// doc 800: warsaw cars[0] has spoiler+205+doors=4 → MATCH.
			// krakow cars split spoiler/205 across cars, both have doors=4
			// → no single car satisfies all three at car-self → ghost only.
			// doc 900: warsaw cars[0] has only doors → no inner AND witness.
			// krakow cars split spoiler/205 like in 800 → ghost only.
			// Net witness across all garages: only doc 800 warsaw car.
			wantDocs: []uint64{800},
		},
		// Sibling OR with both operands Ceiling=pathRoot. accessories and tires are
		// sibling sub-objects under cars; their LCA is cars. Both lifts
		// take the cheap anchor-intersect path because each operand's Raw
		// is already authentic at the LCA (chain bits propagate up via the
		// chain+self+descendantSelves encoding). Result Ceiling=pathRoot: union
		// preserves cleanness when both inputs are clean.
		{
			name: "cars.accessories.type=spoiler OR cars.tires.width=205",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205))
			},
			wantScope: "countries.garages.cars",
			// Ceiling = "" (fully authentic — see fastPathResult doc)
			// (Ceiling=countries). Lifting from accessories/tires to LCA=cars is
			// cheap (target=cars is at-or-below Ceiling=countries), so Raw is
			// reused unchanged and the cleanness claim above LCA is
			// preserved through the lift. OR of two clean-at-countries
			// operands stays clean at countries.
			wantCeiling: pathRoot,
			// spoiler-accessory contributors: doc 100 cars[0], doc 200
			// cars[0], doc 800 g[0] cars[0] + g[1] cars[0], doc 830 cars[0]
			// (accessories[1]), doc 900 g[1] cars[0] — docs {100, 200, 800,
			// 830, 900}.
			// 205-tire contributors: doc 100 cars[0], doc 200 cars[0], doc
			// 800 g[0] cars[0] + g[1] cars[1], doc 900 g[1] cars[1] — docs
			// {100, 200, 800, 900}.
			// Union: {100, 200, 800, 830, 900}. doc 830 only via
			// accessories (no tires); all others appear in both.
			wantDocs: []uint64{100, 200, 800, 830, 900},
		},
		// Ancestor+child OR with both operands Ceiling=pathRoot. Pins the
		// "container exists" semantic: docs 800 and 900 have warsaw
		// garages but no ford cars, and must still pass via the warsaw
		// branch alone. A bug that merged at C.Scope=cars (instead of LCA
		// = A.Scope = garages) would drop them, because A's value bits at
		// cars-self exist only for cars under warsaw garages — empty for
		// warsaw garages with cars that don't satisfy B. The correct
		// merge at A.Scope keeps "garages where A or B fires" without
		// requiring B to fire under A.
		//
		// A.Scope=garages is already the LCA, so A's lift is a no-op.
		// B (Scope=cars, Ceiling=pathRoot) gets the cheap anchor-intersect lift.
		// Result Ceiling=pathRoot: both inputs clean above their Scope, union clean
		// above garages.
		{
			name: "garages.city=warsaw OR cars.make=ford",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "countries.garages.city", "warsaw"),
					leafPositive(idx, "countries.garages.cars.make", "ford"))
			},
			wantScope: "countries.garages",
			// Ceiling = "" (fully authentic — see fastPathResult doc)
			// its lift is the early-return no-op (Raw and Ceiling untouched).
			// ford takes the cheap lift to garages: target=garages is
			// at-or-below Ceiling=countries, Raw is reused and Ceiling preserved.
			// The OR ends up clean at countries.
			wantCeiling: pathRoot,
			// warsaw garages: doc 800 g[0], doc 900 g[0] — docs {800, 900}.
			// ford-cars (make=ford): doc 300, 400, 500 (g[0] cars[1]),
			// 600 (country 0 g[0] cars[0]), 700 (g[1] cars[0]), 820
			// (country 0 g[0] cars[0]) — docs {300, 400, 500, 600, 700,
			// 820}. doc 200 has name=ford (not make=ford) → not a match.
			// Union: docs where some garage has city=warsaw OR some car
			// in any garage has make=ford. Docs 800 and 900 pass via
			// warsaw garage despite having no ford cars at all.
			wantDocs: []uint64{300, 400, 500, 600, 700, 800, 820, 900},
		},
		// Ancestor+child OR with C.Ceiling=Scope. A=warsaw (Scope=garages,
		// Ceiling=pathRoot) is already at the LCA. C=inner AND of colors (Scope=cars,
		// Ceiling=Scope) carries chain ghosts at garages — different cars in
		// the same garage satisfying each colors leaf could leave a garage
		// chain bit even with no single car matching both. The lift uses
		// LiftToAncestor on C.Witnesses to rebuild lifted_C.Raw at garages from
		// authentic Witnesses, wiping those ghost bits before the
		// union. Result Ceiling=Scope because A.Ceiling && C.Ceiling = false.
		{
			name: "garages.city=warsaw OR (cars.colors=red AND cars.colors=blue)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "countries.garages.city", "warsaw"),
					andLeaves(idx,
						leafPositive(idx, "countries.garages.cars.colors", "red"),
						leafPositive(idx, "countries.garages.cars.colors", "blue")))
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			// Inner AND matches only doc 100 cars[1] — the only car with
			// both red and blue colors in any fixture. Lifted to garages
			// = doc 100 g[0]. warsaw garages = doc 800 g[0], doc 900 g[0].
			// Union → {100, 800, 900}.
			wantDocs: []uint64{100, 800, 900},
		},
		// Sibling OR with asymmetric Ceiling. A=NOT spoiler (Scope=accessories,
		// Ceiling=Scope) requires the real LiftToAncestor + Raw reconstruction
		// path because A.Raw at cars carries chain bits for every car
		// that owns any accessory — including cars whose accessories are
		// all spoiler (no non-spoiler witness). LiftToAncestor on A.Witnesses
		// gives only cars with at least one non-spoiler accessory.
		// B=205-tires (Scope=tires, Ceiling=pathRoot) takes the cheap anchor-intersect
		// lift; B.Raw at cars is already authentic. Result Ceiling=Scope
		// because A.Ceiling && B.Ceiling = false.
		//
		// Note: wantDocs at the doc level coincides with the simpler
		// spoiler-OR-205 case above because MaskRootLeaf collapses to
		// docID — the lift's per-car correction (excluding all-spoiler
		// cars from A's contribution at cars-self) doesn't surface as a
		// different doc set. The harness still verifies the trustworthy-
		// scope invariant (Raw ∩ _anchor(cars) == Witnesses), which would fail
		// if A were merged without lifting.
		{
			name: "NOT cars.accessories.type=spoiler OR cars.tires.width=205",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafNot(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// NOT spoiler witness cars (per-car non-spoiler accessory): // doc 100 cars[1] (radio), doc 830 cars[0] (radio at idx 0).
			// 205-tire cars: doc 100 cars[0], doc 200 cars[0], doc 800
			// warsaw cars[0], doc 800 krakow cars[1], doc 900 krakow
			// cars[1].
			// Union at cars-self covers docs {100, 200, 800, 830, 900}.
			wantDocs: []uint64{100, 200, 800, 830, 900},
		},
		// Same-Scope OR with mixed Ceiling: inner AND (Scope=cars, Ceiling=Scope) ORed
		// with a leaf at cars (Ceiling=pathRoot). Both operands at the same Scope, so
		// no lifts; result Ceiling = false && true = false. Verifies the
		// Ceiling-propagation rule for same-Scope OR with mixed input — the
		// existing same-Scope OR test only covers (true, true).
		{
			name: "(cars.accessories.type=spoiler AND cars.tires.width=205) OR cars.make=ford",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					andLeaves(idx,
						leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
						leafPositive(idx, "countries.garages.cars.tires.width", 205)),
					leafPositive(idx, "countries.garages.cars.make", "ford"))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// Inner AND (spoiler+205 same car): docs {100, 200, 800}.
			// ford-cars: docs {300, 400, 500, 600, 700, 820}.
			// Union: {100, 200, 300, 400, 500, 600, 700, 800, 820}.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 800, 820},
		},
		// AND consuming an OR result. Inner OR at cars (Ceiling=pathRoot, since
		// both inputs are clean positives at the same Scope); outer leaf at
		// cars (Ceiling=pathRoot). Same-Scope AND, result Ceiling=Scope structurally.
		// Verifies that andLeaves correctly dispatches on an OR-produced
		// input — the OR's clean output gets ANDed without surprises.
		{
			name: "(cars.make=honda OR cars.make=ford) AND cars.year=2018",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					orLeaves(idx,
						leafPositive(idx, "countries.garages.cars.make", "honda"),
						leafPositive(idx, "countries.garages.cars.make", "ford")),
					leafPositive(idx, "countries.garages.cars.year", 2018))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// Per-car: (make=honda OR make=ford) AND year=2018 in the
			// same car.
			// doc 100: cars[1] make=honda year=2018 ✓ → PASS.
			// doc 820 country 0: cars[0] make=ford year=2018 ✓ → PASS.
			// All other docs: no single car matches both predicates
			// (e.g. doc 500 cars[1] is ford/2019, doc 600 country 0
			// cars[0] is ford/2010, doc 300 cars[0] is ford/2011 etc).
			wantDocs: []uint64{100, 820},
		},
		// Ancestor+child OR with both operands Ceiling=Scope. A=pinned NOT
		// (Scope=garages, Ceiling=Scope) is already at the LCA. C=inner same-Scope
		// AND at cars (Scope=cars, Ceiling=Scope) carries chain ghosts at
		// garages — different cars in the same garage matching each leaf
		// would leave a garage bit even when no single car matches both.
		// C goes through LiftToAncestor on its Witnesses, rebuilding
		// lifted_C.Raw at garages from authentic Witnesses (wiping the ghost
		// bits before the union). Result Ceiling = false && false = false.
		// Verifies the (A.Ceiling=Scope, C.Ceiling=Scope) cell of ancestor+child
		// OR, which the prior tests didn't cover.
		{
			name: "NOT cars[1].year=2020 OR (cars.make=honda AND cars.year=2020)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPinnedNot(idx,
						"countries.garages.cars.year", 2020,
						[]pinSpec{{"countries.garages.cars", 1}}),
					andLeaves(idx,
						leafPositive(idx, "countries.garages.cars.make", "honda"),
						leafPositive(idx, "countries.garages.cars.year", 2020)))
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			// A (NOT cars[1].year=2020 at garages): garages where cars[1]
			// missing or year≠2020 → docs {100, 300, 400, 500, 600, 700,
			// 800, 830, 900}.
			// Inner AND (same car has make=honda AND year=2020): doc 200
			// cars[1], doc 810 cars[0], doc 820 country 0 cars[1] → docs
			// {200, 810, 820}. Lifted to garages: doc 200 g[0], doc 810
			// g[0], doc 820 country 0 g[0].
			// Union covers every fixture: {100, 200, 300, 400, 500, 600,
			// 700, 800, 810, 820, 830, 900}. C meaningfully contributes
			// 200/810/820 — none are in A.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		// Owner-level IS NULL true combined with a positive child via OR.
		// A=cars IS NULL true at garages (Ceiling=Scope; the IS-NULL-true
		// shape carries chain ghosts at parent garages by construction).
		// C=make=honda at cars (Ceiling=pathRoot). Ancestor+child OR, C.Ceiling=pathRoot →
		// cheap anchor-intersect lift for C; A is already at LCA. Result
		// Ceiling = false && true = false. Verifies that owner-level IS NULL
		// composes correctly as an operand — no prior merge test uses it.
		{
			name: "cars IS NULL true OR cars.make=honda",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafIsNullTrue(idx, "countries.garages.cars"),
					leafPositive(idx, "countries.garages.cars.make", "honda"))
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			// cars IS NULL true at garages: only doc 700 g[0] (the empty
			// cars garage). All other garages have at least one car so
			// IS NULL true fails.
			// make=honda at cars: docs with a car whose make field is
			// "honda" — {100, 200, 300, 810, 820}. Docs 500/600 only
			// have name=honda (not make), so they don't qualify.
			// Union → {100, 200, 300, 700, 810, 820}.
			wantDocs: []uint64{100, 200, 300, 700, 810, 820},
		},
		// NOT applied to a compound (AND) result. The algorithm exposes
		// negate as a generic universe-subtract at the compound's
		// Scope: NOT-Witnesses = _anchor(Scope) ANDNOT compound.Witnesses. That gives
		// per-element-existential semantics at the compound's Scope — "some
		// element at Scope does NOT satisfy the compound." A doc passes if
		// any Scope-element falls outside the compound's Witnesses.
		//
		// Note: NOT-of-compound is a deferred design area (per-element vs
		// owner-level semantics differ for non-trivial doc shapes). This
		// test pins the algorithmic behavior of negate(), not a chosen
		// user-facing semantic.
		{
			name: "NOT (NOT cars[1].year=2020 AND cars[2].name=honda)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, andLeaves(idx,
					leafPinnedNot(idx,
						"countries.garages.cars.year", 2020,
						[]pinSpec{{"countries.garages.cars", 1}}),
					leafPinnedPositive(idx,
						"countries.garages.cars.name", "honda",
						[]pinSpec{{"countries.garages.cars", 2}})))
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			// Inner AND Witnesses at garages = {doc 100 g[0], doc 500 g[1]}.
			// negate at garages = _anchor(garages) ANDNOT inner.Witnesses = every
			// garage except those two.
			// doc 100: only g[0], which IS in inner → NOT has zero garages
			// → doc 100 fails.
			// doc 500: g[0] not in inner, g[1] in inner → NOT has g[0] →
			// doc 500 passes.
			// Every other doc: no garages in inner → all garages survive
			// the subtract → docs pass.
			wantDocs: []uint64{200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		// andN orchestration: sorts the 3 operands by Scope depth (deepest
		// first, Ceiling=pathRoot breaking ties) before sequential folding via
		// andLeaves. cars.tires.width=205 (Scope=cars.tires, depth 4) lands
		// first; cars.make=honda and cars.year=2020 (Scope=cars, depth 3,
		// both Ceiling=pathRoot) fold after. The dispatch keeps the running
		// result at the deepest Scope (cars.tires) for both subsequent
		// merges because each shallower ancestor is Ceiling=pathRoot (Row 1 of
		// the AND dispatch: merge at child.Scope, Ceiling=child.Ceiling).
		//
		// A naive left-to-right nested call (((make AND year) AND
		// tires)) lands at cars / Ceiling=Scope because the first same-Scope
		// AND already collapses to Ceiling=Scope. andN gets the same wantDocs
		// but at a strictly more informative Scope.
		{
			name: "andN(cars.make=honda, cars.year=2020, cars.tires.width=205)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andN(idx,
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.year", 2020),
					leafPositive(idx, "countries.garages.cars.tires.width", 205))
			},
			wantScope: "countries.garages.cars.tires",
			// Ceiling tracking through the fold:
			// Step 1 (tires AND honda): ancestor=honda(cars), child=tires.
			//   Broadcasting → Ceiling = honda.Scope = cars.
			// Step 2 (... AND year): year.Scope=cars is ancestor, but
			//   the accumulator is at cars.tires; year is also ancestor.
			//   Ceiling stays cars.
			// Above cars the chain bits are shared and ghosts can survive,
			// which is exactly what Ceiling=cars disclaims.
			wantCeiling: "countries.garages.cars",
			// Only doc 200 cars[1] satisfies all three (make=honda,
			// year=2020, tires.width=205) in the same car. Other
			// candidates fail one of the conjuncts: // - doc 100 cars[0]: toyota/2020/205 → fails honda.
			// - doc 100 cars[1]: honda/2018/195 → fails year and tires.
			// - doc 200 cars[0]: honda/2015/205 → fails year.
			// - doc 810 cars[0]: honda/2020/no-tires → fails tires.
			// - doc 820 cars[1]: honda/2020/no-tires → fails tires.
			wantDocs: []uint64{200},
		},
		// andN equivalent of the ghost-mitigation case above. Inputs: // accessories (depth 4, Ceiling=pathRoot), tires (depth 4, Ceiling=pathRoot),
		// garages.city (depth 2, Ceiling=pathRoot). Sort places accessories and
		// tires first (siblings under cars), then warsaw. Fold: //   - accessories AND tires → sibling lift to LCA=cars, same-Scope,
		//     Ceiling=Scope, Scope=cars.
		//   - result AND warsaw → ancestor=warsaw(garages, Ceiling=pathRoot),
		//     child=result(cars, Ceiling=Scope). Row 1: merge at cars,
		//     Ceiling=child.Ceiling=Scope.
		// Same wantDocs / Scope / Ceiling as the pairwise version.
		{
			name: "andN(spoiler, tires.width=205, garages.city=warsaw)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andN(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205),
					leafPositive(idx, "countries.garages.city", "warsaw"))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// doc 800 warsaw cars[0] has spoiler+205 under a warsaw
			// garage. krakow split spoiler/205 ghost gets filtered by
			// the warsaw constraint at garages.
			wantDocs: []uint64{800},
		},
		// andN over three same-depth siblings (accessories, tires,
		// doors — all sub-objects of cars, depth 4, Ceiling=pathRoot). Sort
		// leaves the input order intact (all same depth+Ceiling). Fold: //   - accessories AND tires → sibling at LCA=cars, Ceiling=Scope.
		//   - result(cars, Ceiling=Scope) AND doors(cars.doors, Ceiling=pathRoot) →
		//     Row 2 (A.Ceiling=Scope, C.Ceiling=pathRoot): merge at A.Scope=cars,
		//     Ceiling=Scope.
		// Same result as the pairwise contrast test above. Useful as
		// a regression check that the N-ary dispatch handles a uniform
		// sibling triple without surprises.
		{
			name: "andN(spoiler, tires.width=205, doors.count=4)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andN(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205),
					leafPositive(idx, "countries.garages.cars.doors.count", 4))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// Only doc 800 warsaw cars[0] has all three in a single car.
			wantDocs: []uint64{800},
		},
		// andN over four leaves spanning two Scope levels (two siblings at
		// depth 4, two same-Scope leaves at depth 3). Inputs: //   - cars.accessories.type=spoiler (Scope=accessories, depth 4)
		//   - cars.tires.width=205          (Scope=tires,       depth 4)
		//   - cars.make=honda               (Scope=cars,        depth 3)
		//   - cars.year=2020                (Scope=cars,        depth 3)
		// All Ceiling=pathRoot. Sort yields depth-4 pair first, then the depth-3
		// pair. Fold: //   - accessories AND tires → sibling lift to cars, same-Scope at
		//     cars, Ceiling=Scope.
		//   - result(cars, Ceiling=Scope) AND make(cars, Ceiling=pathRoot) → same-Scope,
		//     Ceiling=Scope.
		//   - result(cars, Ceiling=Scope) AND year(cars, Ceiling=pathRoot) → same-Scope,
		//     Ceiling=Scope.
		// Per-car semantics: car with spoiler accessory AND 205 tire AND
		// make=honda AND year=2020 simultaneously.
		{
			name: "andN(spoiler, tires.width=205, make=honda, year=2020)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andN(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205),
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.year", 2020))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// Only doc 200 cars[1] has all four (spoiler, 205, honda,
			// 2020) in the same car. Other candidates fail at least one: // - doc 100 cars[0]: spoiler+205+toyota+2020 → fails make.
			// - doc 200 cars[0]: spoiler+205+honda+2015 → fails year.
			// - doc 800 warsaw cars[0]: spoiler+205+no make → fails make.
			wantDocs: []uint64{200},
		},
		// andN with three leaves spanning two Scope levels and mixed Ceiling.
		// Inputs: //   - NOT cars[1].year=2020 (Scope=garages, depth 2, Ceiling=Scope)
		//   - cars.make=honda          (Scope=cars,   depth 3, Ceiling=pathRoot)
		//   - cars.year=2018           (Scope=cars,   depth 3, Ceiling=pathRoot)
		// Sort places the two depth-3 Ceiling=pathRoot leaves first, then the
		// depth-2 Ceiling=Scope one. Fold: //   - make AND year → same-Scope at cars, Ceiling=Scope.
		//   - result(cars, Ceiling=Scope) AND NOT(garages, Ceiling=Scope) → Row 3
		//     (both Ceiling=Scope): lift result to garages, same-Scope at
		//     garages, Ceiling=Scope.
		// Per-garage semantics: garage where some car has make=honda
		// AND year=2018 simultaneously, AND cars[1] is not year=2020.
		{
			name: "andN(NOT cars[1].year=2020, cars.make=honda, cars.year=2018)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andN(idx,
					leafPinnedNot(idx,
						"countries.garages.cars.year", 2020,
						[]pinSpec{{"countries.garages.cars", 1}}),
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.year", 2018))
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			// doc 100 g[0]: cars[1] is honda/2018 — same car has both.
			// cars[1].year=2018 ≠ 2020 → NOT also satisfied → PASS.
			// All other docs fail at least one conjunct in same-car or
			// same-garage form.
			wantDocs: []uint64{100},
		},
		// orN over three same-Scope positive leaves (all at cars, all
		// Ceiling=pathRoot). Sort is a no-op (input order preserved). Fold: //   - make OR year → same-Scope at cars, no lifts. Ceiling=pathRoot.
		//   - result OR colors → same-Scope at cars, no lifts. Ceiling=pathRoot.
		// Verifies the orchestration on the simplest path (no scope
		// transitions, no lifts).
		{
			name: "orN(cars.make=honda, cars.year=2018, cars.colors=blue)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orN(idx,
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.year", 2018),
					leafPositive(idx, "countries.garages.cars.colors", "blue"))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: pathRoot,
			// make=honda: {100, 200, 300, 810, 820}.
			// year=2018: {100, 500, 600, 820} (cars with year=2018).
			// colors=blue: {100, 200} (cars with blue in their colors).
			// Union: {100, 200, 300, 500, 600, 810, 820}.
			wantDocs: []uint64{100, 200, 300, 500, 600, 810, 820},
		},
		// orN over three sibling sub-object leaves under cars (all Ceiling=
		// true, all depth 4). Sort is a no-op. Fold: //   - spoiler OR 205 → sibling lift to LCA=cars (both cheap
		//     since Ceiling=pathRoot), same-Scope at cars. Ceiling=pathRoot.
		//   - result(cars, Ceiling=pathRoot) OR doors(cars.doors, Ceiling=pathRoot) →
		//     LCA=cars. result no-op (already at LCA). doors cheap
		//     lift to cars. Union. Ceiling=pathRoot.
		// Verifies the matrix's "OR always lands at LCA" rule across a
		// 3-way sibling union with full Ceiling preservation.
		{
			name: "orN(spoiler, tires.width=205, doors.count=4)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orN(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205),
					leafPositive(idx, "countries.garages.cars.doors.count", 4))
			},
			wantScope: "countries.garages.cars",
			// Ceiling = "" (fully authentic — see fastPathResult doc)
			// cars; each lifts to LCA=cars via the cheap path (Raw and Ceiling
			// preserved). OR result keeps the chain-up cleanness all the
			// way to the property root.
			wantCeiling: pathRoot,
			// spoiler: {100, 200, 800, 830, 900}.
			// 205-tires: {100, 200, 800, 900}.
			// doors=4: {800, 900}.
			// Union: {100, 200, 800, 830, 900}. doc 830 only via spoiler.
			wantDocs: []uint64{100, 200, 800, 830, 900},
		},
		// orN spanning three Scope levels (garages, cars, cars.tires), all
		// Ceiling=pathRoot. Sort places tires (depth 4), then cars-level honda
		// (depth 3), then warsaw (depth 2). Fold: //   - tires OR honda → LCA=cars. tires cheap lift to cars
		//     (Ceiling=pathRoot). honda no-op. Ceiling=pathRoot.
		//   - result(cars, Ceiling=pathRoot) OR warsaw(garages, Ceiling=pathRoot) →
		//     LCA=garages. result cheap lift to garages (Ceiling=pathRoot).
		//     warsaw no-op. Union at garages. Ceiling=pathRoot.
		// Verifies the container-exists semantic in a 3-way OR: doc 800
		// passes via warsaw alone, doc 810/820 via honda alone, etc.
		{
			name: "orN(garages.city=warsaw, cars.make=honda, cars.tires.width=205)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orN(idx,
					leafPositive(idx, "countries.garages.city", "warsaw"),
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205))
			},
			wantScope: "countries.garages",
			// Ceiling = "" (fully authentic — see fastPathResult doc)
			// (Ceiling=countries); each lift to LCA=garages is cheap and
			// preserves the operand's Ceiling.
			wantCeiling: pathRoot,
			// warsaw garages: {800, 900}.
			// make=honda: {100, 200, 300, 810, 820}.
			// 205-tires: {100, 200, 800, 900}.
			// Union: {100, 200, 300, 800, 810, 820, 900}.
			wantDocs: []uint64{100, 200, 300, 800, 810, 820, 900},
		},
		// NOT applied to an AND of ancestor + descendant. Inner AND: // warsaw(Scope=garages, Ceiling=pathRoot) AND doors=4(Scope=cars.doors,
		// Ceiling=pathRoot). Row 1 dispatch merges at child.Scope=cars.doors with
		// Ceiling=pathRoot; inner.Witnesses = doors under warsaw garages with count=4.
		// negate at cars.doors: doors NOT in inner.Witnesses. The result is
		// at the compound's Scope (cars.doors), per-element-existential.
		{
			name: "NOT (garages.city=warsaw AND cars.doors.count=4)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, andLeaves(idx,
					leafPositive(idx, "countries.garages.city", "warsaw"),
					leafPositive(idx, "countries.garages.cars.doors.count", 4)))
			},
			wantScope:   "countries.garages.cars.doors",
			wantCeiling: "countries.garages.cars.doors",
			// Inner Witnesses at cars.doors: doors under warsaw garages with
			// count=4 = doc 800 warsaw cars[0].doors[0], doc 900 warsaw
			// cars[0].doors[0].
			// NOT at cars.doors: every other doors-self position.
			// MaskRootLeaf: docs with some door NOT in inner.
			// - docs 100-700, 810-830: no doors-self bits at all → no
			//   witness possible → fail.
			// - doc 800: krakow cars[0/1].doors[0]=4 NOT in inner →
			//   witnesses → PASS.
			// - doc 900: krakow cars[0/1].doors[0]=4 NOT in inner →
			//   witnesses → PASS.
			wantDocs: []uint64{800, 900},
		},
		// NOT applied to an AND of siblings under cars. Inner AND: // spoiler(Scope=accessories) AND 205(Scope=tires) → sibling lift to
		// LCA=cars, same-Scope AND at cars, Ceiling=Scope. inner.Witnesses = cars
		// with both spoiler accessory and 205 tire in the same car.
		// negate at cars: cars NOT in inner.Witnesses.
		//
		// Result is trivially true at the doc level for these fixtures
		// because every doc has at least one car that fails one of the
		// conjuncts. The test still pins the algorithm's behavior: // negate at the sibling-AND Scope (cars) and per-element-
		// existential semantics.
		{
			name: "NOT (cars.accessories.type=spoiler AND cars.tires.width=205)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, andLeaves(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205)))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// Inner Witnesses at cars: cars with spoiler+205 in the same car =
			// doc 100 cars[0], doc 200 cars[0], doc 200 cars[1], doc 800
			// warsaw cars[0]. Every doc has at least one other car that
			// is not in inner.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		// NOT applied to an OR of same-scope leaves. Inner OR: // make=honda OR make=ford at cars (both Ceiling=pathRoot). Union Witnesses at
		// cars = cars whose make is honda or ford. negate at cars: // cars without honda/ford make. A doc fails only when every
		// car has make in {honda, ford}.
		{
			name: "NOT (cars.make=honda OR cars.make=ford)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.make", "ford")))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// Docs where every car has make in {honda, ford}: // - doc 300: cars[0]=ford, cars[1]=honda → all in → FAIL.
			// - doc 400: cars[0]=ford → all in → FAIL.
			// - doc 700: g[1] cars[0]=ford → all (one) in → FAIL.
			// Other docs have at least one car with a different make
			// (or no make at all → also not honda/ford).
			wantDocs: []uint64{100, 200, 500, 600, 800, 810, 820, 830, 900},
		},
		// NOT applied to an OR of ancestor + descendant. Inner OR: // warsaw(Scope=garages, Ceiling=pathRoot) OR honda(Scope=cars, Ceiling=pathRoot) → OR
		// always lands at LCA=garages. honda gets cheap lift to garages.
		// inner.Witnesses at garages = warsaw garages ∪ garages-with-honda-
		// cars. negate at garages: garages with neither.
		{
			name: "NOT (garages.city=warsaw OR cars.make=honda)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPositive(idx, "countries.garages.city", "warsaw"),
					leafPositive(idx, "countries.garages.cars.make", "honda")))
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			// Inner Witnesses at garages: warsaw garages {800 g[0], 900 g[0]}
			// ∪ honda-car garages {100 g[0], 200 g[0], 300 g[0],
			// 810 g[0], 820 country 0 g[0]}.
			// NOT at garages: garages with neither. Docs that pass: // - 400, 500, 600, 700: no warsaw, no honda → garages pass.
			// - 800: g[1] krakow has no honda → witness.
			// - 820: country 1 g[0] has no warsaw no honda → witness.
			// - 830: g[0] no warsaw, only car has no make → witness.
			// - 900: g[1] krakow no warsaw no honda → witness.
			// Docs that fail (every garage has warsaw or honda): // - 100, 200, 300, 810: only garage has honda.
			wantDocs: []uint64{400, 500, 600, 700, 800, 820, 830, 900},
		},
		// NOT applied to an OR of siblings under cars. Inner OR: // spoiler(Scope=accessories) OR 205(Scope=tires) → sibling lift to
		// LCA=cars. Each operand's Ceiling=pathRoot so both lifts are cheap.
		// inner.Witnesses at cars = cars with spoiler accessory or 205 tire.
		// negate at cars: cars without either.
		{
			name: "NOT (cars.accessories.type=spoiler OR cars.tires.width=205)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205)))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// Docs that fail (every car has spoiler or 205): // - 800: warsaw cars[0] (spoiler+205), krakow cars[0]
			//   (spoiler), krakow cars[1] (205) — all in inner → FAIL.
			// - 830: only car has spoiler → FAIL.
			// All other docs have at least one car without either.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 810, 820, 900},
		},
		// ---------------------------------------------------------------
		// De Morgan transformations of the three NOT(OR) tests above.
		// These manually express the rewrite NOT(A OR B) → NOT(A) AND
		// NOT(B). For same-scope clauses De Morgan is an equivalence and
		// the result coincides with the NOT(OR) form. For cross-scope
		// clauses (ancestor-child or siblings) the transformation diverges
		// — each NOT keeps its natural Scope, then the AND correlates
		// per-element at the deeper scope. This: //   - Includes docs where a non-warsaw garage contains a non-honda
		//     car alongside a honda car (lost by the NOT(OR) form because
		//     the garage gets flagged by the inner OR).
		//   - Excludes docs whose relevant scopes are empty (e.g. no
		//     accessories or no tires at all) since per-element AND
		//     requires an actual witness, not vacuous truth.
		// These tests pin the transformed semantics; they are not used by
		// the implementation today but document the expected behaviour
		// should a planner choose to apply the rewrite.

		// Same-scope De Morgan: should match its NOT(OR) counterpart docs
		// exactly. Acts as a sanity check that the transformation is a
		// no-op for same-Scope clauses.
		{
			name: "NOT(cars.make=honda) AND NOT(cars.make=ford) [De Morgan of NOT(honda OR ford)]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafNot(idx, "countries.garages.cars.make", "honda"),
					leafNot(idx, "countries.garages.cars.make", "ford"))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// Same wantDocs as NOT(honda OR ford) — De Morgan holds at
			// same scope. Docs that fail: 300 (cars[0]=ford, cars[1]=
			// honda → every car is honda or ford), 400 (only car is
			// ford), 700 (only car is ford).
			wantDocs: []uint64{100, 200, 500, 600, 800, 810, 820, 830, 900},
		},
		// Cross-scope De Morgan: ancestor-child relation (garages vs cars).
		// Diverges from NOT(warsaw OR honda) which excludes 100/200/300/
		// 810 because those garages contain a honda car. The transformed
		// form keeps per-element NOT(honda) at cars, then ANDs with the
		// per-element NOT(warsaw) at garages — every doc has at least one
		// non-warsaw garage with a non-honda car (or a car without make),
		// so all 12 docs pass.
		{
			name: "NOT(garages.city=warsaw) AND NOT(cars.make=honda) [De Morgan of NOT(warsaw OR honda)]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafNot(idx, "countries.garages.city", "warsaw"),
					leafNot(idx, "countries.garages.cars.make", "honda"))
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			// Per-garage: garage is not warsaw AND contains at least one
			// non-honda car descendant.
			// - 100 g[0]: no city set, cars[0]=toyota → passes.
			// - 200 g[0]: no city, cars[2]=name=ford (no make) → passes.
			// - 300 g[0]: cars[0]=ford → passes.
			// - 400-700: no warsaw, non-honda cars present → all pass.
			// - 800 g[1]=krakow, cars have no make field → passes.
			// - 810: no city, cars[1]=toyota → passes.
			// - 820: no city, cars[0]=ford → passes.
			// - 830: no city, cars[0] has no make → passes.
			// - 900 g[1]=krakow, cars have no make → passes.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		// Sibling De Morgan: accessories vs tires under cars. Diverges
		// from NOT(spoiler OR 205) which passes 10 docs (any car without
		// spoiler-or-205 attributes satisfies it vacuously). The
		// transformed form requires an actual non-spoiler accessory AND
		// a non-205 tire under the same car — empty-scope docs (no
		// accessories or no tires) fail.
		{
			name: "NOT(spoiler) AND NOT(205) [De Morgan of NOT(spoiler OR 205)]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafNot(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafNot(idx, "countries.garages.cars.tires.width", 205))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// Per-car: car has at least one non-spoiler accessory AND at
			// least one non-205 tire.
			// - doc 100 cars[1]: radio (non-spoiler) + 195 (non-205) ✓.
			// - doc 200: cars[0] and cars[1] both spoiler+205; cars[2]
			//   has no accessories/tires → no witnesses anywhere → FAIL.
			// - doc 830 cars[0]: radio but no tires → no 205 witness → FAIL.
			// - All other docs lack either accessories or tires entirely
			//   → no per-element witness on at least one side.
			wantDocs: []uint64{100},
		},

		// ContainsAny over a scalar field at Scope=cars. Equivalent to
		// orN of make=honda and make=ford with both at the same Scope.
		// Same-Scope OR: pure union with no lifts, Ceiling=pathRoot preserved.
		{
			name: "cars.make ContainsAny [honda, ford]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafContainsAny(idx, "countries.garages.cars.make", "honda", "ford")
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: pathRoot,
			// honda: {100, 200, 300, 810, 820}.
			// ford: {300, 400, 500, 600, 700, 820}.
			// Union: {100, 200, 300, 400, 500, 600, 700, 810, 820}.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 810, 820},
		},
		// ContainsAny with a rarer value set. Verifies orN over makes
		// not covered by the main fixtures (bmw, kia).
		{
			name: "cars.make ContainsAny [bmw, kia]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafContainsAny(idx, "countries.garages.cars.make", "bmw", "kia")
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: pathRoot,
			// bmw: doc 500 g[1] cars[0], doc 600 country 1 g[0] cars[0].
			// kia: doc 820 country 1 g[0] cars[0].
			// Union: {500, 600, 820}.
			wantDocs: []uint64{500, 600, 820},
		},
		// ContainsAll over a scalar-array field at Scope=cars. Equivalent
		// to andN of colors=red and colors=blue, both at Scope=cars. Same-
		// Scope AND enforces per-car co-occurrence: only cars whose colors
		// array contains BOTH values survive.
		{
			name: "cars.colors ContainsAll [red, blue]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafContainsAll(idx, "countries.garages.cars.colors", "red", "blue")
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// Only doc 100 cars[1].colors = ["blue", "red"] satisfies
			// both. Other docs either lack one of the values or have
			// them split across different cars (e.g. doc 200 cars[0]
			// has red, cars[1] has blue).
			wantDocs: []uint64{100},
		},
		// Pinned ContainsAny on cars[1].make. Narrows the value-union
		// (honda ∪ ford) with the cars[1] pin idx before lifting to
		// Scope=garages.
		{
			name: "cars[1].make ContainsAny [honda, ford]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAny(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages.cars", 1}},
					"honda", "ford")
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			// cars[1].make=honda: doc 100, 200, 300, 820 country 0.
			// cars[1].make=ford: doc 500 g[0].
			// Union: {100, 200, 300, 500, 820}.
			wantDocs: []uint64{100, 200, 300, 500, 820},
		},
		// Pinned ContainsAll on cars[1].colors. The value-intersection
		// (red ∩ blue) collapses to cars where the same cars[1].colors
		// array contains both, then the cars[1] pin narrows to garages
		// whose cars[1] qualifies.
		{
			name: "cars[1].colors ContainsAll [red, blue]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAll(idx,
					"countries.garages.cars.colors",
					[]pinSpec{{"countries.garages.cars", 1}},
					"red", "blue")
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			// Only doc 100 cars[1].colors = ["blue", "red"] satisfies
			// both at cars[1]. doc 200 cars[1] = [blue] (no red); doc
			// 300 cars[1] = [red] (no blue).
			wantDocs: []uint64{100},
		},
		// Multi-pin ContainsAll: garages[0].cars[1].colors ContainsAll
		// [red, blue]. Two pins narrow the value-intersection at every
		// level of the chain (garages[0] AND cars[1]). Scope = parent of
		// outermost pin = countries.
		{
			name: "garages[0].cars[1].colors ContainsAll [red, blue]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAll(idx,
					"countries.garages.cars.colors",
					[]pinSpec{
						{"countries.garages", 0},
						{"countries.garages.cars", 1},
					},
					"red", "blue")
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			// Only doc 100 g[0].cars[1].colors = ["blue", "red"]
			// satisfies both at the pinned slot. doc 200 g[0].cars[1]
			// has only blue; doc 300 g[0].cars[1] has only red.
			wantDocs: []uint64{100},
		},
		// Intermediate-pin ContainsAny: garages[1].cars.make ContainsAny
		// [honda, ford]. Pinned at garages level only; cars unpinned, so
		// any car under garages[1] can satisfy. Scope = countries.
		{
			name: "garages[1].cars.make ContainsAny [honda, ford]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAny(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}},
					"honda", "ford")
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			// Need a doc with garages[1] whose cars include make=honda
			// or make=ford. Only doc 700 g[1].cars[0]=ford qualifies.
			// doc 500 g[1] cars have only "name" fields (no make).
			// doc 800/900 g[1] cars have no make at all.
			// Other docs have no g[1].
			wantDocs: []uint64{700},
		},
		// Multi-pin ContainsAny: garages[0].cars[2].name ContainsAny
		// [honda, ford]. Two pins narrow the value-union at every
		// chain level. Scope = countries.
		{
			name: "garages[0].cars[2].name ContainsAny [honda, ford]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAny(idx,
					"countries.garages.cars.name",
					[]pinSpec{
						{"countries.garages", 0},
						{"countries.garages.cars", 2},
					},
					"honda", "ford")
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			// doc 100 g[0].cars[2].name=honda ✓.
			// doc 200 g[0].cars[2].name=ford ✓.
			// doc 600 country 0 g[0].cars[2].name=ford ✓; country 1
			//   g[0].cars[2].name=honda ✓.
			// doc 500 g[0] has only 2 cars → no cars[2].
			// Other docs either lack cars[2] in g[0] or have no name
			// field there.
			wantDocs: []uint64{100, 200, 600},
		},
		// Intermediate-pin ContainsAll: garages[0].cars.colors ContainsAll
		// [red, blue]. Pinned at garages only; cars unpinned. Value-
		// intersection at cars (per-car co-occurrence) narrowed by
		// garages[0]; the same car under g[0] must contain both colors.
		// Scope = countries.
		{
			name: "garages[0].cars.colors ContainsAll [red, blue]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAll(idx,
					"countries.garages.cars.colors",
					[]pinSpec{{"countries.garages", 0}},
					"red", "blue")
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			// doc 100 g[0].cars[1].colors=[blue, red] satisfies both
			// at a single car. doc 200 has red and blue split across
			// cars[0] and cars[1] → no per-car co-occurrence. doc 300
			// has only red. No other doc has both colors anywhere.
			wantDocs: []uint64{100},
		},
		// ---------------------------------------------------------------
		// ContainsNone equivalence trios. Each trio runs the same query
		// in three syntactic forms — every form produces identical
		// wantDocs because ContainsNone is owner-level by design: //   (a) leafContainsNone / leafPinnedContainsNone — direct
		//       chained AndNot (pinned variant always owner-level,
		//       regardless of pin layout).
		//   (b) negate(leafContainsAny / leafPinnedContainsAny) —
		//       universe-subtract of positive ContainsAny.
		//   (c) negate(orLeaves(value-positive, value-positive)) — the
		//       canonical NOT-of-explicit-OR form.
		// Covers unpinned, fully-pinned single-pin, and intermediate-pin.
		// All three forms agree everywhere because the operator's
		// natural reading ("the array contains none of these values")
		// is universal over the array — the pinned variant doesn't
		// dispatch on hasPinGap. See
		// TestFastPathL2_IntermediatePinContainsNoneAgreement for a
		// discriminator fixture that confirms agreement on a mixed
		// matching/non-matching pattern.

		// --- Unpinned trio: cars.make ∉ {honda, ford} ------------------
		// Form (c) for the unpinned trio is already covered by the
		// existing "NOT (cars.make=honda OR cars.make=ford)" test
		// further up — same wantDocs proves the equivalence.
		{
			name: "cars.make ContainsNone [honda, ford] [direct]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafContainsNone(idx, "countries.garages.cars.make", "honda", "ford")
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// Per-car: a car whose make is not honda and not ford
			// (cars without a make field also satisfy). Excludes docs
			// where every car has make in {honda, ford}: // - doc 300: cars[0]=ford, cars[1]=honda → FAIL.
			// - doc 400: only car is ford → FAIL.
			// - doc 700: only car is ford → FAIL.
			wantDocs: []uint64{100, 200, 500, 600, 800, 810, 820, 830, 900},
		},
		{
			name: "NOT (cars.make ContainsAny [honda, ford]) [via negate]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, leafContainsAny(idx, "countries.garages.cars.make", "honda", "ford"))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// Same docs as the ContainsNone form — proves the direct
			// helper matches universe-subtract of ContainsAny.
			wantDocs: []uint64{100, 200, 500, 600, 800, 810, 820, 830, 900},
		},

		// --- Single-pin trio: cars[1].make ∉ {honda, ford} -------------
		// Fully-pinned (no gap) — leafPinnedContainsNone routes through
		// negate(ContainsAny). All three forms compute the owner-level
		// result, which coincides with per-element because cars[1] is
		// uniquely identified per garage.
		{
			name: "cars[1].make ContainsNone [honda, ford] [direct]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsNone(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages.cars", 1}},
					"honda", "ford")
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			// cars[1].make=honda fires for docs 100/200/300 + 820 c0.
			// cars[1].make=ford fires for doc 500 g[0].
			// Per-garage: garage passes if its cars[1] is neither honda
			// nor ford (cars[1] missing or no make also counts).
			// - doc 100/200/300: only garage's cars[1] is honda → FAIL.
			// - doc 400: cars[1] missing → garage passes.
			// - doc 500: g[0] ford fails, g[1] cars[1]=name=honda (no
			//   make field) passes → doc passes.
			// - doc 820: country 0 honda fails, country 1 mazda passes.
			// - all other docs have a passing garage.
			wantDocs: []uint64{400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		{
			name: "NOT (cars[1].make ContainsAny [honda, ford]) [via negate]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, leafPinnedContainsAny(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages.cars", 1}},
					"honda", "ford"))
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			wantDocs:    []uint64{400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		{
			name: "NOT (cars[1].make=honda OR cars[1].make=ford) [via NOT-of-OR]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPinnedPositive(idx, "countries.garages.cars.make", "honda",
						[]pinSpec{{"countries.garages.cars", 1}}),
					leafPinnedPositive(idx, "countries.garages.cars.make", "ford",
						[]pinSpec{{"countries.garages.cars", 1}})))
			},
			wantScope:   "countries.garages",
			wantCeiling: "countries.garages",
			wantDocs:    []uint64{400, 500, 600, 700, 800, 810, 820, 830, 900},
		},

		// --- Intermediate-pin trio: garages[1].cars.make ∉ {honda, ford}
		// All three forms are owner-level by construction:
		//   - leafPinnedContainsNone = negate(leafPinnedContainsAny)
		//     (does NOT dispatch on hasPinGap — see the helper's doc).
		//   - The two negate variants invert the positive support at
		//     countries directly.
		// On the standard L2 fixtures the three trivially agree because
		// no doc has g[1] containing both a matching and a non-matching
		// car. TestFastPathL2_IntermediatePinContainsNoneAgreement below
		// adds doc 1000 to verify they still agree on the discriminator
		// pattern where a matching and a non-matching car coexist in g[1].
		{
			name: "garages[1].cars.make ContainsNone [honda, ford] [direct, owner-level]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsNone(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}},
					"honda", "ford")
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			// Owner-level: doc passes iff no country in it has g[1]
			// containing a car with make ∈ {honda, ford}. ContainsAny's
			// positive support = {country 0 of doc 700} (only country
			// whose g[1] holds a honda-or-ford car — cars[0]=ford). negate
			// at countries = every country except that one → doc 700 is
			// the sole exclusion.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 800, 810, 820, 830, 900},
		},
		{
			name: "NOT (garages[1].cars.make ContainsAny [honda, ford]) [via negate]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, leafPinnedContainsAny(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}},
					"honda", "ford"))
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			// Same construction as ContainsNone direct above
			// (leafPinnedContainsNone is defined as this very negate). Same
			// witnesses, same exclusion.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 800, 810, 820, 830, 900},
		},
		{
			name: "NOT (garages[1].cars.make=honda OR garages[1].cars.make=ford) [via NOT-of-OR]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPinnedPositive(idx, "countries.garages.cars.make", "honda",
						[]pinSpec{{"countries.garages", 1}}),
					leafPinnedPositive(idx, "countries.garages.cars.make", "ford",
						[]pinSpec{{"countries.garages", 1}})))
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			wantDocs:    []uint64{100, 200, 300, 400, 500, 600, 800, 810, 820, 830, 900},
		},
		// Sibling AND with mixed Ceiling: A=radio at accessories (Ceiling=pathRoot) AND
		// B=NOT 205 at tires (Ceiling=Scope). Both lifts land at LCA=cars: A
		// via cheap anchor-intersect (Ceiling=pathRoot), B via real LiftToAncestor
		// on its Witnesses (Ceiling=Scope). Per-car AND requires the same
		// car to have a radio accessory AND a non-205 tire.
		{
			name: "cars.accessories.type=radio AND NOT cars.tires.width=205",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "radio"),
					leafNot(idx, "countries.garages.cars.tires.width", 205))
			},
			wantScope:   "countries.garages.cars",
			wantCeiling: "countries.garages.cars",
			// radio cars: doc 100 cars[1] (accessories=[radio]) and
			// doc 830 cars[0] (accessories=[radio, spoiler]). doc 200's
			// cars all have spoiler, not radio.
			// non-205 tire cars (per-element witness): doc 100 cars[1]
			// (tire=195), doc 200 cars[1] (tire=195). Other tire-bearing
			// cars have only 205. Cars without tires have no per-element
			// witness (empty scope).
			// Intersection at cars-self: doc 100 cars[1]. doc 830 cars[0]
			// has radio but no tires → no non-205 witness.
			wantDocs: []uint64{100},
		},
	}

	runFastPathCases(t, idx, cases)
}

// TestFastPathL2_IntermediatePinContainsNoneAgreement uses a fixture
// pattern the shared L2 set doesn't contain — garages[1] holding both
// a matching car (honda) and a non-matching car (toyota) — to confirm
// that all three ContainsNone forms agree even on this discriminator: //
//   - leafPinnedContainsNone (always owner-level, no hasPinGap dispatch).
//   - negate(leafPinnedContainsAny).
//   - negate(orLeaves(positive, positive)).
//
// All three exclude doc 1000 because garages[1] contains a honda car,
// which is the natural reading of "ContainsNone [honda, ford]": the
// array must contain *none* of those values, and one honda is enough
// to fail the predicate.
//
// Historical note: an earlier version of leafPinnedContainsNone used
// per-element semantics via hasPinGap dispatch (matching the
// leafPinnedNot family), which produced wantDocs={1000} here — a
// non-matching car (toyota) would witness the predicate even when
// matching cars were present in the same garage. That contradicted
// the operator's English meaning and was reverted in favour of
// uniform owner-level semantics for ContainsNone.
func TestFastPathL2_IntermediatePinContainsNoneAgreement(t *testing.T) {
	prop := l2Schema()
	idx := newFastPathIndex("countries")
	// Discriminator doc: garages[1] contains a honda car AND a toyota
	// car. honda matches the ContainsNone subtractand set; toyota does
	// not. Per-element would let toyota witness the predicate; owner-
	// level rejects the country because the garage contains a honda car.
	// All three forms below use owner-level → all three reject.
	idx.addDoc(t, prop, 1000, []any{
		map[string]any{"garages": []any{
			map[string]any{"cars": []any{}},
			map[string]any{"cars": []any{
				map[string]any{"make": "honda"},
				map[string]any{"make": "toyota"},
			}},
		}},
	})

	cases := []fastPathTestCase{
		// Direct helper. After the owner-level change this goes through
		// negate(leafPinnedContainsAny) internally. honda is lifted to
		// country 0 via the positive ContainsAny; negate excludes it,
		// result empty.
		{
			name: "garages[1].cars.make ContainsNone [honda, ford] [direct, owner-level]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsNone(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}},
					"honda", "ford")
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			wantDocs:    nil,
		},
		// Owner-level via explicit negate(ContainsAny).
		{
			name: "NOT (garages[1].cars.make ContainsAny [honda, ford]) [via negate]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, leafPinnedContainsAny(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}},
					"honda", "ford"))
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			wantDocs:    nil,
		},
		// Owner-level via canonical NOT-of-OR. positive(honda) fires on
		// country 0, OR includes it, negate excludes it, result empty.
		{
			name: "NOT (garages[1].cars.make=honda OR garages[1].cars.make=ford) [via NOT-of-OR]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPinnedPositive(idx, "countries.garages.cars.make", "honda",
						[]pinSpec{{"countries.garages", 1}}),
					leafPinnedPositive(idx, "countries.garages.cars.make", "ford",
						[]pinSpec{{"countries.garages", 1}})))
			},
			wantScope:   "countries",
			wantCeiling: "countries",
			wantDocs:    nil,
		},
	}

	runFastPathCases(t, idx, cases)
}

// TestFastPathL2_AncestorChildAND_CleanRange pins the cleanness ceiling
// for ancestor+child AND.
//
// 2-leaf compound `garages.city=warsaw AND cars.doors.count=4`.
// A.Scope=garages, C.Scope=cars.doors. The broadcasting branch sets
// Ceiling=garages (= ancestor.Scope), so [cars.doors, cars, garages] is
// asserted clean and countries is explicitly disclaimed.
//
// Self-contained mini-index:
//   - doc 1: warsaw garage with a car having doors.count=4 → genuine
//     same-garage match.
//   - doc 2: warsaw garage with NO doors.count=4 in its cars, plus a
//     krakow garage with doors.count=4. No same-garage match: the
//     intersection at cars.doors / cars / garages is empty, but at
//     countries-level both operands' country chain bits survive —
//     owner-level ghost.
//
// Verifies both halves: projections at cars.doors / cars / garages
// yield wantDocs=[1], and the projection at countries picks up doc 2 as
// the ghost — confirming it sits outside the clean range as Ceiling
// disclaims.
func TestFastPathL2_AncestorChildAND_CleanRange(t *testing.T) {
	prop := l2Schema()
	idx := newFastPathIndex("countries")

	// doc 1: genuine same-garage match (warsaw + doors=4 under same garage).
	idx.addDoc(t, prop, 1, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "warsaw", "cars": []any{
				map[string]any{"doors": []any{map[string]any{"count": 4}}},
			}},
		}},
	})
	// doc 2: split — warsaw garage has no doors=4; krakow garage does.
	// Owner-level "country has warsaw garage AND country has doors=4
	// somewhere" is true, but no SAME garage satisfies both.
	idx.addDoc(t, prop, 2, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "warsaw", "cars": []any{
				map[string]any{"doors": []any{map[string]any{"count": 99}}},
			}},
			map[string]any{"city": "krakow", "cars": []any{
				map[string]any{"doors": []any{map[string]any{"count": 4}}},
			}},
		}},
	})

	r := andLeaves(idx,
		leafPositive(idx, "countries.garages.city", "warsaw"),
		leafPositive(idx, "countries.garages.cars.doors.count", 4))

	require.Equal(t, "countries.garages.cars.doors", r.Scope)
	// Ceiling = ancestor.Scope, the structural ceiling for ancestor+child
	// AND. Strictly above this scope (= "countries"), owner-level ghosts
	// can survive — and we explicitly disclaim cleanness there.
	require.Equal(t, "countries.garages", r.Ceiling)

	wantDocs := []uint64{1}

	// Baseline: Witnesses at Scope yields the correct docs.
	assert.ElementsMatch(t, wantDocs, idx.docIDs(r),
		"Witnesses at Scope must yield the same-garage match")

	// Inside the clean range — projections must be authentic.
	for _, scope := range []string{
		"countries.garages.cars", // parent(Scope), inside clean range
		"countries.garages",      // Ceiling, top of the clean range
	} {
		richAtScope := r.Raw.Clone().And(idx.anchorAt(scope))
		docs, _ := idx.ops.MaskRootLeaf(richAtScope)
		assert.ElementsMatch(t, wantDocs, docs.ToArray(),
			"inside clean range: Raw ∩ _anchor(%q) must yield wantDocs", scope)
	}

	// Strictly above the clean range — projection at countries picks up
	// doc 2 as an owner-level ghost, which Ceiling=garages correctly
	// disclaims.
	richAtCountries := r.Raw.Clone().And(idx.anchorAt("countries"))
	docsAtCountries, _ := idx.ops.MaskRootLeaf(richAtCountries)
	assert.ElementsMatch(t, []uint64{1, 2}, docsAtCountries.ToArray(),
		"above Ceiling: projection picks up the owner-level ghost (doc 2) — this is why countries is disclaimed")
}

// TestFastPathL2_CompoundANDLiftedAboveCleanScope exercises a compound
// whose Ceiling sits at an intermediate scope (e.g. ancestor.Scope from
// an ancestor+child AND), then re-merged with another operand whose
// Scope is ABOVE that Ceiling.
//
// `(garages.city=warsaw AND cars.doors.count=4)` has Ceiling=garages.
// AND it with `garages IS NULL false` (Scope=countries) — forces the
// merge scope up to countries. The compound's Ceiling=garages signals
// "not clean at countries", so liftToScope takes the full-lift path
// (rebuild Raw at countries from authentic Witnesses), which wipes the
// ghost.
//
// Discriminator: doc 2 has a warsaw garage and a separate krakow
// garage-with-doors=4 (no same-garage match). doc 2 must NOT appear in
// the result. doc 1 and doc 3 (genuine same-garage matches) must
// appear.
//
// Also pins the andLeaves dispatch guard: when the child of
// ancestor+child AND has ceilingAboveScope=true but its Ceiling is
// DEEPER than ancestor.Scope, the merge must lift child to
// ancestor.Scope — child's bits at ancestor.Scope aren't authentic.
func TestFastPathL2_CompoundANDLiftedAboveCleanScope(t *testing.T) {
	prop := l2Schema()
	idx := newFastPathIndex("countries")

	// doc 1: genuine same-garage match.
	idx.addDoc(t, prop, 1, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "warsaw", "cars": []any{
				map[string]any{"doors": []any{map[string]any{"count": 4}}},
			}},
		}},
	})
	// doc 2: split fixture. Country has a warsaw garage (no doors=4)
	// and a krakow garage (with doors=4). NO same-garage match — must
	// be excluded from `warsaw AND doors=4 AND garages IS NULL false`.
	idx.addDoc(t, prop, 2, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "warsaw", "cars": []any{
				map[string]any{"doors": []any{map[string]any{"count": 99}}},
			}},
			map[string]any{"city": "krakow", "cars": []any{
				map[string]any{"doors": []any{map[string]any{"count": 4}}},
			}},
		}},
	})
	// doc 3: another genuine same-garage match — extra positive evidence
	// that the result isn't accidentally empty for some unrelated reason.
	idx.addDoc(t, prop, 3, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "warsaw", "cars": []any{
				map[string]any{"doors": []any{map[string]any{"count": 4}}},
			}},
		}},
	})

	// Step 1: build the ancestor+child compound. Ceiling = garages
	// per the broadcasting rule.
	compound := andLeaves(idx,
		leafPositive(idx, "countries.garages.city", "warsaw"),
		leafPositive(idx, "countries.garages.cars.doors.count", 4))
	require.Equal(t, "countries.garages.cars.doors", compound.Scope)
	require.Equal(t, "countries.garages", compound.Ceiling)

	// Step 2: AND with `garages IS NULL false` (Scope=countries=propName).
	// This forces the merge to land at countries — strictly above the
	// compound's Ceiling=garages — exercising the lift-above-Ceiling path.
	garagesExist := leafIsNullFalse(idx, "countries.garages")
	require.Equal(t, "countries", garagesExist.Scope)
	require.Equal(t, pathRoot, garagesExist.Ceiling)

	r := andLeaves(idx, compound, garagesExist)

	// The result must yield only the same-garage matches. Doc 2 has no
	// same-garage witness for warsaw+doors=4 and must NOT leak.
	wantDocs := []uint64{1, 3}
	assert.ElementsMatch(t, wantDocs, idx.docIDs(r),
		"compound AND (Ceiling=garages) merged with leaf at countries (above Ceiling) must lift compound to countries — ghosts above Ceiling get wiped; doc 2 must not leak")
}
