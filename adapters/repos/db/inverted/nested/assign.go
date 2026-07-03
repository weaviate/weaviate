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
	"fmt"
	"time"

	"github.com/google/uuid"
	filnested "github.com/weaviate/weaviate/entities/filters/nested"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// ElemIdx is the raw element index stored in posArena and all walker scratch
// buffers. It is a named type so that passing a raw index to Encode (which expects
// uint32) requires an explicit cast, keeping the raw-index → encoded-uint64
// boundary visible and compile-checked.
type ElemIdx uint32

// PosRange locates a contiguous block of ElemIdx values inside AssignResult.posArena.
// off and length are plain uint32 slot counts — NOT element indices — and
// intentionally do NOT use the ElemIdx named type.
type PosRange struct{ off, length uint32 }

// ValueEntry represents a leaf value discovered during position assignment.
// Pos references the element's positions in AssignResult.posArena; the caller
// expands them via AssignResult.Positions and ORs in the real docID.
type ValueEntry struct {
	Path         string                     // dot-notation path, e.g. "addresses.city"
	PropName     string                     // leaf property name, e.g. "city"
	Value        any                        // raw value (caller analyzes based on DataType)
	DataType     schema.DataType            // scalar data type for value analysis
	Tokenization string                     // tokenization strategy (for text types)
	TextAnalyzer *models.TextAnalyzerConfig // custom text analyzer config (may be nil)
	Pos          PosRange                   // positions range in posArena
}

// IdxEntry records which positions belong to a specific array element.
// Used for same-element verification in cross-sibling filter correlation.
type IdxEntry struct {
	Path  string   // array path, e.g. "addresses" or "owner.nicknames"
	Index int      // 0-based element index
	Pos   PosRange // positions range in posArena
}

// ExistsEntry records which positions have a given property present.
// Used for IS NULL checks and ALL/NONE filter operators.
type ExistsEntry struct {
	Path string   // property path; empty string for root-level exists
	Pos  PosRange // positions range in posArena
}

// AnchorEntry records the raw self-marker index of a single element at a given path.
// Distinct from ExistsEntry: ExistsEntry stores the full elementPositions
// (self marker + descendants); AnchorEntry stores only the self marker.
type AnchorEntry struct {
	Path     string  // path of the element's containing collection; empty for root-level
	Position ElemIdx // self marker (raw index, no docID); exactly one marker per entry
}

// propKind classifies a schema property for the walker.
type propKind uint8

const (
	propKindScalar      propKind = iota // plain scalars: text, int, bool, …
	propKindScalarArray                 // scalar arrays: text[], int[], …
	propKindNested                      // nested objects: object, object[]
)

// propSchema is the precomputed, per-level schema descriptor built once from
// *models.NestedProperty before the walk. The path string and classification
// are derived from the schema, not the data, so they are identical for every
// element visited at the same schema level.
type propSchema struct {
	name         string
	path         string
	dt           schema.DataType
	tokenization string
	textAnalyzer *models.TextAnalyzerConfig
	kind         propKind
	nestedDT     schema.DataType // for propKindNested: DataTypeObject or DataTypeObjectArray
	children     LevelSchema     // populated only for propKindNested
}

// LevelSchema holds the categorised property descriptors for one schema level,
// split into two sub-slices the walker processes in order:
//   - arrayOrNested — processed first in Phase 1; these claim elemIdx via recursion.
//   - scalars       — processed after elementPositions is built.
//
// Preserving the original nestedProps order within each sub-slice satisfies
// the DFS pre-order elemIdx invariant: nested/array children claim elemIdx
// before any scalar emission, and the global counter advances in the same
// order as the original two-pass algorithm.
//
// LevelSchema is immutable after construction: no walker or caller writes any
// field during a walk. It is safe to share across concurrent
// AssignPositionsFromSchema calls without additional synchronisation.
type LevelSchema struct {
	arrayOrNested []propSchema
	scalars       []propSchema
	// maxDepth is the deepest nesting level within this schema subtree.
	// depth=0 means this level has no nested or scalar-array children.
	// Consumed by newWalker to pre-allocate the depth-indexed scratch slices
	// (descs/subtrees) of size maxDepth+1.
	maxDepth int
}

// buildLevelSchema precomputes the schema tree once before the walk starts.
// It calls joinPath exactly once per property, classifies each into
// arrayOrNested or scalars while preserving the original nestedProps order
// within each sub-slice, and recurses into nested children.
func buildLevelSchema(prefix string, nestedProps []*models.NestedProperty) LevelSchema {
	ls := LevelSchema{
		arrayOrNested: make([]propSchema, 0, len(nestedProps)),
		scalars:       make([]propSchema, 0, len(nestedProps)),
	}
	for _, np := range nestedProps {
		path := joinPath(prefix, np.Name)
		dt := schema.DataType(np.DataType[0])
		ps := propSchema{
			name:         np.Name,
			path:         path,
			dt:           dt,
			tokenization: np.Tokenization,
			textAnalyzer: np.TextAnalyzer,
		}
		switch {
		case schema.IsNested(dt):
			ps.kind = propKindNested
			ps.nestedDT = dt
			ps.children = buildLevelSchema(path, np.NestedProperties)
			ls.arrayOrNested = append(ls.arrayOrNested, ps)
		case schema.IsScalarArrayType(dt):
			ps.kind = propKindScalarArray
			ls.arrayOrNested = append(ls.arrayOrNested, ps)
		default:
			ps.kind = propKindScalar
			ls.scalars = append(ls.scalars, ps)
		}
	}

	// Compute maxDepth: the deepest nesting level reachable from this schema node.
	depth := 0
	for _, ps := range ls.arrayOrNested {
		if ps.kind == propKindNested {
			if d := ps.children.maxDepth + 1; d > depth {
				depth = d
			}
		} else if 1 > depth {
			// Scalar-array nodes never recurse, but their parent walkObject call
			// still needs a descs/subtrees slot at its own depth. depth=1 ensures
			// that slot exists. This is a safe conservative choice — at most one
			// extra (unused) entry is allocated at the leaf level — not an off-by-one.
			depth = 1
		}
	}
	ls.maxDepth = depth

	return ls
}

// BuildSchema validates that prop is a nested type and precomputes the
// LevelSchema tree from its NestedProperties. The returned schema is
// immutable and safe to cache for the lifetime of a collection, then pass to
// AssignPositionsFromSchema for each write without rebuilding.
func BuildSchema(prop *models.Property) (LevelSchema, error) {
	if _, ok := schema.AsNested(prop.DataType); !ok {
		return LevelSchema{}, fmt.Errorf("property %q is not a nested type", prop.Name)
	}
	return buildLevelSchema("", prop.NestedProperties), nil
}

// AssignResult holds all positioned values and metadata produced by
// walking a nested property value.
//
// posArena is the flat backing store for all position data. ValueEntry,
// IdxEntry, and ExistsEntry carry PosRange values that index into posArena;
// callers expand them via Positions.
type AssignResult struct {
	posArena []ElemIdx
	Values   []ValueEntry
	Idx      []IdxEntry
	Exists   []ExistsEntry
	Anchors  []AnchorEntry
}

// Positions returns the ElemIdx slice at range p within posArena.
// The returned slice aliases posArena — callers must not modify it.
func (r *AssignResult) Positions(p PosRange) []ElemIdx {
	return r.posArena[p.off : p.off+p.length]
}

// AssignPositionsFromSchema is the hot path for repeated writes against the
// same collection. Callers build ls once with BuildSchema and reuse it across
// all writes, avoiding the per-call schema tree construction.
//
// The property must be of type object or object[]. For object type,
// the value is wrapped in a 1-element array.
//
// All positions are raw element indices (ElemIdx); callers convert them to
// encoded uint64 values with the real docID via PositionsWithDocID.
//
// A single walker persists across all top-level elements so that elemIdx
// advances globally and never resets per root. This makes the predecessor
// scan in LiftToAncestor correct for any ancestor level without a separate
// root-index axis.
func AssignPositionsFromSchema(ls LevelSchema, prop *models.Property, value any) (*AssignResult, error) {
	if value == nil {
		return &AssignResult{}, nil
	}

	dt, ok := schema.AsNested(prop.DataType)
	if !ok {
		return nil, fmt.Errorf("property %q is not a nested type", prop.Name)
	}

	var elements []any
	switch dt {
	case schema.DataTypeObject:
		elements = []any{value}
	case schema.DataTypeObjectArray:
		arr, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("expected []any for object[] %q, got %T", prop.Name, value)
		}
		elements = arr
	default:
		// Unreachable: dt is returned by AsNested which only returns members of
		// NestedDataTypes (DataTypeObject, DataTypeObjectArray).
		return nil, fmt.Errorf("property %q has unexpected data type %q", prop.Name, dt)
	}

	if len(elements) == 0 {
		return &AssignResult{}, nil
	}

	result := &AssignResult{}
	w := newWalker(result, ls.maxDepth)

	for i, elem := range elements {
		elemMap, ok := elem.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected map[string]any for %q[%d], got %T",
				prop.Name, i, elem)
		}

		// Root elements have no ancestors — w.chainDepth is 0. walkObject
		// returns epOff pointing to the start of the chain[:0]+self+desc block,
		// which for root elements (chainDepth==0) is just self+desc.
		epOff, err := w.walkObject(0, "", ls, elemMap)
		if err != nil {
			return nil, fmt.Errorf("walk element %d of %q: %w", i, prop.Name, err)
		}
		epLen := uint32(len(result.posArena)) - epOff

		// Root-level _idx entry: records which positions belong to root element i.
		// Required for arr[N] positional filtering (e.g. "addresses[1].city = X").
		result.Idx = append(result.Idx, IdxEntry{
			Path:  "",
			Index: i,
			Pos:   PosRange{epOff, epLen},
		})
	}

	// Root-level _exists: all positions across all root elements (union of subtreeSelves).
	if len(w.rootSelves) > 0 {
		exOff := uint32(len(result.posArena))
		result.posArena = append(result.posArena, w.rootSelves...)
		exLen := uint32(len(result.posArena)) - exOff
		result.Exists = append(result.Exists, ExistsEntry{
			Path: "",
			Pos:  PosRange{exOff, exLen},
		})
	}

	return result, nil
}

// AssignPositions is a convenience wrapper that builds the schema tree from
// prop on each call and delegates to AssignPositionsFromSchema. For repeated
// writes to the same collection, prefer building the schema once with
// BuildSchema and calling AssignPositionsFromSchema directly.
func AssignPositions(prop *models.Property, value any) (*AssignResult, error) {
	ls, err := BuildSchema(prop)
	if err != nil {
		return nil, err
	}
	return AssignPositionsFromSchema(ls, prop, value)
}

// walker is not goroutine-safe; each call to AssignPositions must use its own instance.
//
// Concurrency safety: LevelSchema and propSchema are immutable after construction
// and safe to share across concurrent AssignPositionsFromSchema calls. The walker
// itself (elemIdx, posArena writes, chain, descs, subtrees, sels, rootSelves) is
// owned by a single goroutine per call.
type walker struct {
	elemIdx    uint32
	result     *AssignResult
	chain      []ElemIdx // ancestor self-markers; w.chain[:chainDepth] is the valid prefix
	chainDepth int       // number of valid entries in w.chain; the rest is scratch capacity
	// descs[d] accumulates descendant self-markers for walkObject(selfDepth=d).
	// Reset to [:0] at the start of each walkObject(d) call.
	descs [][]ElemIdx
	// subtrees[d] accumulates subtreeSelves from walkObject(depth=d+1) elements.
	// Reset to [:0] at the start of each walkNestedArray(parentDepth=d) call.
	subtrees [][]ElemIdx
	// sels holds the self-markers emitted by the current walkScalarArray call.
	// Reset to [:0] at the start of each walkScalarArray call.
	sels []ElemIdx
	// rootSelves accumulates subtreeSelves for all root-level walkObject calls.
	// Written into posArena once, after the element loop, as the root ExistsEntry.
	rootSelves []ElemIdx
}

func newWalker(result *AssignResult, maxDepth int) *walker {
	w := &walker{
		elemIdx:    1,
		result:     result,
		chain:      make([]ElemIdx, 0, maxDepth+1),
		descs:      make([][]ElemIdx, maxDepth+1),
		subtrees:   make([][]ElemIdx, maxDepth+1),
		sels:       make([]ElemIdx, 0, 8),
		rootSelves: make([]ElemIdx, 0, 8),
	}
	for i := range w.descs {
		w.descs[i] = make([]ElemIdx, 0, 4)
		w.subtrees[i] = make([]ElemIdx, 0, 4)
	}
	return w
}

func (w *walker) nextElem() (ElemIdx, error) {
	if int(w.elemIdx) >= MaxElems {
		return 0, fmt.Errorf("element count %d exceeds maximum %d", w.elemIdx, MaxElems-1)
	}
	idx := ElemIdx(w.elemIdx)
	w.elemIdx++
	return idx, nil
}

// walkObject processes a single object element depth-first.
//
// selfDepth is the nesting depth of this element (0 for root elements).
// The ancestor chain is maintained in w.chain[:w.chainDepth]; before descending
// into children, the chain is extended with selfMarker (if any children exist).
//
// Returns epOff: the offset into posArena where this element's positions block
// begins. The block layout is: chain[:savedChainDepth] ++ selfMarker ++ descs[selfDepth].
// The caller computes epLen = len(posArena) - epOff after walkObject returns.
//
// subtreeSelves (selfMarker ++ descs[selfDepth], the chain-free portion) are
// value-copied into the parent's accumulator (subtrees[selfDepth-1] or rootSelves)
// before any further posArena writes, so the sub-slice alias is safe.
func (w *walker) walkObject(selfDepth int, prefix string, ls LevelSchema, obj map[string]any,
) (epOff uint32, err error) {
	// Phase 0: allocate this element's marker BEFORE walking descendants so DFS
	// pre-order puts the parent marker ahead of its children in elemIdx space.
	// This guarantees the predecessor scan in LiftToAncestor finds the correct parent.
	selfMarker, err := w.nextElem()
	if err != nil {
		return 0, err
	}
	w.result.Anchors = append(w.result.Anchors, AnchorEntry{
		Path:     prefix,
		Position: selfMarker,
	})

	// savedChainDepth is the depth of the ancestor chain visible TO this element
	// (not including selfMarker). Phase 2 uses it to write the chain prefix and
	// to compute the subtreeSelves sub-slice offset.
	savedChainDepth := w.chainDepth
	// Reset the descendant accumulator for this depth. Siblings at the same depth
	// are processed sequentially, so the reset prevents prior siblings' descendants
	// from appearing in this element's posArena block.
	w.descs[selfDepth] = w.descs[selfDepth][:0]

	// Phase 1: process nested objects/arrays and scalar arrays depth-first.
	// Extend the shared chain with selfMarker so children see [ancestors..., selfMarker].
	// The defer restores chainDepth on all return paths so subsequent siblings
	// at this depth read the correct chain prefix.
	if len(ls.arrayOrNested) > 0 {
		w.chain = append(w.chain[:w.chainDepth], selfMarker)
		w.chainDepth++
		defer func() { w.chainDepth = savedChainDepth }()
	}
	for _, ps := range ls.arrayOrNested {
		val, exists := obj[ps.name]
		if !exists || val == nil {
			continue
		}
		if ps.kind == propKindNested {
			if err = w.walkNestedArray(selfDepth, ps.path, ps.nestedDT, val, ps.children); err != nil {
				return 0, err
			}
			// subtrees[selfDepth] was filled by the children's walkObject calls.
			// Append now before the next sibling's walkNestedArray resets it.
			w.descs[selfDepth] = append(w.descs[selfDepth], w.subtrees[selfDepth]...)
		} else {
			if err = w.walkScalarArray(val, ps); err != nil {
				return 0, err
			}
			// sels was filled by walkScalarArray. Append now before the next
			// scalar-array sibling's call resets it.
			w.descs[selfDepth] = append(w.descs[selfDepth], w.sels...)
		}
	}

	// Phase 2: write elementPositions to posArena.
	// Layout: chain[:savedChainDepth] ++ selfMarker ++ descs[selfDepth]
	epOff = uint32(len(w.result.posArena))
	w.result.posArena = append(w.result.posArena, w.chain[:savedChainDepth]...)
	w.result.posArena = append(w.result.posArena, selfMarker)
	w.result.posArena = append(w.result.posArena, w.descs[selfDepth]...)

	// Phase 3: scalar properties share the posArena block written in Phase 2.
	// epRange covers exactly chain[:savedChainDepth]+selfMarker+descs[selfDepth]
	// without a separate allocation per scalar property.
	epRange := PosRange{epOff, uint32(len(w.result.posArena)) - epOff}
	for _, ps := range ls.scalars {
		val, exists := obj[ps.name]
		if !exists || val == nil {
			continue
		}
		w.result.Values = append(w.result.Values, ValueEntry{
			Path:         ps.path,
			PropName:     ps.name,
			Value:        val,
			DataType:     ps.dt,
			Tokenization: ps.tokenization,
			TextAnalyzer: ps.textAnalyzer,
			Pos:          epRange,
		})
		w.result.Exists = append(w.result.Exists, ExistsEntry{
			Path: ps.path,
			Pos:  epRange,
		})
	}

	// Push this element's subtreeSelves (selfMarker + descs[selfDepth], no chain prefix)
	// to the parent's subtree accumulator. subtreeSelves is a sub-slice of posArena;
	// the values are copied by the append before any further posArena writes, so the
	// alias is safe.
	subtreeSelves := w.result.posArena[epOff+uint32(savedChainDepth):]
	if selfDepth == 0 {
		w.rootSelves = append(w.rootSelves, subtreeSelves...)
	} else {
		w.subtrees[selfDepth-1] = append(w.subtrees[selfDepth-1], subtreeSelves...)
	}

	return epOff, nil
}

// walkNestedArray processes a nested object or object[] property.
// For object type, the value is wrapped in a 1-element array.
//
// parentDepth is the selfDepth of the enclosing walkObject. This function
// uses subtrees[parentDepth] as the element accumulator.
//
// The chain visible to each array element is w.chain[:w.chainDepth] — already
// extended by the enclosing walkObject with its own selfMarker before calling here.
// walkObject restores w.chainDepth on return, keeping the chain stable between
// elements.
func (w *walker) walkNestedArray(parentDepth int, path string, dt schema.DataType,
	val any, ls LevelSchema,
) error {
	var elements []any
	switch dt {
	case schema.DataTypeObject:
		elements = []any{val}
	case schema.DataTypeObjectArray:
		arr, ok := val.([]any)
		if !ok {
			return fmt.Errorf("expected []any for %s, got %T", path, val)
		}
		elements = arr
	default:
		// Unreachable: walkNestedArray is only called when IsNested(dt) is true,
		// which only holds for DataTypeObject and DataTypeObjectArray.
		return fmt.Errorf("unexpected data type %q at path %q", dt, path)
	}

	// Reset the subtree accumulator for parentDepth. Each element's walkObject
	// call pushes its subtreeSelves (self + descendants) here.
	w.subtrees[parentDepth] = w.subtrees[parentDepth][:0]
	for i, elem := range elements {
		elemMap, ok := elem.(map[string]any)
		if !ok {
			return fmt.Errorf("expected map for %s[%d], got %T", path, i, elem)
		}
		epOff, err := w.walkObject(parentDepth+1, path, ls, elemMap)
		if err != nil {
			return err
		}
		epLen := uint32(len(w.result.posArena)) - epOff
		w.result.Idx = append(w.result.Idx, IdxEntry{
			Path:  path,
			Index: i,
			Pos:   PosRange{epOff, epLen},
		})
	}

	if len(w.subtrees[parentDepth]) > 0 {
		// ExistsEntry: chain at call time (includes parent selfMarker) +
		// union of all element subtreeSelves from subtrees[parentDepth].
		exOff := uint32(len(w.result.posArena))
		w.result.posArena = append(w.result.posArena, w.chain[:w.chainDepth]...)
		w.result.posArena = append(w.result.posArena, w.subtrees[parentDepth]...)
		exLen := uint32(len(w.result.posArena)) - exOff
		w.result.Exists = append(w.result.Exists, ExistsEntry{
			Path: path,
			Pos:  PosRange{exOff, exLen},
		})
	}
	return nil
}

// walkScalarArray processes a scalar array property (text[], int[], etc.).
// Each element gets its own leaf position.
//
// ps is the precomputed schema descriptor for this property. The chain visible
// to each scalar element is w.chain[:chainLen], where chainLen is w.chainDepth
// at call time (the enclosing walkObject has already extended the chain with
// its selfMarker). Value and Idx entries carry the full per-element posArena
// block (chain + self); Anchor is the exact self marker; ExistsEntry collects
// chain plus every element's self.
//
// After this call, w.sels contains every element's self marker (no chain) for
// the caller to append into its descs accumulator.
func (w *walker) walkScalarArray(val any, ps propSchema) error {
	scalarDT := schema.ScalarFromArrayType(ps.dt)
	// chainLen is the chain depth at call time; stable for the entire function
	// since walkScalarArray is a leaf (no recursive walker calls).
	chainLen := w.chainDepth
	w.sels = w.sels[:0]

	// appendElem records a single array element directly — elem is assignable
	// to ValueEntry.Value (type any) without an intermediate []any copy.
	appendElem := func(i int, elem any) error {
		pos, err := w.nextElem()
		if err != nil {
			return fmt.Errorf("scalar array %s[%d]: %w", ps.path, i, err)
		}
		// Per-element posArena block: chain + self.
		feOff := uint32(len(w.result.posArena))
		w.result.posArena = append(w.result.posArena, w.chain[:chainLen]...)
		w.result.posArena = append(w.result.posArena, pos)
		feLen := uint32(len(w.result.posArena)) - feOff
		feRange := PosRange{feOff, feLen}
		w.result.Values = append(w.result.Values, ValueEntry{
			Path:         ps.path,
			PropName:     ps.name,
			Value:        elem,
			DataType:     scalarDT,
			Tokenization: ps.tokenization,
			TextAnalyzer: ps.textAnalyzer,
			Pos:          feRange,
		})
		w.result.Idx = append(w.result.Idx, IdxEntry{
			Path:  ps.path,
			Index: i,
			Pos:   feRange,
		})
		w.result.Anchors = append(w.result.Anchors, AnchorEntry{
			Path:     ps.path,
			Position: pos,
		})
		w.sels = append(w.sels, pos)
		return nil
	}

	// Iterate directly over the concrete slice type. Values arrive in different
	// forms depending on the write path:
	//   - API/JSON path: enrichSchemaTypes converts []interface{} to []string,
	//     []float64, or []bool based on the element type.
	//   - Internal paths (e.g. direct programmatic insertion without a JSON
	//     round-trip) may produce []int, []time.Time, or []uuid.UUID.
	// All cases are handled defensively so that walkScalarArray remains correct
	// regardless of how the caller obtained the value.
	switch v := val.(type) {
	case []any:
		for i, elem := range v {
			if err := appendElem(i, elem); err != nil {
				return err
			}
		}
	case []string:
		for i, elem := range v {
			if err := appendElem(i, elem); err != nil {
				return err
			}
		}
	case []float64:
		for i, elem := range v {
			if err := appendElem(i, elem); err != nil {
				return err
			}
		}
	case []bool:
		for i, elem := range v {
			if err := appendElem(i, elem); err != nil {
				return err
			}
		}
	case []int:
		for i, elem := range v {
			if err := appendElem(i, elem); err != nil {
				return err
			}
		}
	case []time.Time:
		for i, elem := range v {
			if err := appendElem(i, elem); err != nil {
				return err
			}
		}
	case []uuid.UUID:
		for i, elem := range v {
			if err := appendElem(i, elem); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("expected []any for %s, got %T", ps.path, val)
	}

	if len(w.sels) == 0 {
		return nil
	}

	// ExistsEntry for the scalar-array path covers the chain plus every
	// element's self marker — the union of all per-element positions.
	exOff := uint32(len(w.result.posArena))
	w.result.posArena = append(w.result.posArena, w.chain[:chainLen]...)
	w.result.posArena = append(w.result.posArena, w.sels...)
	exLen := uint32(len(w.result.posArena)) - exOff
	w.result.Exists = append(w.result.Exists, ExistsEntry{
		Path: ps.path,
		Pos:  PosRange{exOff, exLen},
	})
	return nil
}

// joinPath concatenates prefix + separator + name without the
// []string + strings.Join allocation that filnested.JoinPath incurs.
// Called once per NestedProperty per object during nested write
// analysis, so the per-call alloc adds up on heavy ingest.
func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + filnested.PathSep + name
}
