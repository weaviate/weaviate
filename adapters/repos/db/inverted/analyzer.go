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
	"bytes"
	"encoding/binary"
	"iter"
	"slices"
	"sync"

	"github.com/google/uuid"
	nested2 "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested2"
	ent "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/tokenizer"
)

type IsFallbackToSearchable func() bool

type Countable struct {
	Data          []byte
	TermFrequency float32
}

type Property struct {
	Name               string
	Items              []Countable
	RawValues          []string // Original text values before tokenization (text/text[] only)
	Length             int
	HasFilterableIndex bool // roaring set index
	HasSearchableIndex bool // map index (with frequencies)
	HasRangeableIndex  bool // roaring set index for ranged queries
}

type NilProperty struct {
	Name                string
	AddToPropertyLength bool
}

// NestedValue is a single analyzed value from a nested property.
// The Has*Index flags indicate which buckets this value should be written to.
type NestedValue struct {
	Path               string   // dot-notation path, e.g. "addresses.city"
	Data               []byte   // analyzed value bytes
	Positions          []uint64 // positions with docID=0
	HasFilterableIndex bool
	HasSearchableIndex bool
	HasRangeableIndex  bool
}

// NestedProperty holds all analyzed values and metadata from position
// assignment of a single nested property.
type NestedProperty struct {
	Name               string        // top-level property name (for bucket naming)
	Values             []NestedValue // analyzed values for the value bucket
	Idx                []NestedMeta  // _idx metadata entries
	Exists             []NestedMeta  // _exists metadata entries
	Anchors            []NestedMeta  // _anchor metadata entries (per-element self markers)
	HasFilterableIndex bool          // any value needs the filterable bucket
	HasSearchableIndex bool          // any value needs the searchable bucket
	HasRangeableIndex  bool          // any value needs the rangeable bucket
}

// NestedMeta is an _idx, _exists, or _anchor metadata entry from position
// assignment.
type NestedMeta struct {
	Path      string   // e.g. "addresses" for _idx, "owner.firstname" for _exists
	Index     int      // 0-based element index (only used for _idx; -1 for _exists / _anchor)
	Positions []uint64 // positions with docID=0
}

// NestedValue2 is one analyzed term from a nested value leaf in the v2
// pipeline. Pos refers into the owning NestedProperty2's posArena (via
// NestedProperty2.Positions); the arena must outlive the bucket-write that
// expands it via nested2.PositionsWithDocID.
type NestedValue2 struct {
	Path               string
	Data               []byte
	Pos                nested2.PosRange
	HasFilterableIndex bool
	HasSearchableIndex bool
	HasRangeableIndex  bool
}

// NestedProperty2 is the thin output of analyzeNestedProp2. It carries a
// reference to the full AssignResult (arena + raw Idx/Exists/Anchors) and the
// tokenized+gated values slice. Idx/Exists/Anchor entries are NOT copied into
// this struct: Idx is iterated via Idx() (ungated); Exists and Anchors are
// iterated via gated methods. No []uint64 positions are held anywhere.
//
// Lifetime: result (and therefore posArena) must remain live until after both
// nestedFilterableEntries2 and nestedMetaEntries2 complete. Once those calls
// return, every RoaringSetBatchEntry.Values is a fresh []uint64 that no longer
// references the arena.
type NestedProperty2 struct {
	Name               string
	values             []NestedValue2
	result             *nested2.AssignResult
	configs            map[string]nestedIndexConfig // drives Exists/Anchors gating
	numFilterable      int
	HasFilterableIndex bool
	HasSearchableIndex bool
	HasRangeableIndex  bool
}

// ValueView is yielded by NestedProperty2.Values(). It carries the filterable/
// searchable/rangeable flags and the pre-resolved arena subslice.
type ValueView struct {
	Path               string
	Data               []byte
	HasFilterableIndex bool
	HasSearchableIndex bool
	HasRangeableIndex  bool
	// Positions is the pre-resolved arena subslice for this value entry. It
	// aliases posArena and must not be mutated by the consumer.
	Positions []nested2.ElemIdx
}

// IdxView is yielded by NestedProperty2.Idx().
type IdxView struct {
	Path  string
	Index int
	// Positions is the pre-resolved arena subslice for this Idx entry. It
	// aliases posArena and must not be mutated by the consumer.
	Positions []nested2.ElemIdx
}

// ExistsView is yielded by NestedProperty2.Exists().
type ExistsView struct {
	Path string
	// Positions is the pre-resolved arena subslice for this Exists entry. It
	// aliases posArena and must not be mutated by the consumer.
	Positions []nested2.ElemIdx
}

// AnchorView is yielded by NestedProperty2.Anchors(). It carries only a scalar
// ElemIdx: anchor entries need a single Encode(Position, docID) call at write
// time, not a full position-set materialisation. Exposing Positions() here would
// hide this direct-encode fast path behind needless arena overhead.
type AnchorView struct {
	Path     string
	Position nested2.ElemIdx
}

// Values returns an iterator over the tokenized+gated value entries. No
// additional gating is applied here; values are already gated at analyze time
// (entries for disabled leaves are not included in np.values).
func (np *NestedProperty2) Values() iter.Seq[ValueView] {
	return func(yield func(ValueView) bool) {
		for _, v := range np.values {
			if !yield(ValueView{
				Path:               v.Path,
				Data:               v.Data,
				HasFilterableIndex: v.HasFilterableIndex,
				HasSearchableIndex: v.HasSearchableIndex,
				HasRangeableIndex:  v.HasRangeableIndex,
				Positions:          np.result.Positions(v.Pos),
			}) {
				return
			}
		}
	}
}

// Idx returns an iterator over the raw (ungated) IdxEntry values from the
// AssignResult. Idx is never gated: every entry is written to the meta bucket
// regardless of per-leaf index flags.
func (np *NestedProperty2) Idx() iter.Seq[IdxView] {
	return func(yield func(IdxView) bool) {
		for _, idx := range np.result.Idx {
			if !yield(IdxView{Path: idx.Path, Index: idx.Index, Positions: np.result.Positions(idx.Pos)}) {
				return
			}
		}
	}
}

// NumIdx returns the exact count of Idx entries. Used to size the _idx key slab
// in nestedMetaEntries2 (must be exactly NumIdx()*IdxKeySize bytes).
func (np *NestedProperty2) NumIdx() int { return len(np.result.Idx) }

// NumFilterable returns the exact count of filterable value entries. Precomputed
// during analyze so nestedFilterableEntries2 can pre-allocate the result slice
// without a scan.
func (np *NestedProperty2) NumFilterable() int { return np.numFilterable }

// NumMetaHint returns an over-bound estimate of the total meta entry count
// (Idx + Exists + Anchors, all ungated). Used as an initial capacity hint for
// nestedMetaEntries2; the actual written count may be lower after Exists/Anchors
// gating, but never higher.
func (np *NestedProperty2) NumMetaHint() int {
	return len(np.result.Idx) + len(np.result.Exists) + len(np.result.Anchors)
}

// Exists returns an iterator over AssignResult.Exists entries that pass the
// per-leaf gating rule. Leaf paths absent from any inverted index (per configs)
// are skipped. The root sentinel (Path="") and intermediate array paths not in
// configs are always yielded — they back IS NULL on the array property itself,
// independent of leaf-level index flags.
func (np *NestedProperty2) Exists() iter.Seq[ExistsView] {
	return func(yield func(ExistsView) bool) {
		for _, e := range np.result.Exists {
			if e.Path != "" {
				if cfg, isLeaf := np.configs[e.Path]; isLeaf && !cfg.hasAny() {
					continue
				}
			}
			if !yield(ExistsView{Path: e.Path, Positions: np.result.Positions(e.Pos)}) {
				return
			}
		}
	}
}

// Anchors returns an iterator over AssignResult.Anchors entries that pass the
// same per-leaf gating rule as Exists.
func (np *NestedProperty2) Anchors() iter.Seq[AnchorView] {
	return func(yield func(AnchorView) bool) {
		for _, a := range np.result.Anchors {
			if a.Path != "" {
				if cfg, isLeaf := np.configs[a.Path]; isLeaf && !cfg.hasAny() {
					continue
				}
			}
			if !yield(AnchorView{Path: a.Path, Position: a.Position}) {
				return
			}
		}
	}
}

// HasFilterableEntries reports whether at least one value in np.values has
// HasFilterableIndex == true. This is a per-document predicate: a non-nil np
// can have meta entries but no filterable values (e.g., all leaves are
// non-filterable). Precomputed in analyzeNestedProp2 — no scan at call time.
func (np *NestedProperty2) HasFilterableEntries() bool { return np.numFilterable > 0 }

// HasMetaEntries reports whether there is anything to write to the meta
// bucket. Uses ungated result slice lengths — the root sentinel (Path="")
// bypasses the leaf-drop rule, so ungated-empty implies gated-empty.
func (np *NestedProperty2) HasMetaEntries() bool {
	return len(np.result.Idx) > 0 || len(np.result.Exists) > 0 || len(np.result.Anchors) > 0
}

// HasSearchableEntries always returns false. The v2 path writes only
// filterable and meta buckets; searchable bucket support is a future addition.
// This method exists so the write-path predicate surface is stable: adding
// searchable support changes only this method body, not any call sites.
func (np *NestedProperty2) HasSearchableEntries() bool { return false }

// HasRangeableEntries always returns false. Same rationale as
// HasSearchableEntries.
func (np *NestedProperty2) HasRangeableEntries() bool { return false }

func DedupItems(props []Property) []Property {
	for i := range props {
		seen := map[string]struct{}{}
		items := props[i].Items

		var key string
		// reverse order to keep latest elements
		for j := len(items) - 1; j >= 0; j-- {
			key = string(items[j].Data)
			if _, ok := seen[key]; ok {
				// remove element already seen
				items = append(items[:j], items[j+1:]...)
			}
			seen[key] = struct{}{}
		}
		props[i].Items = items
	}
	return props
}

// PropertyOverlay describes inverted-index flags and (optionally) a
// tokenization to apply to a single property *for the duration of a single
// analyzer call*. It is used by runtime reindex migrations that build a new
// inverted bucket before the corresponding schema flag has been flipped via
// RAFT (see EnableFilterableStrategy / EnableSearchableStrategy).
//
// The overlay is read by Analyzer.analyzeProps which, when an entry exists
// for the property name, treats the property as if its IndexFilterable /
// IndexSearchable / IndexRangeFilters flags were already true (and uses
// Tokenization in place of the stored value, when non-empty). The live
// schema is never mutated.
//
// Pointers are owned by the caller and MUST NOT be modified after handing
// them to the analyzer.
type PropertyOverlay struct {
	ForceFilterable bool
	ForceSearchable bool
	ForceRangeable  bool
	// Tokenization, when non-empty, overrides prop.Tokenization for the
	// duration of analysis. Used by EnableSearchableStrategy to tokenize
	// with the target tokenization before the RAFT update applies it.
	Tokenization string
}

type analyzerCacheEntry struct {
	fold     bool     // whether ASCIIFold was enabled
	ignore   []string // the ASCIIFoldIgnore list used to build prepared
	prepared *tokenizer.PreparedAnalyzer
}

type Analyzer struct {
	isFallbackToSearchable IsFallbackToSearchable
	className              string
	captureRawValues       bool
	// schemaOverlay maps property name → forced-flag overrides for runtime
	// reindex migrations. nil/empty means "use the live schema as-is".
	schemaOverlay map[string]PropertyOverlay

	// cache maps property name → cached PreparedAnalyzer.
	// Invalidated when the property's fold setting or ASCIIFoldIgnore list changes.
	cache sync.Map // map[string]analyzerCacheEntry
}

// preparedFor returns a cached PreparedAnalyzer for the given property,
// rebuilding it only when the fold config has changed.
func (a *Analyzer) preparedFor(propName string, cfg *models.TextAnalyzerConfig) *tokenizer.PreparedAnalyzer {
	fold := cfg != nil && cfg.ASCIIFold
	var currentIgnore []string
	if fold {
		currentIgnore = cfg.ASCIIFoldIgnore
	}

	if v, ok := a.cache.Load(propName); ok {
		entry := v.(analyzerCacheEntry)
		if entry.fold == fold && slices.Equal(entry.ignore, currentIgnore) {
			return entry.prepared
		}
	}

	prepared := tokenizer.NewPreparedAnalyzer(cfg)
	a.cache.Store(propName, analyzerCacheEntry{
		fold:     fold,
		ignore:   slices.Clone(currentIgnore),
		prepared: prepared,
	})
	return prepared
}

// Text tokenizes given input according to selected tokenization,
// then aggregates duplicates
func (a *Analyzer) Text(tokenization, in, propName string, TextAnalyzer *models.TextAnalyzerConfig) []Countable {
	return a.TextArray(tokenization, []string{in}, propName, TextAnalyzer)
}

// TextArray tokenizes given input according to selected tokenization,
// then aggregates duplicates
func (a *Analyzer) TextArray(tokenization string, inArr []string, propName string, TextAnalyzer *models.TextAnalyzerConfig) []Countable {
	prepared := a.preparedFor(propName, TextAnalyzer)
	counts := map[string]uint64{}
	for _, in := range inArr {
		// Analyze with nil stopwords: indexing stores all tokens including stopwords
		result := tokenizer.Analyze(in, tokenization, a.className, prepared, nil)
		for _, term := range result.Indexed {
			counts[term]++
		}
	}

	countable := make([]Countable, len(counts))
	i := 0
	for term, count := range counts {
		countable[i] = Countable{
			Data:          []byte(term),
			TermFrequency: float32(count),
		}
		i++
	}
	return countable
}

// Int requires no analysis, so it's actually just a simple conversion to a
// string-formatted byte slice of the int
func (a *Analyzer) Int(in int64) ([]Countable, error) {
	data, err := ent.LexicographicallySortableInt64(in)
	if err != nil {
		return nil, err
	}

	return []Countable{
		{
			Data: data,
		},
	}, nil
}

// UUID requires no analysis, so it's just dumping the raw binary representation
func (a *Analyzer) UUID(in uuid.UUID) ([]Countable, error) {
	return []Countable{
		{
			Data: in[:],
		},
	}, nil
}

// UUID array requires no analysis, so it's just dumping the raw binary
// representation of each contained element
func (a *Analyzer) UUIDArray(in []uuid.UUID) ([]Countable, error) {
	out := make([]Countable, len(in))
	for i := range in {
		out[i] = Countable{
			Data: in[i][:],
		}
	}

	return out, nil
}

// Int array requires no analysis, so it's actually just a simple conversion to a
// string-formatted byte slice of the int
func (a *Analyzer) IntArray(in []int64) ([]Countable, error) {
	out := make([]Countable, len(in))
	for i := range in {
		data, err := ent.LexicographicallySortableInt64(in[i])
		if err != nil {
			return nil, err
		}
		out[i] = Countable{Data: data}
	}

	return out, nil
}

// Float requires no analysis, so it's actually just a simple conversion to a
// lexicographically sortable byte slice.
func (a *Analyzer) Float(in float64) ([]Countable, error) {
	data, err := ent.LexicographicallySortableFloat64(in)
	if err != nil {
		return nil, err
	}

	return []Countable{
		{
			Data: data,
		},
	}, nil
}

// Float array requires no analysis, so it's actually just a simple conversion to a
// lexicographically sortable byte slice.
func (a *Analyzer) FloatArray(in []float64) ([]Countable, error) {
	out := make([]Countable, len(in))
	for i := range in {
		data, err := ent.LexicographicallySortableFloat64(in[i])
		if err != nil {
			return nil, err
		}
		out[i] = Countable{Data: data}
	}

	return out, nil
}

// BoolArray requires no analysis, so it's actually just a simple conversion to a
// little-endian ordered byte slice
func (a *Analyzer) BoolArray(in []bool) ([]Countable, error) {
	out := make([]Countable, len(in))
	for i := range in {
		b := bytes.NewBuffer(nil)
		err := binary.Write(b, binary.LittleEndian, &in[i])
		if err != nil {
			return nil, err
		}
		out[i] = Countable{Data: b.Bytes()}
	}

	return out, nil
}

// Bool requires no analysis, so it's actually just a simple conversion to a
// little-endian ordered byte slice
func (a *Analyzer) Bool(in bool) ([]Countable, error) {
	b := bytes.NewBuffer(nil)
	err := binary.Write(b, binary.LittleEndian, &in)
	if err != nil {
		return nil, err
	}

	return []Countable{
		{
			Data: b.Bytes(),
		},
	}, nil
}

// RefCount does not index the content of the refs, but only the count with 0
// being an explicitly allowed value as well.
func (a *Analyzer) RefCount(in models.MultipleRef) ([]Countable, error) {
	length := uint64(len(in))
	data, err := ent.LexicographicallySortableUint64(length)
	if err != nil {
		return nil, err
	}

	return []Countable{
		{
			Data: data,
		},
	}, nil
}

// Ref indexes references as beacon-strings
func (a *Analyzer) Ref(in models.MultipleRef) ([]Countable, error) {
	out := make([]Countable, len(in))

	for i, ref := range in {
		out[i] = Countable{
			Data: []byte(ref.Beacon),
		}
	}

	return out, nil
}

func NewAnalyzer(isFallbackToSearchable IsFallbackToSearchable, className string) *Analyzer {
	if isFallbackToSearchable == nil {
		isFallbackToSearchable = func() bool { return false }
	}
	return &Analyzer{isFallbackToSearchable: isFallbackToSearchable, className: className}
}

// NewAnalyzerWithRawValues creates an analyzer that captures raw (pre-tokenization)
// text values in Property.RawValues. This is only needed during migration reads
// where retokenize strategies require the original text. Normal ingestion should
// use NewAnalyzer to avoid the extra allocation on the hot path.
func NewAnalyzerWithRawValues(isFallbackToSearchable IsFallbackToSearchable, className string) *Analyzer {
	if isFallbackToSearchable == nil {
		isFallbackToSearchable = func() bool { return false }
	}
	return &Analyzer{isFallbackToSearchable: isFallbackToSearchable, className: className, captureRawValues: true}
}

// WithSchemaOverlay attaches an in-memory schema overlay used only during
// analysis. See PropertyOverlay for semantics. Returns the same analyzer to
// allow fluent chaining at the call-site. Pass nil/empty to clear.
//
// This is intended for runtime reindex backfill: the analyzer must treat
// the target property as if its inverted-index flag were already on, even
// though the RAFT-stored schema still has it off. Production ingest paths
// MUST NOT call this — they should always see the live schema unmodified.
func (a *Analyzer) WithSchemaOverlay(overlay map[string]PropertyOverlay) *Analyzer {
	a.schemaOverlay = overlay
	return a
}
