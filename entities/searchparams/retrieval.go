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

package searchparams

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/entities/search"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

type SelectionMMR struct {
	Limit   uint32  `json:"limit"`
	Balance float32 `json:"balance"`
}

type Selection struct {
	MMR *SelectionMMR `json:"mmr,omitempty"`
}

// Per-request HNSW filter strategies. They mirror the collection-level
// vectorIndexConfig.filterStrategy values but are defined here so that this
// package stays free of the index packages (which import it).
const (
	FilterStrategySweeping = "sweeping"
	FilterStrategyAcorn    = "acorn"
	FilterStrategyPathseer = "pathseer"
)

// VectorSearchParams carries per-request overrides for the vector index search.
// It is set on a single search request and takes precedence over the
// collection-level configuration. Each field belongs to one index type and is
// ignored by the others: EF and FilterStrategy override vectorIndexConfig.ef
// and vectorIndexConfig.filterStrategy for HNSW, SearchProbe overrides
// vectorIndexConfig.searchProbe for HFresh. A zero field, or a nil
// *VectorSearchParams, means the collection default is used.
//
// PerTarget holds overrides for individual target vectors of a multi-target
// search, keyed by target vector name. It is only meaningful at request and
// shard level: the shard resolves it with ForTarget before calling an index,
// so an index never sees a non-empty PerTarget.
//
// The struct crosses nodes as JSON, so new fields must keep the zero value
// meaning "default", and nodes that predate a field drop it on decode.
type VectorSearchParams struct {
	EF             int                           `json:"ef,omitempty"`
	SearchProbe    int                           `json:"searchProbe,omitempty"`
	FilterStrategy string                        `json:"filterStrategy,omitempty"`
	PerTarget      map[string]VectorSearchParams `json:"perTarget,omitempty"`
}

// EFOrZero returns the per-request ef, or 0 when unset or when p is nil.
func (p *VectorSearchParams) EFOrZero() int {
	if p == nil {
		return 0
	}
	return p.EF
}

// SearchProbeOrZero returns the per-request HFresh search probe, or 0 when
// unset or when p is nil.
func (p *VectorSearchParams) SearchProbeOrZero() int {
	if p == nil {
		return 0
	}
	return p.SearchProbe
}

// FilterStrategyOrEmpty returns the per-request HNSW filter strategy, or ""
// when unset or when p is nil.
func (p *VectorSearchParams) FilterStrategyOrEmpty() string {
	if p == nil {
		return ""
	}
	return p.FilterStrategy
}

// IsZero reports whether p carries no override at all, request-wide or
// per-target. A nil p is zero.
func (p *VectorSearchParams) IsZero() bool {
	return p == nil || (p.EF == 0 && p.SearchProbe == 0 && p.FilterStrategy == "" && len(p.PerTarget) == 0)
}

// Validate checks p against the HNSW_MAX_SEARCH_EF and
// HFRESH_MAX_SEARCH_PROBE caps (a cap <= 0 is unbounded) and the known filter
// strategies, including every per-target entry. A nil p is valid.
func (p *VectorSearchParams) Validate(maxEF, maxProbe int) error {
	if p == nil {
		return nil
	}
	if err := p.validateEntry(maxEF, maxProbe); err != nil {
		return err
	}
	for _, name := range slices.Sorted(maps.Keys(p.PerTarget)) {
		entry := p.PerTarget[name]
		if err := entry.validateEntry(maxEF, maxProbe); err != nil {
			return fmt.Errorf("perTarget[%s]: %w", name, err)
		}
	}
	return nil
}

func (p *VectorSearchParams) validateEntry(maxEF, maxProbe int) error {
	switch {
	case p.EF < 0:
		return fmt.Errorf("ef must be >= 0, got %d", p.EF)
	case maxEF > 0 && p.EF > maxEF:
		return fmt.Errorf("ef %d exceeds HNSW_MAX_SEARCH_EF (%d)", p.EF, maxEF)
	case p.SearchProbe < 0:
		return fmt.Errorf("searchProbe must be >= 0, got %d", p.SearchProbe)
	case maxProbe > 0 && p.SearchProbe > maxProbe:
		return fmt.Errorf("searchProbe %d exceeds HFRESH_MAX_SEARCH_PROBE (%d)", p.SearchProbe, maxProbe)
	}
	switch p.FilterStrategy {
	case "", FilterStrategySweeping, FilterStrategyAcorn, FilterStrategyPathseer:
		return nil
	default:
		return fmt.Errorf("filterStrategy must be one of sweeping, acorn, pathseer, got %q", p.FilterStrategy)
	}
}

// ForTarget returns the parameters for target vector name: its per-target
// entry merged over the request-wide values, field by field. It returns p
// itself when p has no per-target entries, a copy without PerTarget otherwise,
// and nil for a nil p. An entry cannot unset a request-wide value.
func (p *VectorSearchParams) ForTarget(name string) *VectorSearchParams {
	if p == nil {
		return nil
	}
	entry, ok := p.PerTarget[name]
	if !ok {
		if len(p.PerTarget) == 0 {
			return p
		}
		return &VectorSearchParams{EF: p.EF, SearchProbe: p.SearchProbe, FilterStrategy: p.FilterStrategy}
	}
	merged := VectorSearchParams{EF: p.EF, SearchProbe: p.SearchProbe, FilterStrategy: p.FilterStrategy}
	if entry.EF > 0 {
		merged.EF = entry.EF
	}
	if entry.SearchProbe > 0 {
		merged.SearchProbe = entry.SearchProbe
	}
	if entry.FilterStrategy != "" {
		merged.FilterStrategy = entry.FilterStrategy
	}
	return &merged
}

type NearVector struct {
	Certainty     float64         `json:"certainty"`
	Distance      float64         `json:"distance"`
	WithDistance  bool            `json:"-"`
	Vectors       []models.Vector `json:"vectors"`
	TargetVectors []string        `json:"targetVectors"`
}

type KeywordRanking struct {
	Type                   string   `json:"type"`
	Properties             []string `json:"properties"`
	Query                  string   `json:"query"`
	AdditionalExplanations bool     `json:"additionalExplanations"`
	MinimumOrTokensMatch   int      `json:"minimumOrTokensMatch"`
	SearchOperator         string   `json:"searchOperator"`
}

// Indicates whether property should be indexed
// Index holds document ids with property of/containing particular value
// and number of its occurrences in that property
// (index created using bucket of StrategyMapCollection)
func HasSearchableIndex(prop *models.Property) bool {
	switch dt, _ := schema.AsPrimitive(prop.DataType); dt {
	case schema.DataTypeText, schema.DataTypeTextArray:
		// by default property has searchable index only for text/text[] props
		if prop.IndexSearchable == nil {
			return true
		}
		return *prop.IndexSearchable
	default:
		return false
	}
}

func PropertyHasSearchableIndex(class *models.Class, tentativePropertyName string) bool {
	if class == nil {
		return false
	}

	propertyName := strings.Split(tentativePropertyName, "^")[0]
	p, err := schema.GetPropertyByName(class, propertyName)
	if err != nil {
		return false
	}
	return HasSearchableIndex(p)
}

// GetPropertyByName returns the class by its name
func GetPropertyByName(c *models.Class, propName string) (*models.Property, error) {
	// a nested property is addressed by its first dot-notation segment
	root, _, _ := strings.Cut(propName, ".")
	for _, prop := range c.Properties {
		if prop.Name == root {
			return prop, nil
		}
	}
	return nil, fmt.Errorf("property %v not found %v", propName, c.Class)
}

func (k *KeywordRanking) ChooseSearchableProperties(class *models.Class) {
	var validProperties []string
	for _, prop := range k.Properties {
		property, err := GetPropertyByName(class, prop)
		if err != nil {
			continue
		}
		if HasSearchableIndex(property) {
			validProperties = append(validProperties, prop)
		}
	}
	k.Properties = validProperties
}

type WeightedSearchResult struct {
	SearchParams interface{} `json:"searchParams"`
	Weight       float64     `json:"weight"`
	Type         string      `json:"type"`
}

type HybridSearch struct {
	SubSearches          interface{}   `json:"subSearches"`
	Type                 string        `json:"type"`
	Alpha                float64       `json:"alpha"`
	Query                string        `json:"query"`
	Vector               models.Vector `json:"vector"`
	Properties           []string      `json:"properties"`
	TargetVectors        []string      `json:"targetVectors"`
	FusionAlgorithm      int           `json:"fusionalgorithm"`
	Distance             float32       `json:"distance"`
	WithDistance         bool          `json:"withDistance"`
	MinimumOrTokensMatch int           `json:"minimumOrTokenMatch"`
	SearchOperator       string        `json:"searchOperator"`
	NearTextParams       *NearTextParams
	NearVectorParams     *NearVector
}

type NearObject struct {
	ID            string   `json:"id"`
	Beacon        string   `json:"beacon"`
	Certainty     float64  `json:"certainty"`
	Distance      float64  `json:"distance"`
	WithDistance  bool     `json:"-"`
	TargetVectors []string `json:"targetVectors"`
}

type ObjectMove struct {
	ID     string
	Beacon string
}

// ExploreMove moves an existing Search Vector closer (or further away from) a specific other search term
type ExploreMove struct {
	Values  []string
	Force   float32
	Objects []ObjectMove
}

type NearTextParams struct {
	Values        []string
	Limit         int
	MoveTo        ExploreMove
	MoveAwayFrom  ExploreMove
	Certainty     float64
	Distance      float64
	WithDistance  bool
	Network       bool
	Autocorrect   bool
	TargetVectors []string
}

type GroupBy struct {
	Property        string
	Groups          int
	ObjectsPerGroup int
	Properties      search.SelectProperties
}
