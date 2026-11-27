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
	"strings"

	"github.com/weaviate/weaviate/entities/search"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type SelectionMMR struct {
	Limit   uint32  `json:"limit"`
	Balance float32 `json:"balance"`
}

type Selection struct {
	MMR *SelectionMMR `json:"mmr,omitempty"`
}

// Filter strategy names accepted per request, aliased from the hnsw config.
const (
	FilterStrategySweeping = hnsw.FilterStrategySweeping
	FilterStrategyAcorn    = hnsw.FilterStrategyAcorn
	FilterStrategyPathseer = hnsw.FilterStrategyPathseer
)

// VectorIndexParams overrides vector index search settings for one request; zero fields mean the collection default.
// It travels between nodes as JSON, so a new field's zero value must mean "default".
type VectorIndexParams struct {
	EF             int                          `json:"ef,omitempty"`
	SearchProbe    int                          `json:"searchProbe,omitempty"`
	FilterStrategy string                       `json:"filterStrategy,omitempty"`
	PerTarget      map[string]VectorIndexParams `json:"perTarget,omitempty"`
}

// EFOrZero is nil-safe.
func (p *VectorIndexParams) EFOrZero() int {
	if p == nil {
		return 0
	}
	return p.EF
}

// SearchProbeOrZero is nil-safe.
func (p *VectorIndexParams) SearchProbeOrZero() int {
	if p == nil {
		return 0
	}
	return p.SearchProbe
}

// FilterStrategyOrEmpty is nil-safe.
func (p *VectorIndexParams) FilterStrategyOrEmpty() string {
	if p == nil {
		return ""
	}
	return p.FilterStrategy
}

// IsZero reports whether p sets nothing, per-target entries included.
func (p *VectorIndexParams) IsZero() bool {
	return p == nil || (p.EF == 0 && p.SearchProbe == 0 && p.FilterStrategy == "" && len(p.PerTarget) == 0)
}

// FieldNames spells VectorIndexParams' fields the way a wire format does.
type FieldNames struct {
	EF, SearchProbe, FilterStrategy string
}

var (
	JSONFieldNames  = FieldNames{EF: "ef", SearchProbe: "searchProbe", FilterStrategy: "filterStrategy"}
	ProtoFieldNames = FieldNames{EF: "ef", SearchProbe: "search_probe", FilterStrategy: "filter_strategy"}
)

// Validate checks one entry (not PerTarget) against the caps and known strategies. A cap <= 0 means no cap; a nil p is valid.
func (p *VectorIndexParams) Validate(maxEF, maxProbe int, names FieldNames) error {
	if p == nil {
		return nil
	}
	switch {
	case p.EF < 0:
		return fmt.Errorf("%s must be >= 0, got %d", names.EF, p.EF)
	case maxEF > 0 && p.EF > maxEF:
		return fmt.Errorf("%s %d exceeds HNSW_MAX_SEARCH_EF (%d)", names.EF, p.EF, maxEF)
	case p.SearchProbe < 0:
		return fmt.Errorf("%s must be >= 0, got %d", names.SearchProbe, p.SearchProbe)
	case maxProbe > 0 && p.SearchProbe > maxProbe:
		return fmt.Errorf("%s %d exceeds HFRESH_MAX_SEARCH_PROBE (%d)", names.SearchProbe, p.SearchProbe, maxProbe)
	}
	switch p.FilterStrategy {
	case "", FilterStrategySweeping, FilterStrategyAcorn, FilterStrategyPathseer:
		return nil
	default:
		return fmt.Errorf("%s must be one of sweeping, acorn, pathseer, got %q", names.FilterStrategy, p.FilterStrategy)
	}
}

// ForTarget merges the entry for name over the request-wide values; a per-target zero never unsets a request-wide value.
// The result has no PerTarget.
func (p *VectorIndexParams) ForTarget(name string) *VectorIndexParams {
	if p == nil {
		return nil
	}
	entry, ok := p.PerTarget[name]
	if !ok {
		if len(p.PerTarget) == 0 {
			return p
		}
		return &VectorIndexParams{EF: p.EF, SearchProbe: p.SearchProbe, FilterStrategy: p.FilterStrategy}
	}
	merged := VectorIndexParams{EF: p.EF, SearchProbe: p.SearchProbe, FilterStrategy: p.FilterStrategy}
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
