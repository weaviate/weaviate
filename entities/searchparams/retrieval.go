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

package searchparams

import (
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/entities/search"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

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
	for _, prop := range c.Properties {
		// Check if the name of the property is the given name, that's the property we need
		if prop.Name == strings.Split(propName, ".")[0] {
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
