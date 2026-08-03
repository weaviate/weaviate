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

package aggregation

type Result struct {
	Groups []Group `json:"groups"`
}

type Group struct {
	Properties map[string]Property `json:"properties"`
	GroupedBy  *GroupedBy          `json:"groupedBy"` // optional to support ungrouped aggregations (formerly meta)
	Count      int                 `json:"count"`
}

type Property struct {
	Type                  PropertyType           `json:"type"`
	NumericalAggregations map[string]interface{} `json:"numericalAggregations"`
	TextAggregation       Text                   `json:"textAggregation"`
	BooleanAggregation    Boolean                `json:"booleanAggregation"`
	SchemaType            string                 `json:"schemaType"`
	ReferenceAggregation  Reference              `json:"referenceAggregation"`
	DateAggregations      map[string]interface{} `json:"dateAggregation"`
	// ApproximateCardinality is a bloom-filter estimate of the property's
	// distinct inverted-index keys; keys of deleted or updated values are
	// retained until compaction, so it can exceed the number of distinct
	// values currently live. nil unless requested.
	ApproximateCardinality *uint32 `json:"approximateCardinality,omitempty"`
}

type Text struct {
	Items []TextOccurrence `json:"items"`
	Count int              `json:"count"`
	// CutoffExceeded reports that the property holds more distinct values
	// than the requested top-occurrences cutoff allowed; Items is then empty.
	CutoffExceeded bool `json:"cutoffExceeded,omitempty"`
	// ValuesComplete reports that Items holds every value of the property in
	// this shard rather than the top ones, which is what lets a shard combiner
	// count the collection's distinct values.
	ValuesComplete bool `json:"valuesComplete,omitempty"`
}

type PropertyType string

const (
	PropertyTypeNumerical PropertyType = "numerical"
	PropertyTypeBoolean   PropertyType = "boolean"
	PropertyTypeText      PropertyType = "text"
	PropertyTypeDate      PropertyType = "date"
	PropertyTypeReference PropertyType = "cref"
)

type GroupedBy struct {
	Value interface{} `json:"value"`
	Path  []string    `json:"path"`
}

type TextOccurrence struct {
	Value  string `json:"value"`
	Occurs int    `json:"occurs"`
}

type Boolean struct {
	Count           int     `json:"count"`
	TotalTrue       int     `json:"totalTrue"`
	TotalFalse      int     `json:"totalFalse"`
	PercentageTrue  float64 `json:"percentageTrue"`
	PercentageFalse float64 `json:"percentageFalse"`
}

type Reference struct {
	PointingTo []string `json:"pointingTo"`
}
