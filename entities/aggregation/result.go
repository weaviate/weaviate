//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package aggregation

type Result struct {
	Groups []Group
}

type Group struct {
	Properties map[string]Property
	GroupedBy  *GroupedBy // optional to support ungrouped aggregations (formerly meta)
	Count      int
}

type Property struct {
	Type                  PropertyType
	NumericalAggregations map[string]float64
	TextAggregation       Text
	BooleanAggregation    Boolean
	SchemaType            string
	ReferenceAggregation  Reference
}

type Text struct {
	Items []TextOccurrence
	Count int
}

type PropertyType string

const (
	PropertyTypeNumerical PropertyType = "numerical"
	PropertyTypeBoolean   PropertyType = "boolean"
	PropertyTypeText      PropertyType = "text"
	PropertyTypeReference PropertyType = "cref"
)

type GroupedBy struct {
	Value interface{}
	Path  []string
}

type TextOccurrence struct {
	Value  string
	Occurs int
}

type Boolean struct {
	Count           int
	TotalTrue       int
	TotalFalse      int
	PercentageTrue  float64
	PercentageFalse float64
}

type Reference struct {
	PointingTo []string
}
