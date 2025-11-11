//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package filters

import (
	"encoding/json"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

type Operator int

const (
	OperatorEqual Operator = iota + 1
	OperatorNotEqual
	OperatorGreaterThan
	OperatorGreaterThanEqual
	OperatorLessThan
	OperatorLessThanEqual
	OperatorAnd
	OperatorOr
	OperatorWithinGeoRange
	OperatorLike
	OperatorIsNull
	ContainsAny
	ContainsAll
	ContainsNone
	OperatorNot
)

func (o Operator) OnValue() bool {
	switch o {
	case OperatorEqual,
		OperatorNotEqual,
		OperatorGreaterThan,
		OperatorGreaterThanEqual,
		OperatorLessThan,
		OperatorLessThanEqual,
		OperatorWithinGeoRange,
		OperatorLike,
		OperatorIsNull,
		ContainsAny,
		ContainsAll,
		ContainsNone:
		return true

	case OperatorOr, OperatorAnd, OperatorNot:
		return false

	default:
		return false
	}
}

func (o Operator) Name() string {
	switch o {
	case OperatorEqual:
		return "Equal"
	case OperatorNotEqual:
		return "NotEqual"
	case OperatorGreaterThan:
		return "GreaterThan"
	case OperatorGreaterThanEqual:
		return "GreaterThanEqual"
	case OperatorLessThan:
		return "LessThan"
	case OperatorLessThanEqual:
		return "LessThanEqual"
	case OperatorAnd:
		return "And"
	case OperatorOr:
		return "Or"
	case OperatorWithinGeoRange:
		return "WithinGeoRange"
	case OperatorLike:
		return "Like"
	case OperatorIsNull:
		return "IsNull"
	case ContainsAny:
		return "ContainsAny"
	case ContainsAll:
		return "ContainsAll"
	case ContainsNone:
		return "ContainsNone"
	case OperatorNot:
		return "Not"
	default:
		panic("Unknown operator")
	}
}

func (o Operator) IsContains() bool {
	switch o {
	case ContainsAny, ContainsAll, ContainsNone:
		return true
	default:
		return false
	}
}

type LocalFilter struct {
	Root *Clause `json:"root"`
}

type Value struct {
	Value interface{}     `json:"value"`
	Type  schema.DataType `json:"type"`
}

func (v *Value) UnmarshalJSON(data []byte) error {
	type Alias Value
	aux := struct {
		*Alias
	}{
		Alias: (*Alias)(v),
	}

	err := json.Unmarshal(data, &aux)
	if err != nil {
		return err
	}

	asFloat, ok := v.Value.(float64)
	if v.Type == schema.DataTypeInt && ok {
		v.Value = int(asFloat)
	}

	if v.Type == schema.DataTypeGeoCoordinates {
		temp := struct {
			Value GeoRange `json:"value"`
		}{}

		if err := json.Unmarshal(data, &temp); err != nil {
			return err
		}
		v.Value = temp.Value
	}

	return nil
}

type Clause struct {
	Operator Operator `json:"operator"`
	On       *Path    `json:"on"`
	Value    *Value   `json:"value"`
	Operands []Clause `json:"operands"`
}

// GeoRange to be used with fields of type GeoCoordinates. Identifies a point
// and a maximum distance from that point.
type GeoRange struct {
	*models.GeoCoordinates
	Distance float32 `json:"distance"`
}
