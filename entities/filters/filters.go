//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package filters

import (
	"encoding/json"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
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
	OperatorNot
	OperatorWithinGeoRange
	OperatorLike
	OperatorIsNull
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
		OperatorIsNull:
		return true
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
	case OperatorNot:
		return "Not"
	case OperatorWithinGeoRange:
		return "WithinGeoRange"
	case OperatorLike:
		return "Like"
	case OperatorIsNull:
		return "IsNull"
	default:
		panic("Unknown operator")
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
