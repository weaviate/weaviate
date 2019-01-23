/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package common_filters

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema"
)

type Operator int

const (
	OperatorEqual            Operator = 1
	OperatorNotEqual         Operator = 2
	OperatorGreaterThan      Operator = 3
	OperatorGreaterThanEqual Operator = 4
	OperatorLessThan         Operator = 5
	OperatorLessThanEqual    Operator = 6
	OperatorAnd              Operator = 7
	OperatorOr               Operator = 8
	OperatorNot              Operator = 9
)

func (o Operator) OnValue() bool {
	switch o {
	case OperatorEqual,
		OperatorNotEqual,
		OperatorGreaterThan,
		OperatorGreaterThanEqual,
		OperatorLessThan,
		OperatorLessThanEqual:
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
	default:
		panic("Unknown operator")
	}
}

type LocalFilter struct {
	Root *Clause
}

// Represents the path in a filter.
// Either RelationProperty or PrimitiveProperty must be empty (e.g. "").
type Path struct {
	Class    schema.ClassName
	Property schema.PropertyName

	// If nil, then this is the property we're interested in.
	// If a pointer to another Path, the constraint applies to that one.
	Child *Path
}

type Value struct {
	Value interface{}
	Type  schema.DataType
}

type Clause struct {
	Operator Operator
	On       *Path
	Value    *Value
	Operands []Clause
}

// GetInnerMost recursively searches for child paths, only when no more
// children can be found will the path be returned
func (p *Path) GetInnerMost() *Path {
	if p.Child == nil {
		return p
	}

	return p.Child.GetInnerMost()
}

// Slice flattens the nested path into a slice of segments
func (p *Path) Slice() []interface{} {
	result := []interface{}{}
	// On first round ommit ClassName

	result = append(result, string(p.Property))
	return result
}
