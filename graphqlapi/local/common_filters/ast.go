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

	// If nil, than this is the property we're interested in.
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
