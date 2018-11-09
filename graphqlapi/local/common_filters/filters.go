package common_filters

import (
	"encoding/json"
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/graphql-go/graphql"
)

var CommonFilters graphql.InputObjectConfigFieldMap = graphql.InputObjectConfigFieldMap{
	"operator": &graphql.InputObjectFieldConfig{
		Type: graphql.NewEnum(graphql.EnumConfig{
			Name: "WhereOperatorEnum",
			Values: graphql.EnumValueConfigMap{
				"And":              &graphql.EnumValueConfig{},
				"Or":               &graphql.EnumValueConfig{},
				"Equal":            &graphql.EnumValueConfig{},
				"Not":              &graphql.EnumValueConfig{},
				"NotEqual":         &graphql.EnumValueConfig{},
				"GreaterThan":      &graphql.EnumValueConfig{},
				"GreaterThanEqual": &graphql.EnumValueConfig{},
				"LessThan":         &graphql.EnumValueConfig{},
				"LessThanEqual":    &graphql.EnumValueConfig{},
			},
			Description: "Enumeration object for the 'where' filter",
		}),
		Description: "Operator in the 'where' filter field, value is one of the 'WhereOperatorEnum' object",
	},
	"path": &graphql.InputObjectFieldConfig{
		Type:        graphql.NewList(graphql.String),
		Description: "Path of from 'Things' or 'Actions' to the property name through the classes",
	},
	"valueInt": &graphql.InputObjectFieldConfig{
		Type:        graphql.Int,
		Description: "Integer value that the property at the provided path will be compared to by an operator",
	},
	"valueNumber": &graphql.InputObjectFieldConfig{
		Type:        graphql.Float,
		Description: "Number value that the property at the provided path will be compared to by an operator",
	},
	"valueBoolean": &graphql.InputObjectFieldConfig{
		Type:        graphql.Boolean,
		Description: "Boolean value that the property at the provided path will be compared to by an operator",
	},
	"valueString": &graphql.InputObjectFieldConfig{
		Type:        graphql.String,
		Description: "String value that the property at the provided path will be compared to by an operator",
	},
}

func init() {
	var operands *graphql.InputObject

	operands = graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name: "WhereOperandsInpObj",
			Fields: (graphql.InputObjectConfigFieldMapThunk)(func() graphql.InputObjectConfigFieldMap {
				CommonFilters["operands"] = &graphql.InputObjectFieldConfig{
					Type:        graphql.NewList(operands),
					Description: "Operands in the 'where' filter field, is a list of objects",
				}
				return CommonFilters
			}),
			Description: "Operands in the 'where' filter field, is a list of objects",
		},
	)

	CommonFilters["operands"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.NewList(operands),
		Description: "Operands in the 'where' filter field, is a list of objects",
	}
}

type Operator int

const OperatorEqual = 1

type LocalFilter struct {
	Root *Clause
}

type Path struct {
	Kind     kind.Kind
	Class    schema.ClassName
	Property schema.PropertyName
}

type Value struct {
	Value interface{}
}

type Clause struct {
	Operator Operator
	On       *Path
	Value    *Value
}

func ExtractFilters(args map[string]interface{}) (*LocalFilter, error) {
	where, wherePresent := args["where"]
	if !wherePresent {
		return nil, nil
	} else {
		whereMap := where.(map[string]interface{}) // guaranteed by GraphQL to be a map.
		rootClause, err := parseClause(whereMap)
		if err != nil {
			return nil, err
		} else {
			return &LocalFilter{Root: rootClause}, nil
		}
	}
}

func parseClause(args map[string]interface{}) (*Clause, error) {
	operator, operatorOk := args["operator"]
	if !operatorOk {
		return nil, fmt.Errorf("operand is missing in clause %s", jsonify(args))
	}

	switch operator {
	case "Equal":
		return &Clause{
			Operator: OperatorEqual,
		}, nil
	default:
		return nil, fmt.Errorf("Unknown operator '%s' in clause %s", operator, jsonify(args))
	}
}

func jsonify(stuff interface{}) string {
	j, _ := json.Marshal(stuff)
	return string(j)
}
