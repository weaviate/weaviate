package common_filters

import (
	"encoding/json"
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"time"
)

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

	var clause *Clause
	var err error

	switch operator {
	case "And":
		clause, err = parseOperandsOp(args, OperatorAnd)
	case "Or":
		clause, err = parseOperandsOp(args, OperatorOr)
	case "Not":
		clause, err = parseOperandsOp(args, OperatorOr)
	case "Equal":
		clause, err = parseCompareOp(args, OperatorEqual)
	case "NotEqual":
		clause, err = parseCompareOp(args, OperatorNotEqual)
	case "GreaterThan":
		clause, err = parseCompareOp(args, OperatorGreaterThan)
	case "GreaterThanEqual":
		clause, err = parseCompareOp(args, OperatorGreaterThanEqual)
	case "LessThan":
		clause, err = parseCompareOp(args, OperatorLessThan)
	case "LessThanEqual":
		clause, err = parseCompareOp(args, OperatorLessThanEqual)
	default:
		err = fmt.Errorf("Unknown operator '%s' in clause %s", operator, jsonify(args))
	}

	return clause, err
}

func parseCompareOp(args map[string]interface{}, operator Operator) (*Clause, error) {
	path, err := parsePath(args)
	if err != nil {
		return nil, err
	}

	value, err := parseValue(args)
	if err != nil {
		return nil, err
	}

	return &Clause{
		Operator: operator,
		On:       path,
		Value:    value,
	}, nil
}

func parseOperandsOp(args map[string]interface{}, operator Operator) (*Clause, error) {
	path, err := parsePath(args)
	if err != nil {
		return nil, err
	}

	rawOperands, ok := args["operands"]
	if !ok {
		return nil, fmt.Errorf("No operands given in clause '%s'", jsonify(args))
	}

	rawOperandsList, ok := rawOperands.([]interface{})
	if !ok {
		return nil, fmt.Errorf("The operands in clause '%s' are not a list", jsonify(args))
	}

	var operands []Clause

	for _, rawOperand := range rawOperandsList {
		rawOperandMap, ok := rawOperand.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("The operand '%s' is not valid")
		}

		operand, err := parseClause(rawOperandMap)

		if err != nil {
			return nil, err
		}

		operands = append(operands, *operand)
	}

	if len(operands) == 0 {
		return nil, fmt.Errorf("Empty clause given")
	}

	return &Clause{
		Operator: operator,
		On:       path,
		Operands: operands,
	}, nil
}

func parsePath(args map[string]interface{}) (*Path, error) {
	rawPath, ok := args["path"]
	if !ok {
		return nil, fmt.Errorf("Missing the 'path' field for the filter '%s'", jsonify(args))
	}

	pathElements, ok := rawPath.([]interface{})
	if !ok {
		return nil, fmt.Errorf("The 'path' field for the filter '%s' is not a list of strings", jsonify(args))
	}

	if len(pathElements) < 2 {
		return nil, fmt.Errorf("The 'path' field for the filter '%s' is empty! You need to specify at least a class and a property name", jsonify(args))
	}

	var sentinel Path
	var current *Path = &sentinel

	for i := 0; i < len(pathElements); i += 2 {
		lengthRemaining := len(pathElements) - i
		if lengthRemaining < 2 {
			return nil, fmt.Errorf("The 'path' field for the filter '%s' is invalid! Missing an argument after '%s'", jsonify(args), pathElements[i])
		}

		rawClassName, ok := pathElements[i].(string)
		if !ok {
			return nil, fmt.Errorf("The 'path' field for the filter '%s' is invalid! Element %v is not a string", jsonify(args), i+1)
		}

		rawPropertyName, ok := pathElements[i+1].(string)
		if !ok {
			return nil, fmt.Errorf("The 'path' field for the filter '%s' is invalid! Element %v is not a string", jsonify(args), i+2)
		}

		err, className := schema.ValidateClassName(rawClassName)
		if err != nil {
			return nil, fmt.Errorf("Expected a valid class name in 'path' field for the filter '%s', but got '%s'", jsonify(args), rawClassName)
		}

		err, propertyName := schema.ValidatePropertyName(rawPropertyName)
		if err != nil {
			return nil, fmt.Errorf("Expected a valid property name in 'path' field for the filter '%s', but got '%s'", jsonify(args), rawPropertyName)
		}

		current.Child = &Path{
			Class:    className,
			Property: propertyName,
		}

		current = current.Child
	}

	return sentinel.Child, nil
}

// List of functions that can potentially extract a Value from the various valueXXXX fields in a clause.
var dataTypeExtractors [](func(args map[string]interface{}) (*Value, error)) = [](func(args map[string]interface{}) (*Value, error)){
	// Ints
	func(args map[string]interface{}) (*Value, error) {
		rawVal, ok := args["valueInt"]
		if !ok {
			return nil, nil
		}

		val, ok := rawVal.(int)
		if !ok {
			return nil, fmt.Errorf("the provided valueInt is not an int")
		} else {
			return &Value{
				Type:  schema.DataTypeInt,
				Value: val,
			}, nil
		}
	},
	// Number
	func(args map[string]interface{}) (*Value, error) {
		rawVal, ok := args["valueNumber"]
		if !ok {
			return nil, nil
		}

		val, ok := rawVal.(float64)
		if !ok {
			return nil, fmt.Errorf("the provided valueNumber is not a float")
		} else {
			return &Value{
				Type:  schema.DataTypeNumber,
				Value: val,
			}, nil
		}
	},
	// Boolean
	func(args map[string]interface{}) (*Value, error) {
		rawVal, ok := args["valueBoolean"]
		if !ok {
			return nil, nil
		}

		val, ok := rawVal.(bool)
		if !ok {
			return nil, fmt.Errorf("the provided valueBool is not a boolean")
		} else {
			return &Value{
				Type:  schema.DataTypeBoolean,
				Value: val,
			}, nil
		}
	},
	// Strings
	func(args map[string]interface{}) (*Value, error) {
		rawVal, ok := args["valueString"]
		if !ok {
			return nil, nil
		}

		val, ok := rawVal.(string)
		if !ok {
			return nil, fmt.Errorf("the provided valueString is not a string")
		} else {
			return &Value{
				Type:  schema.DataTypeString,
				Value: val,
			}, nil
		}
	},
	// Dates
	func(args map[string]interface{}) (*Value, error) {
		rawVal, ok := args["valueDate"]
		if !ok {
			return nil, nil
		}

		stringVal, ok := rawVal.(string)
		if !ok {
			return nil, fmt.Errorf("the provided valueDate is not a date string")
		} else {
			date, err := time.Parse(time.RFC3339, stringVal)

			if err != nil {
				return nil, fmt.Errorf("failed to parse the value '%s' as a date in valueDate", stringVal)
			}

			return &Value{
				Type:  schema.DataTypeString,
				Value: date.Format(time.RFC3339),
			}, nil
		}
	},
}

func parseValue(args map[string]interface{}) (*Value, error) {
	var value *Value

	for _, extractor := range dataTypeExtractors {
		foundValue, err := extractor(args)
		if err != nil {
			return nil, err
		}

		if foundValue != nil {
			if value != nil {
				return nil, fmt.Errorf("Found two values the clause '%s'", jsonify(args))
			} else {
				value = foundValue
			}
		}
	}

	if value == nil {
		return nil, fmt.Errorf("No value given in filter '%s'", jsonify(args))
	}

	return value, nil
}

func jsonify(stuff interface{}) string {
	j, _ := json.Marshal(stuff)
	return string(j)
}
