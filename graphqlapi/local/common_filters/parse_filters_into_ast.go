package common_filters

import (
	"encoding/json"
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"strings"
	"time"
)

// Extract the filters from the arguments of a Local->Get or Local->GetMeta query.
func ExtractFilters(args map[string]interface{}) (*LocalFilter, error) {
	where, wherePresent := args["where"]
	if !wherePresent {
		// No filters; all is fine!
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

// Parse a single clause
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

// Parses a 'comperator' filter
// Each of those has:
// 1. The operator applied (e.g. Equal, LessThanEqual)
// 2. A value (valueString, valueDate)
// 3. The path ["SomeAction", "color"]
func parseCompareOp(args map[string]interface{}, operator Operator) (*Clause, error) {
	_, operandsPresent := args["operands"]

	if operandsPresent {
		return nil, fmt.Errorf("a 'operands' is given in clause '%s'; this is not allowed for a %s clause", jsonify(args), operator.Name())
	}

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

// Parse an 'operand' filter.
// One of those has:
// 1. The operator appied (e.g. And, Or)
// 2. The operands (e.g. a list of operands)
//    Each operand will be parsed as a new clause.
func parseOperandsOp(args map[string]interface{}, operator Operator) (*Clause, error) {
	_, pathPresent := args["path"]

	if pathPresent {
		return nil, fmt.Errorf("a 'path' is given in clause '%s'; this is not allowed for a %s clause", jsonify(args), operator.Name())
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
			return nil, fmt.Errorf("The operand '%s' is not valid", jsonify(rawOperand))
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
		Operands: operands,
	}, nil
}

// Parses the path
// It parses an array of strings in this format
// [0] ClassName -> The root class name we're drilling down from
// [1] propertyName -> The property name we're interested in.
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

	// The sentinel is used to bootstrap the inlined recursion.
	// we return sentinal.Child at the end.
	var sentinel Path

	// Keep track of where we are in the path (e.g. always points to latest Path segment)
	var current *Path = &sentinel

	// Now go through the path elements, step over it in increments of two.
	// Simple case:      ClassName -> property
	// Nested path case: ClassName -> HasRef -> ClassOfRef -> Property
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

		// Invalid property name?
		// Try to parse it as as a reference.
		if err != nil {
			untitlizedPropertyName := strings.ToLower(rawPropertyName[0:1]) + rawPropertyName[1:len(rawPropertyName)]
			err, propertyName = schema.ValidatePropertyName(untitlizedPropertyName)

			if err != nil {
				return nil, fmt.Errorf("Expected a valid property name in 'path' field for the filter '%s', but got '%s'", jsonify(args), rawPropertyName)
			}
		}

		current.Child = &Path{
			Class:    className,
			Property: propertyName,
		}

		// And down we go.
		current = current.Child
	}

	return sentinel.Child, nil
}

// Parse a value used in a comparator operator.
func parseValue(args map[string]interface{}) (*Value, error) {
	// Split into this two parts:
	// 1. This function that loops over the extractors and ensures exactly one value is found
	// 2. A list of extractors that test if any of them matches (valueExtractors)

	var value *Value

	for _, extractor := range valueExtractors {
		foundValue, err := extractor(args)

		// Abort if we found a value, but it's for being passed a string to an int value.
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

// List of functions that can potentially extract a Value from the various valueXXXX fields in a clause.
var valueExtractors [](func(args map[string]interface{}) (*Value, error)) = [](func(args map[string]interface{}) (*Value, error)){
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

// Small utility function used in printing error messages.
func jsonify(stuff interface{}) string {
	j, _ := json.Marshal(stuff)
	return string(j)
}
