/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package common_filters

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
)

// Extract the filters from the arguments of a Local->Get or Local->GetMeta query.
func ExtractFilters(args map[string]interface{}, rootClass string) (*LocalFilter, error) {
	where, wherePresent := args["where"]
	if !wherePresent {
		// No filters; all is fine!
		return nil, nil
	} else {
		whereMap := where.(map[string]interface{}) // guaranteed by GraphQL to be a map.
		rootClause, err := parseClause(whereMap, rootClass)
		if err != nil {
			return nil, err
		} else {
			return &LocalFilter{Root: rootClause}, nil
		}
	}
}

// Parse a single clause
func parseClause(args map[string]interface{}, rootClass string) (*Clause, error) {
	operator, operatorOk := args["operator"]
	if !operatorOk {
		return nil, fmt.Errorf("operand is missing in clause %s", jsonify(args))
	}

	var clause *Clause
	var err error

	switch operator {
	case "And":
		clause, err = parseOperandsOp(args, OperatorAnd, rootClass)
	case "Or":
		clause, err = parseOperandsOp(args, OperatorOr, rootClass)
	case "Not":
		clause, err = parseOperandsOp(args, OperatorOr, rootClass)
	case "Equal":
		clause, err = parseCompareOp(args, OperatorEqual, rootClass)
	case "NotEqual":
		clause, err = parseCompareOp(args, OperatorNotEqual, rootClass)
	case "GreaterThan":
		clause, err = parseCompareOp(args, OperatorGreaterThan, rootClass)
	case "GreaterThanEqual":
		clause, err = parseCompareOp(args, OperatorGreaterThanEqual, rootClass)
	case "LessThan":
		clause, err = parseCompareOp(args, OperatorLessThan, rootClass)
	case "LessThanEqual":
		clause, err = parseCompareOp(args, OperatorLessThanEqual, rootClass)
	case "WithinGeoRange":
		clause, err = parseCompareOp(args, OperatorWithinGeoRange, rootClass)
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
func parseCompareOp(args map[string]interface{}, operator Operator, rootClass string) (*Clause, error) {
	_, operandsPresent := args["operands"]

	if operandsPresent {
		return nil, fmt.Errorf("a 'operands' is given in clause '%s'; this is not allowed for a %s clause", jsonify(args), operator.Name())
	}

	path, err := parsePathFromArgs(args, rootClass)
	if err != nil {
		return nil, err
	}

	value, err := ParseValue(args)
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
func parseOperandsOp(args map[string]interface{}, operator Operator, rootClass string) (*Clause, error) {
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

		operand, err := parseClause(rawOperandMap, rootClass)

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

func parsePathFromArgs(args map[string]interface{}, rootClass string) (*Path, error) {
	rawPath, ok := args["path"]
	if !ok {
		return nil, fmt.Errorf("Missing the 'path' field for the filter '%s'", jsonify(args))
	}

	pathElements, ok := rawPath.([]interface{})
	if !ok {
		return nil, fmt.Errorf("The 'path' field for the filter '%s' is not a list of strings", jsonify(args))
	}

	path, err := ParsePath(pathElements, rootClass)
	if err != nil {
		return nil, fmt.Errorf("invalid 'path' field for filter '%s': %s", jsonify(args), err)
	}

	return path, nil
}

// ParseValue used in a comparator operator.
func ParseValue(args map[string]interface{}) (*Value, error) {
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
		}

		return &Value{
			Type:  schema.DataTypeNumber,
			Value: val,
		}, nil
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
		}

		return &Value{
			Type:  schema.DataTypeBoolean,
			Value: val,
		}, nil
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
		}

		return &Value{
			Type:  schema.DataTypeString,
			Value: val,
		}, nil
	},
	func(args map[string]interface{}) (*Value, error) {
		rawVal, ok := args["valueGeoRange"]
		if !ok {
			return nil, nil
		}

		geoMap, ok := rawVal.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("the provided valueString is not a map")
		}

		c9s := geoMap["geoCoordinates"].(map[string]interface{})
		lat := c9s["latitude"].(float64)
		lon := c9s["longitude"].(float64)
		distance := geoMap["distance"].(map[string]interface{})
		maxDist := distance["max"].(float64)

		return &Value{
			Type: schema.DataTypeGeoCoordinates,
			Value: GeoRange{
				GeoCoordinates: &models.GeoCoordinates{
					Latitude:  float32(lat),
					Longitude: float32(lon),
				},
				Distance: float32(maxDist),
			},
		}, nil
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
		}

		date, err := time.Parse(time.RFC3339, stringVal)

		if err != nil {
			return nil, fmt.Errorf("failed to parse the value '%s' as a date in valueDate", stringVal)
		}

		return &Value{
			Type:  schema.DataTypeDate,
			Value: date,
		}, nil
	},
}

// Small utility function used in printing error messages.
func jsonify(stuff interface{}) string {
	j, _ := json.Marshal(stuff)
	return string(j)
}
