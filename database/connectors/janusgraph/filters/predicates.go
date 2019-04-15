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
package filters

import (
	"fmt"
	"time"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	cf "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

func gremlinPredicateFromOperator(operator cf.Operator,
	value *cf.Value) (*gremlin.Query, error) {
	switch value.Type {
	case schema.DataTypeInt:
		return gremlinIntPredicateFromOperator(operator, value.Value)
	case schema.DataTypeNumber:
		return gremlinFloatPredicateFromOperator(operator, value.Value)
	case schema.DataTypeString:
		return gremlinStringPredicateFromOperator(operator, value.Value)
	case schema.DataTypeBoolean:
		return gremlinBoolPredicateFromOperator(operator, value.Value)
	case schema.DataTypeDate:
		return gremlinDatePredicateFromOperator(operator, value.Value)
	case schema.DataTypeGeoCoordinates:
		return gremlinGeoCoordinatesPredicateFromOperator(operator, value.Value)
	default:
		return nil, fmt.Errorf("unsupported value type '%v'", value.Type)
	}
}

func gremlinIntPredicateFromOperator(operator cf.Operator, value interface{}) (*gremlin.Query, error) {
	valueTyped, ok := value.(int)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int64, but was %t", value)
	}

	switch operator {
	case cf.OperatorEqual:
		return gremlin.EqInt(valueTyped), nil
	case cf.OperatorNotEqual:
		return gremlin.NeqInt(valueTyped), nil
	case cf.OperatorLessThan:
		return gremlin.LtInt(valueTyped), nil
	case cf.OperatorLessThanEqual:
		return gremlin.LteInt(valueTyped), nil
	case cf.OperatorGreaterThan:
		return gremlin.GtInt(valueTyped), nil
	case cf.OperatorGreaterThanEqual:
		return gremlin.GteInt(valueTyped), nil
	default:
		return nil, fmt.Errorf("unrecoginzed operator %v", operator)
	}
}

func gremlinFloatPredicateFromOperator(operator cf.Operator, value interface{}) (*gremlin.Query, error) {
	valueTyped, ok := value.(float64)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int64, but was %t", value)
	}

	switch operator {
	case cf.OperatorEqual:
		return gremlin.EqFloat(float64(valueTyped)), nil
	case cf.OperatorNotEqual:
		return gremlin.NeqFloat(float64(valueTyped)), nil
	case cf.OperatorLessThan:
		return gremlin.LtFloat(float64(valueTyped)), nil
	case cf.OperatorLessThanEqual:
		return gremlin.LteFloat(float64(valueTyped)), nil
	case cf.OperatorGreaterThan:
		return gremlin.GtFloat(float64(valueTyped)), nil
	case cf.OperatorGreaterThanEqual:
		return gremlin.GteFloat(float64(valueTyped)), nil
	default:
		return nil, fmt.Errorf("unrecoginzed operator %v", operator)
	}
}

func gremlinDatePredicateFromOperator(operator cf.Operator, value interface{}) (*gremlin.Query, error) {
	valueTyped, ok := value.(time.Time)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int64, but was %t", value)
	}

	switch operator {
	case cf.OperatorEqual:
		return gremlin.EqDate(valueTyped), nil
	case cf.OperatorNotEqual:
		return gremlin.NeqDate(valueTyped), nil
	case cf.OperatorLessThan:
		return gremlin.LtDate(valueTyped), nil
	case cf.OperatorLessThanEqual:
		return gremlin.LteDate(valueTyped), nil
	case cf.OperatorGreaterThan:
		return gremlin.GtDate(valueTyped), nil
	case cf.OperatorGreaterThanEqual:
		return gremlin.GteDate(valueTyped), nil
	default:
		return nil, fmt.Errorf("unrecoginzed operator %v", operator)
	}
}

func gremlinGeoCoordinatesPredicateFromOperator(operator cf.Operator, value interface{}) (*gremlin.Query, error) {
	r, ok := value.(cf.GeoRange)
	if !ok {
		return nil, fmt.Errorf("expected value to be a GeoRange, but was %t", value)
	}

	switch operator {
	case cf.OperatorWithinGeoRange:
		return gremlin.GeoWithinCircle(r.Latitude, r.Longitude, r.Distance), nil
	default:
		return nil, fmt.Errorf("geoCoordinates only supports WithinGeoRange operator, but got %v", operator)
	}
}

func gremlinStringPredicateFromOperator(operator cf.Operator, value interface{}) (*gremlin.Query, error) {
	valueTyped, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int64, but was %t", value)
	}

	switch operator {
	case cf.OperatorEqual:
		return gremlin.EqString(valueTyped), nil
	case cf.OperatorNotEqual:
		return gremlin.NeqString(valueTyped), nil
	case cf.OperatorLessThan, cf.OperatorLessThanEqual,
		cf.OperatorGreaterThan, cf.OperatorGreaterThanEqual:
		// this is different from an unrecognized operator, in that we recognize
		// the operator exists, but cannot apply it on a this type. We can safely
		// call operator.Name() on it to improve the error message, whereas that
		// might not be possible on an unrecoginzed operator.
		return nil, fmt.Errorf("cannot use operator '%s' on value of type string", operator.Name())
	default:
		return nil, fmt.Errorf("unrecoginzed operator %v", operator)
	}
}

func gremlinBoolPredicateFromOperator(operator cf.Operator, value interface{}) (*gremlin.Query, error) {
	valueTyped, ok := value.(bool)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int64, but was %t", value)
	}

	switch operator {
	case cf.OperatorEqual:
		return gremlin.EqBool(valueTyped), nil
	case cf.OperatorNotEqual:
		return gremlin.NeqBool(valueTyped), nil
	case cf.OperatorLessThan, cf.OperatorLessThanEqual,
		cf.OperatorGreaterThan, cf.OperatorGreaterThanEqual:
		// this is different from an unrecognized operator, in that we recognize
		// the operator exists, but cannot apply it on a this type. We can safely
		// call operator.Name() on it to improve the error message, whereas that
		// might not be possible on an unrecoginzed operator.
		return nil, fmt.Errorf("cannot use operator '%s' on value of type string", operator.Name())
	default:
		return nil, fmt.Errorf("unrecoginzed operator %v", operator)
	}
}
