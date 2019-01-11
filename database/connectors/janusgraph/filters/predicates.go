package filters

import (
	"fmt"
	"time"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

func gremlinPredicateFromOperator(operator common_filters.Operator,
	value *common_filters.Value) (*gremlin.Query, error) {
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
	default:
		return nil, fmt.Errorf("unsupported value type '%v'", value.Type)
	}
}

func gremlinIntPredicateFromOperator(operator common_filters.Operator, value interface{}) (*gremlin.Query, error) {
	valueTyped, ok := value.(int)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int64, but was %t", value)
	}

	switch operator {
	case common_filters.OperatorEqual:
		return gremlin.EqInt(valueTyped), nil
	case common_filters.OperatorNotEqual:
		return gremlin.NeqInt(valueTyped), nil
	case common_filters.OperatorLessThan:
		return gremlin.LtInt(valueTyped), nil
	case common_filters.OperatorLessThanEqual:
		return gremlin.LteInt(valueTyped), nil
	case common_filters.OperatorGreaterThan:
		return gremlin.GtInt(valueTyped), nil
	case common_filters.OperatorGreaterThanEqual:
		return gremlin.GteInt(valueTyped), nil
	default:
		return nil, fmt.Errorf("unrecoginzed operator %v", operator)
	}
}

func gremlinFloatPredicateFromOperator(operator common_filters.Operator, value interface{}) (*gremlin.Query, error) {
	valueTyped, ok := value.(float64)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int64, but was %t", value)
	}

	switch operator {
	case common_filters.OperatorEqual:
		return gremlin.EqFloat(float64(valueTyped)), nil
	case common_filters.OperatorNotEqual:
		return gremlin.NeqFloat(float64(valueTyped)), nil
	case common_filters.OperatorLessThan:
		return gremlin.LtFloat(float64(valueTyped)), nil
	case common_filters.OperatorLessThanEqual:
		return gremlin.LteFloat(float64(valueTyped)), nil
	case common_filters.OperatorGreaterThan:
		return gremlin.GtFloat(float64(valueTyped)), nil
	case common_filters.OperatorGreaterThanEqual:
		return gremlin.GteFloat(float64(valueTyped)), nil
	default:
		return nil, fmt.Errorf("unrecoginzed operator %v", operator)
	}
}

func gremlinDatePredicateFromOperator(operator common_filters.Operator, value interface{}) (*gremlin.Query, error) {
	valueTyped, ok := value.(time.Time)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int64, but was %t", value)
	}

	switch operator {
	case common_filters.OperatorEqual:
		return gremlin.EqDate(valueTyped), nil
	case common_filters.OperatorNotEqual:
		return gremlin.NeqDate(valueTyped), nil
	case common_filters.OperatorLessThan:
		return gremlin.LtDate(valueTyped), nil
	case common_filters.OperatorLessThanEqual:
		return gremlin.LteDate(valueTyped), nil
	case common_filters.OperatorGreaterThan:
		return gremlin.GtDate(valueTyped), nil
	case common_filters.OperatorGreaterThanEqual:
		return gremlin.GteDate(valueTyped), nil
	default:
		return nil, fmt.Errorf("unrecoginzed operator %v", operator)
	}
}

func gremlinStringPredicateFromOperator(operator common_filters.Operator, value interface{}) (*gremlin.Query, error) {
	valueTyped, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int64, but was %t", value)
	}

	switch operator {
	case common_filters.OperatorEqual:
		return gremlin.EqString(valueTyped), nil
	case common_filters.OperatorNotEqual:
		return gremlin.NeqString(valueTyped), nil
	case common_filters.OperatorLessThan, common_filters.OperatorLessThanEqual,
		common_filters.OperatorGreaterThan, common_filters.OperatorGreaterThanEqual:
		// this is different from an unrecognized operator, in that we recognize
		// the operator exists, but cannot apply it on a this type. We can safely
		// call operator.Name() on it to improve the error message, whereas that
		// might not be possible on an unrecoginzed operator.
		return nil, fmt.Errorf("cannot use operator '%s' on value of type string", operator.Name())
	default:
		return nil, fmt.Errorf("unrecoginzed operator %v", operator)
	}
}

func gremlinBoolPredicateFromOperator(operator common_filters.Operator, value interface{}) (*gremlin.Query, error) {
	valueTyped, ok := value.(bool)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int64, but was %t", value)
	}

	switch operator {
	case common_filters.OperatorEqual:
		return gremlin.EqBool(valueTyped), nil
	case common_filters.OperatorNotEqual:
		return gremlin.NeqBool(valueTyped), nil
	case common_filters.OperatorLessThan, common_filters.OperatorLessThanEqual,
		common_filters.OperatorGreaterThan, common_filters.OperatorGreaterThanEqual:
		// this is different from an unrecognized operator, in that we recognize
		// the operator exists, but cannot apply it on a this type. We can safely
		// call operator.Name() on it to improve the error message, whereas that
		// might not be possible on an unrecoginzed operator.
		return nil, fmt.Errorf("cannot use operator '%s' on value of type string", operator.Name())
	default:
		return nil, fmt.Errorf("unrecoginzed operator %v", operator)
	}
}
