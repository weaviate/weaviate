package filters

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

// FilterQuery from filter params. Can be appended to any GremlinFilterQuery
type FilterQuery struct {
	filter *common_filters.LocalFilter
}

// New FilterQuery from local filter params
func New(filter *common_filters.LocalFilter) *FilterQuery {
	return &FilterQuery{
		filter: filter,
	}
}

func (f *FilterQuery) String() (string, error) {
	if f.filter == nil {
		return "", nil
	}

	q := &gremlin.Query{}
	predicate, err := gremlinPredicateFromOperator(f.filter.Root.Operator, f.filter.Root.Value)
	if err != nil {
		return "", fmt.Errorf("operator %v with value %v: %s", f.filter.Root.Operator,
			f.filter.Root.Value.Value, err)
	}

	q = q.Has(string(f.filter.Root.On.Property), predicate)
	return q.String(), nil
}

func gremlinPredicateFromOperator(operator common_filters.Operator, value *common_filters.Value) (*gremlin.Query, error) {
	switch value.Type {
	case schema.DataTypeInt:
		return gremlinIntPredicateFromOperator(operator, value.Value)
	case schema.DataTypeNumber:
		return gremlinFloatPredicateFromOperator(operator, value.Value)
	default:
		return nil, fmt.Errorf("unsupported value type '%v'", value.Type)
	}
}

func gremlinIntPredicateFromOperator(operator common_filters.Operator, value interface{}) (*gremlin.Query, error) {
	valueTyped, ok := value.(int64)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int64, but was %t", value)
	}

	switch operator {
	case common_filters.OperatorEqual:
		return gremlin.EqInt(int(valueTyped)), nil
	case common_filters.OperatorNotEqual:
		return gremlin.NeqInt(int(valueTyped)), nil
	case common_filters.OperatorLessThan:
		return gremlin.LtInt(int(valueTyped)), nil
	case common_filters.OperatorLessThanEqual:
		return gremlin.LteInt(int(valueTyped)), nil
	case common_filters.OperatorGreaterThan:
		return gremlin.GtInt(int(valueTyped)), nil
	case common_filters.OperatorGreaterThanEqual:
		return gremlin.GteInt(int(valueTyped)), nil
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
