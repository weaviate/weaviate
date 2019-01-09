package filters

import (
	"fmt"

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
	predicate, err := gremlinPredicateFromOperator(f.filter.Root.Operator, f.filter.Root.Value.Value)
	if err != nil {
		return "", fmt.Errorf("operator %s with value %v: %s", f.filter.Root.Operator.Name(),
			f.filter.Root.Value.Value, err)
	}

	q = q.Has("population", predicate)
	return q.String(), nil
}

func gremlinPredicateFromOperator(operator common_filters.Operator, value interface{}) (*gremlin.Query, error) {
	// for now we're pretending only int's exist

	valueTyped := value.(int64)
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
		return nil, fmt.Errorf("unrecoginzed operator %s", operator.Name())
	}

}
