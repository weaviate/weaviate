package fetch

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/fetch"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

func (b *Query) conditionQuery(match fetch.PropertyMatch) (*gremlin.Query, error) {
	switch match.Value.Type {
	case schema.DataTypeString:
		return b.stringCondition(match)
	case schema.DataTypeInt:
		return b.intCondition(match)
	}

	return nil, fmt.Errorf("unsupported combination of operator and value")
}

func (b *Query) stringCondition(match fetch.PropertyMatch) (*gremlin.Query, error) {
	switch match.Operator {
	case common_filters.OperatorEqual:
		return gremlin.EqString(match.Value.Value.(string)), nil
	case common_filters.OperatorNotEqual:
		return gremlin.NeqString(match.Value.Value.(string)), nil
	}

	return nil, fmt.Errorf("unsupported combination of operator and string value")
}

func (b *Query) intCondition(match fetch.PropertyMatch) (*gremlin.Query, error) {
	switch match.Operator {
	case common_filters.OperatorEqual:
		return gremlin.EqInt(match.Value.Value.(int)), nil
	case common_filters.OperatorNotEqual:
		return gremlin.NeqInt(match.Value.Value.(int)), nil
	case common_filters.OperatorLessThan:
		return gremlin.LtInt(match.Value.Value.(int)), nil
	case common_filters.OperatorGreaterThan:
		return gremlin.GtInt(match.Value.Value.(int)), nil
	case common_filters.OperatorLessThanEqual:
		return gremlin.LteInt(match.Value.Value.(int)), nil
	case common_filters.OperatorGreaterThanEqual:
		return gremlin.GteInt(match.Value.Value.(int)), nil
	}

	return nil, fmt.Errorf("unsupported combination of operator and string value")
}
