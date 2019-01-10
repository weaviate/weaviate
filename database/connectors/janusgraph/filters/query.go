package filters

import (
	"fmt"
	"time"

	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

// FilterQuery from filter params. Can be appended to any GremlinFilterQuery
type FilterQuery struct {
	filter     *common_filters.LocalFilter
	nameSource nameSource
}

type nameSource interface {
	GetMappedPropertyName(className schema.ClassName, propName schema.PropertyName) state.MappedPropertyName
}

// New FilterQuery from local filter params
func New(filter *common_filters.LocalFilter, nameSource nameSource) *FilterQuery {
	return &FilterQuery{
		filter:     filter,
		nameSource: nameSource,
	}
}

func (f *FilterQuery) String() (string, error) {
	if f.filter == nil {
		return "", nil
	}

	clause := f.filter.Root
	q, err := f.buildClause(clause)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(".%s", q.String()), nil
}

func (f *FilterQuery) buildClause(clause *common_filters.Clause) (*gremlin.Query, error) {
	if clause.Operator.OnValue() {
		return f.buildValueClause(clause)
	}

	if clause.Operator != common_filters.OperatorAnd {
		return nil, fmt.Errorf("unknown operator '%#v'", clause.Operator)
	}

	return f.buildOperandClause(clause)
}

func (f *FilterQuery) buildValueClause(clause *common_filters.Clause) (*gremlin.Query, error) {
	q := &gremlin.Query{}
	predicate, err := gremlinPredicateFromOperator(clause.Operator, clause.Value)
	if err != nil {
		return q, fmt.Errorf("operator %v with value '%v': %s", clause.Operator,
			clause.Value.Value, err)
	}

	var propName string
	if f.nameSource == nil {
		propName = string(clause.On.Property)
	} else {
		propName = string(f.nameSource.GetMappedPropertyName(clause.On.Class, clause.On.Property))
	}

	q = q.Has(propName, predicate)
	return q, nil
}

func (f *FilterQuery) buildOperandClause(clause *common_filters.Clause) (*gremlin.Query, error) {
	q := &gremlin.Query{}
	individualQueries := make([]*gremlin.Query, len(clause.Operands), len(clause.Operands))
	for i, operand := range clause.Operands {
		result, err := f.buildClause(&operand)
		if err != nil {
			return q, fmt.Errorf("build operand '%s': %s", clause.Operator.Name(), err)
		}
		individualQueries[i] = result
	}

	q = q.And(individualQueries...)
	return q, nil
}

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
