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
 */package fetch

import (
	"fmt"
	"time"

	"github.com/creativesoftwarefdn/weaviate/entities/filters"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
)

func (b *Query) conditionQuery(match kinds.FetchPropertyMatch) (*gremlin.Query, error) {
	switch match.Value.Type {
	case schema.DataTypeString:
		return b.stringCondition(match)
	case schema.DataTypeText:
		return nil, fmt.Errorf("indexing/searching not allowed on prop type text")
	case schema.DataTypeBoolean:
		return b.boolCondition(match)
	case schema.DataTypeInt:
		return b.intCondition(match)
	case schema.DataTypeNumber:
		return b.numberCondition(match)
	case schema.DataTypeDate:
		return b.dateCondition(match)
	}

	return nil, fmt.Errorf("unsupported combination of operator and value")
}

func (b *Query) stringCondition(match kinds.FetchPropertyMatch) (*gremlin.Query, error) {
	switch match.Operator {
	case filters.OperatorEqual:
		return gremlin.EqString(match.Value.Value.(string)), nil
	case filters.OperatorNotEqual:
		return gremlin.NeqString(match.Value.Value.(string)), nil
	}

	return nil, fmt.Errorf("unsupported combination of operator and string value")
}

func (b *Query) boolCondition(match kinds.FetchPropertyMatch) (*gremlin.Query, error) {
	switch match.Operator {
	case filters.OperatorEqual:
		return gremlin.EqBool(match.Value.Value.(bool)), nil
	case filters.OperatorNotEqual:
		return gremlin.NeqBool(match.Value.Value.(bool)), nil
	}

	return nil, fmt.Errorf("unsupported combination of operator and boolean value")
}

func (b *Query) intCondition(match kinds.FetchPropertyMatch) (*gremlin.Query, error) {
	switch match.Operator {
	case filters.OperatorEqual:
		return gremlin.EqInt(match.Value.Value.(int)), nil
	case filters.OperatorNotEqual:
		return gremlin.NeqInt(match.Value.Value.(int)), nil
	case filters.OperatorLessThan:
		return gremlin.LtInt(match.Value.Value.(int)), nil
	case filters.OperatorGreaterThan:
		return gremlin.GtInt(match.Value.Value.(int)), nil
	case filters.OperatorLessThanEqual:
		return gremlin.LteInt(match.Value.Value.(int)), nil
	case filters.OperatorGreaterThanEqual:
		return gremlin.GteInt(match.Value.Value.(int)), nil
	}

	return nil, fmt.Errorf("unsupported combination of operator and int value")
}

func (b *Query) numberCondition(match kinds.FetchPropertyMatch) (*gremlin.Query, error) {
	switch match.Operator {
	case filters.OperatorEqual:
		return gremlin.EqFloat(match.Value.Value.(float64)), nil
	case filters.OperatorNotEqual:
		return gremlin.NeqFloat(match.Value.Value.(float64)), nil
	case filters.OperatorLessThan:
		return gremlin.LtFloat(match.Value.Value.(float64)), nil
	case filters.OperatorGreaterThan:
		return gremlin.GtFloat(match.Value.Value.(float64)), nil
	case filters.OperatorLessThanEqual:
		return gremlin.LteFloat(match.Value.Value.(float64)), nil
	case filters.OperatorGreaterThanEqual:
		return gremlin.GteFloat(match.Value.Value.(float64)), nil
	}

	return nil, fmt.Errorf("unsupported combination of operator and number value")
}

func (b *Query) dateCondition(match kinds.FetchPropertyMatch) (*gremlin.Query, error) {
	switch match.Operator {
	case filters.OperatorEqual:
		return gremlin.EqDate(match.Value.Value.(time.Time)), nil
	case filters.OperatorNotEqual:
		return gremlin.NeqDate(match.Value.Value.(time.Time)), nil
	case filters.OperatorLessThan:
		return gremlin.LtDate(match.Value.Value.(time.Time)), nil
	case filters.OperatorGreaterThan:
		return gremlin.GtDate(match.Value.Value.(time.Time)), nil
	case filters.OperatorLessThanEqual:
		return gremlin.LteDate(match.Value.Value.(time.Time)), nil
	case filters.OperatorGreaterThanEqual:
		return gremlin.GteDate(match.Value.Value.(time.Time)), nil
	}

	return nil, fmt.Errorf("unsupported combination of operator and date value")
}
