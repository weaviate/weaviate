//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/schema"
)

const delta = 0.00001

type assertFunc func(response map[string]interface{}) bool

type aggregateResponseAssert struct {
	t      *testing.T
	assert *assert.Assertions
}

func newAggregateResponseAssert(t *testing.T) *aggregateResponseAssert {
	return &aggregateResponseAssert{t, assert.New(t)}
}

func (a *aggregateResponseAssert) meta(count int64) assertFunc {
	return func(response map[string]interface{}) bool {
		metaKey := "meta"
		if !a.assert.Contains(response, metaKey) {
			return false
		}
		return a.hasInt(response[metaKey].(map[string]interface{}), metaKey, "count", count)
	}
}

func (a *aggregateResponseAssert) groupedBy(value string, path ...interface{}) assertFunc {
	return func(response map[string]interface{}) bool {
		groupedByKey := "groupedBy"
		if !a.assert.Contains(response, groupedByKey) {
			return false
		}
		aggMap := response[groupedByKey].(map[string]interface{})
		return combinedAssert(
			a.hasString(aggMap, groupedByKey, "value", value),
			a.hasArray(aggMap, groupedByKey, "path", path),
		)
	}
}

func (a *aggregateResponseAssert) pointingTo(propName string, path ...interface{}) assertFunc {
	return func(response map[string]interface{}) bool {
		if !a.assert.Contains(response, propName) {
			return false
		}
		aggMap := response[propName].(map[string]interface{})
		return a.hasArray(aggMap, propName, "pointingTo", path)
	}
}

func (a *aggregateResponseAssert) typedBoolean(dataType schema.DataType, propName string,
	count, totalFalse, totalTrue int64,
	percentageFalse, percentageTrue float64,
) assertFunc {
	return func(response map[string]interface{}) bool {
		if !a.assert.Contains(response, propName) {
			return false
		}
		aggMap := response[propName].(map[string]interface{})
		return combinedAssert(
			a.hasInt(aggMap, propName, "count", count),
			a.hasInt(aggMap, propName, "totalFalse", totalFalse),
			a.hasInt(aggMap, propName, "totalTrue", totalTrue),
			a.hasNumber(aggMap, propName, "percentageFalse", percentageFalse),
			a.hasNumber(aggMap, propName, "percentageTrue", percentageTrue),
			a.hasString(aggMap, propName, "type", string(dataType)),
		)
	}
}

func (a *aggregateResponseAssert) booleanArray(propName string,
	count, totalFalse, totalTrue int64,
	percentageFalse, percentageTrue float64,
) assertFunc {
	return a.typedBoolean(schema.DataTypeBooleanArray, propName, count, totalFalse, totalTrue,
		percentageFalse, percentageTrue)
}

func (a *aggregateResponseAssert) boolean(propName string,
	count, totalFalse, totalTrue int64,
	percentageFalse, percentageTrue float64,
) assertFunc {
	return a.typedBoolean(schema.DataTypeBoolean, propName, count, totalFalse, totalTrue,
		percentageFalse, percentageTrue)
}

func (a *aggregateResponseAssert) typedBoolean0(dataType schema.DataType, propName string) assertFunc {
	return func(response map[string]interface{}) bool {
		if !a.assert.Contains(response, propName) {
			return false
		}
		aggMap := response[propName].(map[string]interface{})
		return combinedAssert(
			a.hasInt(aggMap, propName, "count", 0),
			a.hasInt(aggMap, propName, "totalFalse", 0),
			a.hasInt(aggMap, propName, "totalTrue", 0),
			a.hasNil(aggMap, propName, "percentageFalse"),
			a.hasNil(aggMap, propName, "percentageTrue"),
			a.hasString(aggMap, propName, "type", string(dataType)),
		)
	}
}

func (a *aggregateResponseAssert) booleanArray0(propName string) assertFunc {
	return a.typedBoolean0(schema.DataTypeBooleanArray, propName)
}

func (a *aggregateResponseAssert) boolean0(propName string) assertFunc {
	return a.typedBoolean0(schema.DataTypeBoolean, propName)
}

func (a *aggregateResponseAssert) typedInts(dataType schema.DataType, propName string,
	count, maximum, minimum, mode, sum int64,
	median, mean float64,
) assertFunc {
	return func(response map[string]interface{}) bool {
		if !a.assert.Contains(response, propName) {
			return false
		}
		aggMap := response[propName].(map[string]interface{})
		return combinedAssert(
			a.hasInt(aggMap, propName, "count", count),
			a.hasInt(aggMap, propName, "maximum", maximum),
			a.hasInt(aggMap, propName, "minimum", minimum),
			a.hasInt(aggMap, propName, "mode", mode),
			a.hasInt(aggMap, propName, "sum", sum),
			a.hasNumber(aggMap, propName, "median", median),
			a.hasNumber(aggMap, propName, "mean", mean),
			a.hasString(aggMap, propName, "type", string(dataType)),
		)
	}
}

func (a *aggregateResponseAssert) intArray(propName string,
	count, maximum, minimum, mode, sum int64,
	median, mean float64,
) assertFunc {
	return a.typedInts(schema.DataTypeIntArray, propName, count, maximum, minimum,
		mode, sum, median, mean)
}

func (a *aggregateResponseAssert) int(propName string,
	count, maximum, minimum, mode, sum int64,
	median, mean float64,
) assertFunc {
	return a.typedInts(schema.DataTypeInt, propName, count, maximum, minimum,
		mode, sum, median, mean)
}

func (a *aggregateResponseAssert) typedInts0(dataType schema.DataType, propName string) assertFunc {
	return func(response map[string]interface{}) bool {
		if !a.assert.Contains(response, propName) {
			return false
		}
		aggMap := response[propName].(map[string]interface{})
		return combinedAssert(
			a.hasInt(aggMap, propName, "count", 0),
			a.hasNil(aggMap, propName, "maximum"),
			a.hasNil(aggMap, propName, "minimum"),
			a.hasNil(aggMap, propName, "mode"),
			a.hasNil(aggMap, propName, "sum"),
			a.hasNil(aggMap, propName, "median"),
			a.hasNil(aggMap, propName, "mean"),
			a.hasString(aggMap, propName, "type", string(dataType)),
		)
	}
}

func (a *aggregateResponseAssert) intArray0(propName string) assertFunc {
	return a.typedInts0(schema.DataTypeIntArray, propName)
}

func (a *aggregateResponseAssert) int0(propName string) assertFunc {
	return a.typedInts0(schema.DataTypeInt, propName)
}

func (a *aggregateResponseAssert) typedNumbers(dataType schema.DataType, propName string,
	count int64,
	maximum, minimum, mode, sum, median, mean float64,
) assertFunc {
	return func(response map[string]interface{}) bool {
		if !a.assert.Contains(response, propName) {
			return false
		}
		aggMap := response[propName].(map[string]interface{})
		return combinedAssert(
			a.hasInt(aggMap, propName, "count", count),
			a.hasNumber(aggMap, propName, "maximum", maximum),
			a.hasNumber(aggMap, propName, "minimum", minimum),
			a.hasNumber(aggMap, propName, "mode", mode),
			a.hasNumber(aggMap, propName, "sum", sum),
			a.hasNumber(aggMap, propName, "median", median),
			a.hasNumber(aggMap, propName, "mean", mean),
			a.hasString(aggMap, propName, "type", string(dataType)),
		)
	}
}

func (a *aggregateResponseAssert) numberArray(propName string,
	count int64,
	maximum, minimum, mode, sum, median, mean float64,
) assertFunc {
	return a.typedNumbers(schema.DataTypeNumberArray, propName, count, maximum, minimum,
		mode, sum, median, mean)
}

func (a *aggregateResponseAssert) number(propName string,
	count int64,
	maximum, minimum, mode, sum, median, mean float64,
) assertFunc {
	return a.typedNumbers(schema.DataTypeNumber, propName, count, maximum, minimum,
		mode, sum, median, mean)
}

func (a *aggregateResponseAssert) typedNumbers0(dataType schema.DataType, propName string) assertFunc {
	return func(response map[string]interface{}) bool {
		if !a.assert.Contains(response, propName) {
			return false
		}
		aggMap := response[propName].(map[string]interface{})
		return combinedAssert(
			a.hasInt(aggMap, propName, "count", 0),
			a.hasNil(aggMap, propName, "maximum"),
			a.hasNil(aggMap, propName, "minimum"),
			a.hasNil(aggMap, propName, "mode"),
			a.hasNil(aggMap, propName, "sum"),
			a.hasNil(aggMap, propName, "median"),
			a.hasNil(aggMap, propName, "mean"),
			a.hasString(aggMap, propName, "type", string(dataType)),
		)
	}
}

func (a *aggregateResponseAssert) numberArray0(propName string) assertFunc {
	return a.typedNumbers0(schema.DataTypeNumberArray, propName)
}

func (a *aggregateResponseAssert) number0(propName string) assertFunc {
	return a.typedNumbers0(schema.DataTypeNumber, propName)
}

func (a *aggregateResponseAssert) dateArray(propName string, count int64) assertFunc {
	return a.date(propName, count)
}

func (a *aggregateResponseAssert) date(propName string, count int64) assertFunc {
	return func(response map[string]interface{}) bool {
		if !a.assert.Contains(response, propName) {
			return false
		}
		return a.hasInt(response[propName].(map[string]interface{}), propName, "count", count)
	}
}

func (a *aggregateResponseAssert) dateArray0(propName string) assertFunc {
	return a.date(propName, 0)
}

func (a *aggregateResponseAssert) date0(propName string) assertFunc {
	return a.date(propName, 0)
}

func (a *aggregateResponseAssert) typedStrings(dataType schema.DataType, propName string,
	count int64,
	values []string, occurrences []int64,
) assertFunc {
	return func(response map[string]interface{}) bool {
		if !a.assert.Contains(response, propName) {
			return false
		}
		aggMap := response[propName].(map[string]interface{})
		return combinedAssert(
			a.hasInt(aggMap, propName, "count", count),
			a.hasString(aggMap, propName, "type", string(dataType)),
			a.hasOccurrences(aggMap, propName, values, occurrences),
		)
	}
}

func (a *aggregateResponseAssert) typedStrings0(dataType schema.DataType, propName string) assertFunc {
	return func(response map[string]interface{}) bool {
		if !a.assert.Contains(response, propName) {
			return false
		}
		aggMap := response[propName].(map[string]interface{})
		return combinedAssert(
			a.hasInt(aggMap, propName, "count", 0),
			a.hasString(aggMap, propName, "type", string(dataType)),
			a.hasOccurrences(aggMap, propName, nil, nil),
		)
	}
}

func (a *aggregateResponseAssert) textArray(propName string,
	count int64,
	values []string, occurrences []int64,
) assertFunc {
	return a.typedStrings(schema.DataTypeTextArray, propName, count, values, occurrences)
}

func (a *aggregateResponseAssert) text(propName string,
	count int64,
	values []string, occurrences []int64,
) assertFunc {
	return a.typedStrings(schema.DataTypeText, propName, count, values, occurrences)
}

func (a *aggregateResponseAssert) textArray0(propName string) assertFunc {
	return a.typedStrings0(schema.DataTypeTextArray, propName)
}

func (a *aggregateResponseAssert) text0(propName string) assertFunc {
	return a.typedStrings0(schema.DataTypeText, propName)
}

func (a *aggregateResponseAssert) hasOccurrences(parentMap map[string]interface{},
	parentKey string, values []string, occurrences []int64,
) bool {
	key := "topOccurrences"
	to, exists := parentMap[key]
	if !exists {
		return a.assert.Fail(fmt.Sprintf("'%s' does not have '%s'\n%#v", parentKey, key, parentMap))
	}

	toArr := to.([]interface{})
	assertResults := make([]bool, len(values))
	for i := range values {
		key := fmt.Sprintf("%s.%s[%d]", parentKey, key, i)
		toSingle := toArr[i].(map[string]interface{})
		assertResults[i] = combinedAssert(
			a.hasString(toSingle, key, "value", values[i]),
			a.hasInt(toSingle, key, "occurs", occurrences[i]),
		)
	}
	return combinedAssert(assertResults...)
}

func (a *aggregateResponseAssert) hasNumber(parentMap map[string]interface{},
	parentKey, key string, expectedValue float64,
) bool {
	v, exist := parentMap[key]
	if !exist {
		return a.assert.Fail(fmt.Sprintf("'%s' does not have '%s'\n%#v", parentKey, key, parentMap))
	}
	if v == nil {
		return a.assert.Fail(fmt.Sprintf("'%s.%s' is nil", parentKey, key))
	}
	if v, ok := v.(json.Number); ok {
		if value, err := v.Float64(); err == nil {
			if a.assert.InDelta(expectedValue, value, delta) {
				return true
			}
			return a.assert.Fail(fmt.Sprintf("'%s.%s' of %#v is not equal to %#v", parentKey, key, value, expectedValue))
		}
	}
	return a.assert.Fail(fmt.Sprintf("'%s.%s' of %#v is not equal to %#v", parentKey, key, v, expectedValue))
}

func (a *aggregateResponseAssert) hasInt(parentMap map[string]interface{},
	parentKey, key string, expectedValue int64,
) bool {
	v, exist := parentMap[key]
	if !exist {
		return a.assert.Fail(fmt.Sprintf("'%s' does not have '%s'\n%#v", parentKey, key, parentMap))
	}
	if v == nil {
		return a.assert.Fail(fmt.Sprintf("'%s.%s' is nil", parentKey, key))
	}
	if v, ok := v.(json.Number); ok {
		if value, err := v.Int64(); err == nil {
			if value == expectedValue {
				return true
			}
			return a.assert.Fail(fmt.Sprintf("'%s.%s' of %#v is not equal to %#v", parentKey, key, value, expectedValue))
		}
	}
	return a.assert.Fail(fmt.Sprintf("'%s.%s' of %#v is not equal to %#v", parentKey, key, v, expectedValue))
}

func (a *aggregateResponseAssert) hasString(parentMap map[string]interface{},
	parentKey, key string, expectedValue string,
) bool {
	v, exist := parentMap[key]
	if !exist {
		return a.assert.Fail(fmt.Sprintf("'%s' does not have '%s'\n%#v", parentKey, key, parentMap))
	}
	if v == nil {
		return a.assert.Fail(fmt.Sprintf("'%s.%s' is nil", parentKey, key))
	}
	if v != expectedValue {
		return a.assert.Fail(fmt.Sprintf("'%s.%s' of %#v is not equal to %#v", parentKey, key, v, expectedValue))
	}
	return true
}

func (a *aggregateResponseAssert) hasNil(parentMap map[string]interface{},
	parentKey, key string,
) bool {
	v, exist := parentMap[key]
	if !exist {
		return a.assert.Fail(fmt.Sprintf("'%s' does not have '%s'\n%#v", parentKey, key, parentMap))
	}
	if v != nil {
		return a.assert.Fail(fmt.Sprintf("'%s.%s' is not nil", parentKey, key))
	}
	return true
}

func (a *aggregateResponseAssert) hasArray(parentMap map[string]interface{},
	parentKey, key string, expectedValue []interface{},
) bool {
	v, exist := parentMap[key]
	if !exist {
		return a.assert.Fail(fmt.Sprintf("'%s' does not have '%s'\n%#v", parentKey, key, parentMap))
	}
	if !a.assert.Equal(expectedValue, v) {
		return a.assert.Fail(fmt.Sprintf("'%s.%s' of %#v is not equal to %#v", parentKey, key, v, expectedValue))
	}
	return true
}

func combinedAssert(assertResults ...bool) bool {
	for _, assertResult := range assertResults {
		if !assertResult {
			return false
		}
	}
	return true
}
