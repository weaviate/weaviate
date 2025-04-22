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

package gqlparser

import (
	"strconv"

	"github.com/tailor-inc/graphql/language/ast"
)

func GetValueAsString(f *ast.ObjectField) *string {
	asString, ok := f.Value.GetValue().(string)
	if ok {
		return &asString
	}
	return nil
}

func GetValueAsStringOrEmpty(f *ast.ObjectField) string {
	if asString := GetValueAsString(f); asString != nil {
		return *asString
	}
	return ""
}

func GetValueAsFloat64(f *ast.ObjectField) *float64 {
	asString := f.Value.GetValue().(string)
	if asFloat64, err := strconv.ParseFloat(asString, 64); err == nil {
		return &asFloat64
	}
	return nil
}

func GetValueAsInt64(f *ast.ObjectField) *int64 {
	asString := f.Value.GetValue().(string)
	if asInt64, err := strconv.ParseInt(asString, 10, 64); err == nil {
		return &asInt64
	}
	return nil
}

func GetValueAsInt(f *ast.ObjectField) *int {
	asString := f.Value.GetValue().(string)
	if asInt, err := strconv.Atoi(asString); err == nil {
		return &asInt
	}
	return nil
}

func GetValueAsStringArray(f *ast.ObjectField) []string {
	switch vals := f.Value.GetValue().(type) {
	case string:
		return []string{vals}
	case []ast.Value:
		var stopSequences []string
		for _, val := range vals {
			stopSequences = append(stopSequences, val.GetValue().(string))
		}
		return stopSequences
	default:
		return nil
	}
}

func GetValueAsStringPtrArray(f *ast.ObjectField) []*string {
	switch vals := f.Value.GetValue().(type) {
	case string:
		return []*string{&vals}
	case []ast.Value:
		var values []*string
		for _, val := range vals {
			value := val.GetValue().(string)
			values = append(values, &value)
		}
		return values
	default:
		return nil
	}
}

func GetValueAsBool(f *ast.ObjectField) *bool {
	asBool, ok := f.Value.GetValue().(bool)
	if ok {
		return &asBool
	}
	return nil
}

func GetValueAsBoolOrFalse(f *ast.ObjectField) bool {
	if asBool := GetValueAsBool(f); asBool != nil {
		return *asBool
	}
	return false
}
