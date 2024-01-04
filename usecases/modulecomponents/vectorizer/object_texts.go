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

package vectorizer

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/fatih/camelcase"
	"github.com/weaviate/weaviate/entities/moduletools"
)

type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizePropertyName(propertyName string) bool
	VectorizeClassName() bool
}

type ObjectVectorizer struct{}

func New() *ObjectVectorizer {
	return &ObjectVectorizer{}
}

func (v *ObjectVectorizer) TextsOrVector(ctx context.Context, className string,
	schema interface{}, objDiff *moduletools.ObjectDiff,
	icheck ClassSettings,
) (string, []float32, error) {
	vectorize := objDiff == nil || objDiff.GetVec() == nil

	var corpi []string
	if icheck.VectorizeClassName() {
		corpi = append(corpi, v.camelCaseToLower(className))
	}
	if schema != nil {
		schemamap := schema.(map[string]interface{})
		for _, prop := range v.sortStringKeys(schemamap) {
			if !icheck.PropertyIndexed(prop) {
				continue
			}

			appended := false
			switch val := schemamap[prop].(type) {
			case []string:
				for _, elem := range val {
					appended = v.appendPropIfText(icheck, &corpi, prop, elem) || appended
				}
			case []interface{}:
				for _, elem := range val {
					appended = v.appendPropIfText(icheck, &corpi, prop, elem) || appended
				}
			default:
				appended = v.appendPropIfText(icheck, &corpi, prop, val)
			}

			vectorize = vectorize || (appended && objDiff != nil && objDiff.IsChangedProp(prop))
		}
	}
	if len(corpi) == 0 {
		// fall back to using the class name
		corpi = append(corpi, v.camelCaseToLower(className))
	}

	// no property was changed, old vector can be used
	if !vectorize {
		return "", objDiff.GetVec(), nil
	}

	text := strings.Join(corpi, " ")
	return text, nil, nil
}

func (v *ObjectVectorizer) sortStringKeys(schemaMap map[string]interface{}) []string {
	keys := make([]string, 0, len(schemaMap))
	for k := range schemaMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (v *ObjectVectorizer) appendPropIfText(icheck ClassSettings, list *[]string, propName string,
	value interface{},
) bool {
	valueString, ok := value.(string)
	if ok {
		if icheck.VectorizePropertyName(propName) {
			// use prop and value
			*list = append(*list, strings.ToLower(
				fmt.Sprintf("%s %s", v.camelCaseToLower(propName), valueString)))
		} else {
			*list = append(*list, strings.ToLower(valueString))
		}
		return true
	}
	return false
}

func (v *ObjectVectorizer) camelCaseToLower(in string) string {
	parts := camelcase.Split(in)
	var sb strings.Builder
	for i, part := range parts {
		if part == " " {
			continue
		}

		if i > 0 {
			sb.WriteString(" ")
		}

		sb.WriteString(strings.ToLower(part))
	}

	return sb.String()
}
