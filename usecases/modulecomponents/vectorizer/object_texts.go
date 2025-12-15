//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package vectorizer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	entcfg "github.com/weaviate/weaviate/entities/config"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/fatih/camelcase"
)

type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizePropertyName(propertyName string) bool
	VectorizeClassName() bool
	Properties() []string
	LowerCaseInput() bool
}

type ObjectVectorizer struct{}

func New() *ObjectVectorizer {
	return &ObjectVectorizer{}
}

func (v *ObjectVectorizer) Texts(ctx context.Context, object *models.Object, icheck ClassSettings,
) (string, bool) {
	text, _, isEmpty := v.TextsWithTitleProperty(ctx, object, icheck, "")
	return text, isEmpty
}

func (v *ObjectVectorizer) separateCamelCase(in string, toLower bool) string {
	parts := camelcase.Split(in)
	var sb strings.Builder
	for i, part := range parts {
		if part == " " {
			continue
		}

		if i > 0 {
			sb.WriteString(" ")
		}

		if toLower {
			part = strings.ToLower(part)
		}

		sb.WriteString(part)
	}

	return sb.String()
}

func (v *ObjectVectorizer) TextsWithTitleProperty(ctx context.Context, object *models.Object, icheck ClassSettings, titlePropertyName string,
) (string, string, bool) {
	var corpi []string
	var titlePropertyValue []string

	hasSourceProperties := len(icheck.Properties()) > 0
	toLowerCase := icheck.LowerCaseInput()
	if entcfg.Enabled(os.Getenv("LOWERCASE_VECTORIZATION_INPUT")) {
		toLowerCase = true
	}
	if icheck.VectorizeClassName() {
		corpi = append(corpi, v.separateCamelCase(object.Class, toLowerCase))
	}
	if object.Properties != nil {
		propMap := object.Properties.(map[string]interface{})
		for _, propName := range moduletools.SortStringKeys(propMap) {
			if !icheck.PropertyIndexed(propName) {
				continue
			}
			isTitleProperty := propName == titlePropertyName
			isPropertyNameVectorizable := icheck.VectorizePropertyName(propName)

			switch val := propMap[propName].(type) {
			case []string:
				for i := range val {
					corpi, titlePropertyValue = v.insertValue(val[i], propName,
						toLowerCase, isPropertyNameVectorizable, isTitleProperty, corpi, titlePropertyValue)
				}
			case string:
				corpi, titlePropertyValue = v.insertValue(val, propName,
					toLowerCase, isPropertyNameVectorizable, isTitleProperty, corpi, titlePropertyValue)
			default:
				// do nothing
			}

			if hasSourceProperties {
				// get the values from additional property types only if source properties are present
				switch val := propMap[propName].(type) {
				case bool, int, int16, int32, int64, float32, float64:
					corpi, titlePropertyValue = v.insertValue(fmt.Sprintf("%v", val), propName,
						toLowerCase, isPropertyNameVectorizable, isTitleProperty, corpi, titlePropertyValue)
				case json.Number:
					corpi, titlePropertyValue = v.insertValue(val.String(), propName,
						toLowerCase, isPropertyNameVectorizable, isTitleProperty, corpi, titlePropertyValue)
				case time.Time:
					corpi, titlePropertyValue = v.insertValue(val.Format(time.RFC3339), propName,
						toLowerCase, isPropertyNameVectorizable, isTitleProperty, corpi, titlePropertyValue)
				case []any:
					if len(val) > 0 {
						if _, ok := val[0].(map[string]any); ok {
							in := v.marshalValue(val)
							corpi, titlePropertyValue = v.insertValue(in, propName,
								toLowerCase, isPropertyNameVectorizable, isTitleProperty, corpi, titlePropertyValue)
						} else {
							for i := range val {
								corpi, titlePropertyValue = v.insertValue(fmt.Sprintf("%v", val[i]), propName,
									toLowerCase, isPropertyNameVectorizable, isTitleProperty, corpi, titlePropertyValue)
							}
						}
					}
				case []float64:
					for i := range val {
						corpi, titlePropertyValue = v.insertValue(fmt.Sprintf("%v", val[i]), propName,
							toLowerCase, isPropertyNameVectorizable, isTitleProperty, corpi, titlePropertyValue)
					}
				case []int:
					for i := range val {
						corpi, titlePropertyValue = v.insertValue(fmt.Sprintf("%v", val[i]), propName,
							toLowerCase, isPropertyNameVectorizable, isTitleProperty, corpi, titlePropertyValue)
					}
				case []int64:
					for i := range val {
						corpi, titlePropertyValue = v.insertValue(fmt.Sprintf("%v", val[i]), propName,
							toLowerCase, isPropertyNameVectorizable, isTitleProperty, corpi, titlePropertyValue)
					}
				case []bool:
					for i := range val {
						corpi, titlePropertyValue = v.insertValue(fmt.Sprintf("%v", val[i]), propName,
							toLowerCase, isPropertyNameVectorizable, isTitleProperty, corpi, titlePropertyValue)
					}
				case map[string]any, []map[string]any:
					in := v.marshalValue(val)
					corpi, titlePropertyValue = v.insertValue(in, propName,
						toLowerCase, isPropertyNameVectorizable, isTitleProperty, corpi, titlePropertyValue)
				default:
					// get the values from additional property types only if source properties are present
				}
			}
		}
	}
	if !hasSourceProperties && len(corpi) == 0 {
		// fall back to using the class name
		corpi = append(corpi, v.separateCamelCase(object.Class, toLowerCase))
	}

	text := strings.Join(corpi, " ")
	titleProperty := strings.Join(titlePropertyValue, " ")
	isEmpty := len(text) == 0 && len(titleProperty) == 0
	return text, titleProperty, isEmpty
}

func (v *ObjectVectorizer) insertValue(
	val, propName string,
	toLowerCase, isPropertyNameVectorizable, isTitleProperty bool,
	corpi, titlePropertyValue []string,
) ([]string, []string) {
	val = v.getValue(val, propName, toLowerCase, isPropertyNameVectorizable)
	if isTitleProperty {
		titlePropertyValue = append(titlePropertyValue, val)
	} else {
		corpi = append(corpi, val)
	}
	return corpi, titlePropertyValue
}

func (v *ObjectVectorizer) getValue(val, propName string, toLowerCase, isPropertyNameVectorizable bool) string {
	if toLowerCase {
		val = strings.ToLower(val)
	}
	if isPropertyNameVectorizable {
		val = fmt.Sprintf("%s %s", v.separateCamelCase(propName, toLowerCase), val)
	}
	return val
}

func (v *ObjectVectorizer) marshalValue(in any) string {
	if val, err := json.Marshal(in); err == nil {
		return string(val)
	}
	return fmt.Sprintf("%v", in)
}
