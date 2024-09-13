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
	"strings"

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
) string {
	text, _ := v.TextsWithTitleProperty(ctx, object, icheck, "")
	return text
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

func (v *ObjectVectorizer) TextsWithTitleProperty(ctx context.Context, object *models.Object, icheck ClassSettings, titlePropertyName string,
) (string, string) {
	var corpi []string
	var titlePropertyValue []string

	if icheck.VectorizeClassName() {
		className := object.Class
		if icheck.LowerCaseInput() {
			className = v.camelCaseToLower(object.Class)
		}
		corpi = append(corpi, className)
	}
	if object.Properties != nil {
		propMap := object.Properties.(map[string]interface{})
		for _, propName := range moduletools.SortStringKeys(propMap) {
			if !icheck.PropertyIndexed(propName) {
				continue
			}
			isTitleProperty := propName == titlePropertyName
			isNameVectorizable := icheck.VectorizePropertyName(propName)

			switch val := propMap[propName].(type) {
			case []string:
				if len(val) > 0 {
					if icheck.LowerCaseInput() {
						propName = v.camelCaseToLower(propName)
					}

					for i := range val {
						str := strings.ToLower(val[i])
						if isTitleProperty {
							titlePropertyValue = append(titlePropertyValue, str)
						}
						if isNameVectorizable {
							str = fmt.Sprintf("%s %s", propName, str)
						}
						corpi = append(corpi, str)
					}
				}
			case string:
				if icheck.LowerCaseInput() {
					val = strings.ToLower(val)
					propName = v.camelCaseToLower(propName)
				}

				if isTitleProperty {
					titlePropertyValue = append(titlePropertyValue, val)
				}
				if icheck.VectorizePropertyName(propName) {
					val = fmt.Sprintf("%s %s", propName, val)
				}
				corpi = append(corpi, val)
			default:
				// properties that are not part of the object
			}
		}
	}
	if len(corpi) == 0 {
		// fall back to using the class name
		className := object.Class
		if icheck.LowerCaseInput() {
			className = v.camelCaseToLower(object.Class)
		}
		corpi = append(corpi, className)
	}

	return strings.Join(corpi, " "), strings.Join(titlePropertyValue, " ")
}
