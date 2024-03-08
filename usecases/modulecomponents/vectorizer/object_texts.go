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
		corpi = append(corpi, v.camelCaseToLower(object.Class))
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
					lowerPropertyName := v.camelCaseToLower(propName)

					for i := range val {
						str := strings.ToLower(val[i])
						if isTitleProperty {
							titlePropertyValue = append(titlePropertyValue, str)
						}
						if isNameVectorizable {
							str = fmt.Sprintf("%s %s", lowerPropertyName, str)
						}
						corpi = append(corpi, str)
					}
				}
			case string:
				str := strings.ToLower(val)
				if isTitleProperty {
					titlePropertyValue = append(titlePropertyValue, str)
				}
				if icheck.VectorizePropertyName(propName) {
					str = fmt.Sprintf("%s %s", v.camelCaseToLower(propName), str)
				}
				corpi = append(corpi, str)
			default:
				// properties that are not part of the object
			}
		}
	}
	if len(corpi) == 0 {
		// fall back to using the class name
		corpi = append(corpi, v.camelCaseToLower(object.Class))
	}

	return strings.Join(corpi, " "), strings.Join(titlePropertyValue, " ")
}
