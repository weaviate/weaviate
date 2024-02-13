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
	comp moduletools.VectorizablePropsComparator, icheck ClassSettings, targetVector string,
) (string, []float32) {
	text, _, vector := v.TextsOrVectorWithTitleProperty(ctx, className, comp, icheck, "", targetVector)
	return text, vector
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

func (v *ObjectVectorizer) TextsOrVectorWithTitleProperty(ctx context.Context, className string,
	comp moduletools.VectorizablePropsComparator, icheck ClassSettings, titlePopertyName string,
	targetVector string,
) (string, string, []float32) {
	prevVector := comp.PrevVector()
	if targetVector != "" {
		prevVector = comp.PrevVectorForName(targetVector)
	}

	vectorize := prevVector == nil

	var titlePropertyValue []string
	var corpi []string

	if icheck.VectorizeClassName() {
		corpi = append(corpi, v.camelCaseToLower(className))
	}

	it := comp.PropsIterator()
	for propName, value, ok := it.Next(); ok; propName, value, ok = it.Next() {
		if !icheck.PropertyIndexed(propName) {
			continue
		}

		switch typed := value.(type) {
		case string:
			vectorize = vectorize || comp.IsChanged(propName)
			isTitleProperty := propName == titlePopertyName

			str := strings.ToLower(typed)
			if isTitleProperty {
				titlePropertyValue = append(titlePropertyValue, str)
			}
			if icheck.VectorizePropertyName(propName) {
				str = fmt.Sprintf("%s %s", v.camelCaseToLower(propName), str)
			}
			corpi = append(corpi, str)

		case []string:
			vectorize = vectorize || comp.IsChanged(propName)

			if len(typed) > 0 {
				isNameVectorizable := icheck.VectorizePropertyName(propName)
				lowerPropertyName := v.camelCaseToLower(propName)
				isTitleProperty := propName == titlePopertyName

				for i := range typed {
					str := strings.ToLower(typed[i])
					if isTitleProperty {
						titlePropertyValue = append(titlePropertyValue, str)
					}
					if isNameVectorizable {
						str = fmt.Sprintf("%s %s", lowerPropertyName, str)
					}
					corpi = append(corpi, str)
				}
			}

		case nil:
			vectorize = vectorize || comp.IsChanged(propName)
		}
	}

	// no property was changed, old vector can be used
	if !vectorize {
		return "", "", prevVector
	}

	if len(corpi) == 0 {
		// fall back to using the class name
		corpi = append(corpi, v.camelCaseToLower(className))
	}

	text := strings.Join(corpi, " ")
	if titlePopertyName == "" {
		return text, "", nil
	}
	titlePropertyVal := strings.Join(titlePropertyValue, " ")
	return text, titlePropertyVal, nil
}
