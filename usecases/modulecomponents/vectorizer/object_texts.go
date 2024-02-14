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

type ClassSettingDefaults struct {
	DefaultVectorizeClassName     bool
	DefaultPropertyIndexed        bool
	DefaultVectorizePropertyName  bool
	DefaultLowerCasePropertyValue bool
}

type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizePropertyName(propertyName string) bool
	VectorizeClassName() bool
	LowerCasePropertyValue() bool
}

type BaseClassSettings struct {
	cfg      moduletools.ClassConfig
	defaults *ClassSettingDefaults
}

func NewBaseClassSettings(c moduletools.ClassConfig, d *ClassSettingDefaults) *BaseClassSettings {
	return &BaseClassSettings{cfg: c, defaults: d}
}

func (cs *BaseClassSettings) PropertyIndexed(propName string) bool {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return cs.defaults.DefaultPropertyIndexed
	}

	vcn, ok := cs.cfg.Property(propName)["skip"]
	if !ok {
		return cs.defaults.DefaultPropertyIndexed
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return cs.defaults.DefaultPropertyIndexed
	}

	return !asBool
}

func (cs *BaseClassSettings) VectorizePropertyName(propName string) bool {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return cs.defaults.DefaultVectorizePropertyName
	}
	vcn, ok := cs.cfg.Property(propName)["vectorizePropertyName"]
	if !ok {
		return cs.defaults.DefaultVectorizePropertyName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return cs.defaults.DefaultVectorizePropertyName
	}

	return asBool
}

func (cs *BaseClassSettings) VectorizeClassName() bool {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return cs.defaults.DefaultVectorizeClassName
	}

	vcn, ok := cs.cfg.Class()["vectorizeClassName"]
	if !ok {
		return cs.defaults.DefaultVectorizeClassName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return cs.defaults.DefaultVectorizeClassName
	}

	return asBool
}

func (cs *BaseClassSettings) LowerCasePropertyValue() bool {
	if cs.cfg == nil {
		return cs.defaults.DefaultLowerCasePropertyValue
	}

	lpv, ok := cs.cfg.Class()["lowerCasePropertyValue"]
	if !ok {
		return cs.defaults.DefaultLowerCasePropertyValue
	}

	asBool, ok := lpv.(bool)
	if !ok {
		return cs.defaults.DefaultLowerCasePropertyValue
	}

	return asBool
}

type ObjectVectorizer struct{}

func New() *ObjectVectorizer {
	return &ObjectVectorizer{}
}

func (v *ObjectVectorizer) TextsOrVector(ctx context.Context, className string,
	comp moduletools.VectorizablePropsComparator, icheck ClassSettings,
) (string, []float32) {
	vectorize := comp.PrevVector() == nil

	var corpi []string

	if icheck.VectorizeClassName() {
		corpi = append(corpi, v.camelCaseToLower(className))
	}

	toLowerCase := func(s string) string {
		if icheck.LowerCasePropertyValue() {
			return strings.ToLower(s)
		}
		return s
	}
	it := comp.PropsIterator()
	for propName, value, ok := it.Next(); ok; propName, value, ok = it.Next() {
		if !icheck.PropertyIndexed(propName) {
			continue
		}

		switch typed := value.(type) {
		case string:
			vectorize = vectorize || comp.IsChanged(propName)

			str := toLowerCase(typed)
			if icheck.VectorizePropertyName(propName) {
				str = fmt.Sprintf("%s %s", v.camelCaseToLower(propName), str)
			}
			corpi = append(corpi, str)

		case []string:
			vectorize = vectorize || comp.IsChanged(propName)

			if len(typed) > 0 {
				isVectorizable := icheck.VectorizePropertyName(propName)
				lowerPropertyName := v.camelCaseToLower(propName)
				for i := range typed {
					str := toLowerCase(typed[i])
					if isVectorizable {
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
		return "", comp.PrevVector()
	}

	if len(corpi) == 0 {
		// fall back to using the class name
		corpi = append(corpi, v.camelCaseToLower(className))
	}

	text := strings.Join(corpi, " ")
	return text, nil
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
