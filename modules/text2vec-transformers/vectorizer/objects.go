//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package vectorizer

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/fatih/camelcase"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/modules/text2vec-transformers/ent"
)

type Vectorizer struct {
	client Client
}

func New(client Client) *Vectorizer {
	return &Vectorizer{
		client: client,
	}
}

type Client interface {
	VectorizeObject(ctx context.Context, input string,
		cfg ent.VectorizationConfig) (*ent.VectorizationResult, error)
	VectorizeQuery(ctx context.Context, input string,
		cfg ent.VectorizationConfig) (*ent.VectorizationResult, error)
}

// IndexCheck returns whether a property of a class should be indexed
type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizeClassName() bool
	VectorizePropertyName(propertyName string) bool
	PoolingStrategy() string
}

func sortStringKeys(schema_map map[string]interface{}) []string {
	keys := make([]string, 0, len(schema_map))
	for k := range schema_map {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object,
	objDiff *moduletools.ObjectDiff, settings ClassSettings,
) error {
	vec, err := v.object(ctx, object.Class, object.Properties, objDiff, settings)
	if err != nil {
		return err
	}

	object.Vector = vec
	return nil
}

func appendPropIfText(icheck ClassSettings, list *[]string, propName string,
	value interface{},
) bool {
	valueString, ok := value.(string)
	if ok {
		if icheck.VectorizePropertyName(propName) {
			// use prop and value
			*list = append(*list, strings.ToLower(
				fmt.Sprintf("%s %s", camelCaseToLower(propName), valueString)))
		} else {
			*list = append(*list, strings.ToLower(valueString))
		}
		return true
	}
	return false
}

func (v *Vectorizer) object(ctx context.Context, className string,
	schema interface{}, objDiff *moduletools.ObjectDiff, icheck ClassSettings,
) ([]float32, error) {
	vectorize := objDiff == nil || objDiff.GetVec() == nil

	var corpi []string
	if icheck.VectorizeClassName() {
		corpi = append(corpi, camelCaseToLower(className))
	}
	if schema != nil {
		schemamap := schema.(map[string]interface{})
		for _, prop := range sortStringKeys(schemamap) {
			if !icheck.PropertyIndexed(prop) {
				continue
			}

			appended := false
			switch val := schemamap[prop].(type) {
			case []string:
				for _, elem := range val {
					appended = appendPropIfText(icheck, &corpi, prop, elem) || appended
				}
			case []interface{}:
				for _, elem := range val {
					appended = appendPropIfText(icheck, &corpi, prop, elem) || appended
				}
			default:
				appended = appendPropIfText(icheck, &corpi, prop, val)
			}

			vectorize = vectorize || (appended && objDiff != nil && objDiff.IsChangedProp(prop))
		}
	}
	if len(corpi) == 0 {
		// fall back to using the class name
		corpi = append(corpi, camelCaseToLower(className))
	}

	// no property was changed, old vector can be used
	if !vectorize {
		return objDiff.GetVec(), nil
	}

	text := strings.Join(corpi, " ")
	res, err := v.client.VectorizeObject(ctx, text, ent.VectorizationConfig{
		PoolingStrategy: icheck.PoolingStrategy(),
	})
	if err != nil {
		return nil, err
	}

	return res.Vector, nil
}

func camelCaseToLower(in string) string {
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
