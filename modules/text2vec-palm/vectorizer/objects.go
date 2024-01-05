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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-palm/ent"
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
	Vectorize(ctx context.Context, input []string,
		config ent.VectorizationConfig, titlePropertyValue string) (*ent.VectorizationResult, error)
	VectorizeQuery(ctx context.Context, input []string,
		config ent.VectorizationConfig) (*ent.VectorizationResult, error)
}

// IndexCheck returns whether a property of a class should be indexed
type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizePropertyName(propertyName string) bool
	VectorizeClassName() bool
	ApiEndpoint() string
	ProjectID() string
	ModelID() string
	TitleProperty() string
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

func appendTitlePropIfText(icheck ClassSettings, list *[]string, propName string,
	value interface{},
) bool {
	if icheck.TitleProperty() == propName {
		return appendPropIfText(icheck, list, propName, value)
	}
	return false
}

func (v *Vectorizer) object(ctx context.Context, className string,
	schema interface{}, objDiff *moduletools.ObjectDiff, icheck ClassSettings,
) ([]float32, error) {
	vectorize := objDiff == nil || objDiff.GetVec() == nil

	var titlePropertyValue []string
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
					appendTitlePropIfText(icheck, &titlePropertyValue, prop, elem)
				}
			case []interface{}:
				for _, elem := range val {
					appended = appendPropIfText(icheck, &corpi, prop, elem) || appended
					appendTitlePropIfText(icheck, &titlePropertyValue, prop, elem)
				}
			default:
				appended = appendPropIfText(icheck, &corpi, prop, val)
				appendTitlePropIfText(icheck, &titlePropertyValue, prop, val)
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

	text := []string{strings.Join(corpi, " ")}
	titleProperty := strings.Join(titlePropertyValue, " ")
	res, err := v.client.Vectorize(ctx, text, ent.VectorizationConfig{
		ApiEndpoint: icheck.ApiEndpoint(),
		ProjectID:   icheck.ProjectID(),
		Model:       icheck.ModelID(),
	}, titleProperty)
	if err != nil {
		return nil, err
	}
	if len(res.Vectors) == 0 {
		return nil, fmt.Errorf("no vectors generated")
	}

	if len(res.Vectors) > 1 {
		return v.CombineVectors(res.Vectors), nil
	}
	return res.Vectors[0], nil
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
