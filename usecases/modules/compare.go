//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modules

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

func reVectorize(ctx context.Context,
	cfg moduletools.ClassConfig,
	mod modulecapabilities.Vectorizer[[]float32],
	object *models.Object,
	class *models.Class,
	sourceProperties []string,
	targetVector string,
	findObjectFn modulecapabilities.FindObjectFn,
	reVectorizeDisabled bool,
) (bool, models.AdditionalProperties, []float32, error) {
	if reVectorizeDisabled {
		return true, nil, nil, nil
	}

	shouldReVectorize, oldObject := reVectorizeEmbeddings(ctx, cfg, mod, object, class, sourceProperties, findObjectFn)
	if shouldReVectorize {
		return shouldReVectorize, nil, nil, nil
	}

	if targetVector == "" {
		return false, oldObject.AdditionalProperties, oldObject.Vector, nil
	} else {
		vector, err := getVector(oldObject.Vectors[targetVector])
		if err != nil {
			return false, nil, nil, fmt.Errorf("get vector: %w", err)
		}
		return false, oldObject.AdditionalProperties, vector, nil
	}
}

func getVector(v models.Vector) ([]float32, error) {
	switch vector := v.(type) {
	case nil:
		return nil, nil
	case []float32:
		return vector, nil
	default:
		return nil, fmt.Errorf("unrecognized vector type: %T", v)
	}
}

func reVectorizeMulti(ctx context.Context,
	cfg moduletools.ClassConfig,
	mod modulecapabilities.Vectorizer[[][]float32],
	object *models.Object,
	class *models.Class,
	sourceProperties []string,
	targetVector string,
	findObjectFn modulecapabilities.FindObjectFn,
	reVectorizeDisabled bool,
) (bool, models.AdditionalProperties, [][]float32, error) {
	if reVectorizeDisabled {
		return true, nil, nil, nil
	}

	shouldReVectorize, oldObject := reVectorizeEmbeddings(ctx, cfg, mod, object, class, sourceProperties, findObjectFn)
	if shouldReVectorize {
		return shouldReVectorize, nil, nil, nil
	}

	if targetVector == "" {
		return false, oldObject.AdditionalProperties, nil, nil
	} else {
		multiVector, err := getMultiVector(oldObject.Vectors[targetVector])
		if err != nil {
			return false, nil, nil, fmt.Errorf("get multi vector: %w", err)
		}
		return false, oldObject.AdditionalProperties, multiVector, nil
	}
}

func getMultiVector(v models.Vector) ([][]float32, error) {
	switch vector := v.(type) {
	case nil:
		return nil, nil
	case [][]float32:
		return vector, nil
	default:
		return nil, fmt.Errorf("unrecognized multi vector type: %T", v)
	}
}

// renderSourceValue renders a value the way the corpus builder does, so equal
// logical values in different Go representations compare equal.
func renderSourceValue(v any) string {
	switch val := v.(type) {
	case time.Time:
		return val.Format(time.RFC3339)
	case string:
		// Normalize a date to RFC3339 (the corpus form) so the same instant compares
		// equal whether stored as a string or supplied as time.Time, ignoring sub-seconds.
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return t.Format(time.RFC3339)
		}
		return val
	case map[string]any, []any, []map[string]any, []float64, []int, []int64, []bool, []string:
		// Composites: JSON is deterministic and matches the corpus builder.
		if b, err := json.Marshal(val); err == nil {
			return string(b)
		}
	}
	return fmt.Sprintf("%v", v)
}

func reVectorizeEmbeddings[T dto.Embedding](ctx context.Context,
	cfg moduletools.ClassConfig,
	mod modulecapabilities.Vectorizer[T],
	object *models.Object,
	class *models.Class,
	sourceProperties []string,
	findObjectFn modulecapabilities.FindObjectFn,
) (bool, *search.Result) {
	textProps, mediaProps, err := mod.VectorizableProperties(cfg)
	if err != nil {
		return true, nil
	}

	type compareProps struct {
		Name    string
		IsArray bool
		// Generic: a non-text source property compared via its corpus string form.
		Generic bool
	}
	propsToCompare := make([]compareProps, 0)

	var sourcePropsSet map[string]struct{} = nil
	if len(sourceProperties) > 0 {
		sourcePropsSet = make(map[string]struct{}, len(sourceProperties))
		for _, sourceProp := range sourceProperties {
			sourcePropsSet[sourceProp] = struct{}{}
		}
	}
	mediaPropsSet := make(map[string]struct{}, len(mediaProps))
	for _, mediaProp := range mediaProps {
		mediaPropsSet[mediaProp] = struct{}{}
	}

	for _, prop := range class.Properties {
		if len(prop.DataType) > 1 {
			continue // multi cref
		}

		// for named vectors with explicit source properties, skip if not in the list
		if sourcePropsSet != nil {
			if _, ok := sourcePropsSet[prop.Name]; !ok {
				continue
			}
		}

		// Honor the per-property skip flag only without source properties; with them
		// membership decides vectorization (PropertyIndexed ignores skip), so must we.
		if sourcePropsSet == nil && prop.ModuleConfig != nil {
			if modConfig, ok := prop.ModuleConfig.(map[string]interface{})[class.Vectorizer]; ok {
				if skip, ok2 := modConfig.(map[string]interface{})["skip"]; ok2 && skip == true {
					continue
				}
			}
		}

		if prop.DataType[0] == schema.DataTypeText.String() && textProps {
			propsToCompare = append(propsToCompare, compareProps{Name: prop.Name, IsArray: false})
			continue
		}

		if prop.DataType[0] == schema.DataTypeTextArray.String() && textProps {
			propsToCompare = append(propsToCompare, compareProps{Name: prop.Name, IsArray: true})
			continue
		}

		if _, ok := mediaPropsSet[prop.Name]; ok {
			propsToCompare = append(propsToCompare, compareProps{Name: prop.Name, IsArray: schema.IsArrayDataType(prop.DataType)})
			continue
		}

		// A blob is a base64 string, which the corpus vectorizes like any indexed
		// string, so a changed blob must re-vectorize. Compare as a string.
		// TODO: also handle schema.DataTypeBlobHash when syncing forward to versions with it.
		if schema.DataType(prop.DataType[0]) == schema.DataTypeBlob {
			propsToCompare = append(propsToCompare, compareProps{Name: prop.Name})
			continue
		}

		// With source properties set, the corpus also vectorizes non-text types
		// (number/int/bool/date/object + array variants); compare those generically.
		if sourcePropsSet != nil {
			switch schema.DataType(prop.DataType[0]) {
			case schema.DataTypeInt, schema.DataTypeNumber, schema.DataTypeBoolean, schema.DataTypeDate,
				schema.DataTypeIntArray, schema.DataTypeNumberArray, schema.DataTypeBooleanArray, schema.DataTypeDateArray,
				schema.DataTypeObject, schema.DataTypeObjectArray:
				propsToCompare = append(propsToCompare, compareProps{Name: prop.Name, Generic: true})
			default:
			}
		}
	}

	// if no properties to compare, we can skip the comparison. Return vectors of old object if present
	if len(propsToCompare) == 0 {
		oldObject, err := findObjectFn(ctx, class.Class, object.ID, nil, additional.Properties{}, object.Tenant)
		if err != nil || oldObject == nil {
			return true, nil
		}
		return false, oldObject
	}

	returnProps := make(search.SelectProperties, 0, len(propsToCompare))
	for _, prop := range propsToCompare {
		returnProps = append(returnProps, search.SelectProperty{Name: prop.Name, IsPrimitive: true, IsObject: false})
	}
	oldObject, err := findObjectFn(ctx, class.Class, object.ID, returnProps, additional.Properties{}, object.Tenant)
	if err != nil || oldObject == nil {
		return true, nil
	}
	oldProps := oldObject.Schema.(map[string]interface{})
	var newProps map[string]interface{}
	if object.Properties == nil {
		newProps = make(map[string]interface{})
	} else {
		newProps = object.Properties.(map[string]interface{})
	}
	for _, propStruct := range propsToCompare {
		valNew, isPresentNew := newProps[propStruct.Name]
		valOld, isPresentOld := oldProps[propStruct.Name]

		if isPresentNew != isPresentOld {
			return true, nil
		}

		if !isPresentNew {
			continue
		}

		if propStruct.Generic {
			// Compare via the corpus rendering, not the raw Go value (see renderSourceValue).
			if renderSourceValue(valOld) != renderSourceValue(valNew) {
				return true, nil
			}
			continue
		}

		if propStruct.IsArray {
			// empty strings do not have type information saved with them - the new value can also come from disk if
			// an update happens
			if _, ok := valOld.([]interface{}); ok && len(valOld.([]interface{})) == 0 {
				valOld = []string{}
			}
			if _, ok := valNew.([]interface{}); ok && len(valNew.([]interface{})) == 0 {
				valNew = []string{}
			}

			if len(valOld.([]string)) != len(valNew.([]string)) {
				return true, nil
			}
			for i, val := range valOld.([]string) {
				if val != valNew.([]string)[i] {
					return true, nil
				}
			}
		} else {
			if valOld != valNew {
				return true, nil
			}
		}
	}
	return false, oldObject
}
