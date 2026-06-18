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
	"reflect"
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

// renderSourceValue renders a non-text source-property value to a stable string
// so that equal values stored and supplied in different Go representations
// (e.g. an RFC3339 date string vs time.Time, int64 vs float64, []interface{} vs
// []float64) compare as equal and don't force an unnecessary re-vectorization.
//
// Composite values (maps, slices) are JSON-marshaled: that is deterministic
// (encoding/json sorts map keys), collapses equivalent representations, and
// matches how the corpus builder renders maps. Dates use RFC3339 and scalars use
// fmt, matching the corpus builder.
func renderSourceValue(v interface{}) string {
	if t, ok := v.(time.Time); ok {
		return t.Format(time.RFC3339)
	}
	if k := reflect.ValueOf(v).Kind(); k == reflect.Map || k == reflect.Slice || k == reflect.Array {
		if b, err := json.Marshal(v); err == nil {
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
		// Generic marks a non-text source property (number, int, boolean, date,
		// object, or an array variant) that the corpus builder vectorizes when
		// explicit source properties are configured. It is compared by rendering
		// to the corpus string form rather than via the text-specific path.
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

		// The per-property skip flag is honored only when there are no explicit
		// source properties. With source properties set, membership in that list
		// governs what gets vectorized (PropertyIndexed ignores skip), so the
		// comparator must ignore skip too -- otherwise a skipped-but-source-listed
		// property would be vectorized yet never compared (a stale vector).
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

		// With explicit source properties configured, the corpus builder also
		// vectorizes the non-text source properties it understands (see
		// object_texts.go: numbers, ints, booleans, dates, objects, and their
		// array variants). A change to any of them must trigger re-vectorization,
		// so compare them generically. Types the corpus builder does NOT vectorize
		// (uuid, geo-coordinates, phone numbers, blobs, cross-references) are left
		// out so they never force an unnecessary re-vectorization.
		if sourcePropsSet != nil {
			switch schema.DataType(prop.DataType[0]) {
			case schema.DataTypeInt, schema.DataTypeNumber, schema.DataTypeBoolean, schema.DataTypeDate,
				schema.DataTypeIntArray, schema.DataTypeNumberArray, schema.DataTypeBooleanArray, schema.DataTypeDateArray,
				schema.DataTypeObject, schema.DataTypeObjectArray:
				propsToCompare = append(propsToCompare, compareProps{Name: prop.Name, Generic: true})
			default:
				// text/text[] are handled above; uuid, geo-coordinates, phone
				// numbers, blobs, and cross-references are not vectorized by the
				// corpus builder, so changes to them must not re-vectorize.
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
			// Compare the corpus contribution, not the raw Go value: the same
			// logical value can be stored (disk) and supplied (update) with
			// different representations (e.g. a date as an RFC3339 string vs
			// time.Time, an int as int64 vs float64, or an empty []interface{} vs a
			// typed empty slice). Rendering both the way the corpus builder
			// stringifies them keeps the decision aligned with what is actually
			// vectorized; equal renders imply an identical corpus, so this can
			// never leave a stale vector.
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
