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
	"github.com/weaviate/weaviate/entities/storobj"
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

// renderSourceValue renders a value as a string, so the same value arriving in
// different Go types compares equal.
func renderSourceValue(v any) string {
	switch val := v.(type) {
	case time.Time:
		return val.Format(time.RFC3339)
	case string:
		// A date comes back from disk as a string but arrives in the request as a
		// time.Time. Format both as RFC3339 so the same instant compares equal.
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return t.Format(time.RFC3339)
		}
		return val
	case map[string]any:
		// Disk reads turn geo/phone shaped maps into structs. Convert the same way
		// here so both sides round floats and drop empty fields identically.
		if shaped, err := storobj.ShapeConvertMap(val); err == nil {
			return renderJSON(shaped)
		}
		return renderJSON(val)
	case []any, []map[string]any, []float64, []int, []int64, []bool, []string, []time.Time,
		*models.GeoCoordinates, *models.PhoneNumber:
		// Arrays, objects, geo and phone values are compared as JSON. []time.Time
		// marshals to the same strings a date array reads back from disk.
		// Single dates compare at second precision because that is what gets
		// vectorized; date arrays keep sub-seconds, which at worst re-vectorizes
		// once too often.
		return renderJSON(val)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func renderJSON(v any) string {
	if b, err := json.Marshal(v); err == nil {
		return string(b)
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
		Name       string
		IsArray    bool
		IsBlobHash bool
		// Generic marks a non-text source property, compared via renderSourceValue.
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

		// skip:true only counts when no source properties are set. With source
		// properties, being on the list alone decides what gets vectorized.
		if sourcePropsSet == nil {
			if skip, ok := cfg.Property(prop.Name)["skip"]; ok && skip == true {
				continue
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
			propsToCompare = append(propsToCompare, compareProps{
				Name:       prop.Name,
				IsArray:    schema.IsArrayDataType(prop.DataType),
				IsBlobHash: schema.IsBlobHashDataType(prop.DataType),
			})
			continue
		}

		// Blob values are vectorized like any other string, so compare them too;
		// for blobHash the stored value is a hash, so compare hashes. Modules that
		// do not vectorize text never see blobs, unless source properties list them.
		if (textProps || sourcePropsSet != nil) && schema.IsBlobLikeDataType(prop.DataType) {
			propsToCompare = append(propsToCompare, compareProps{
				Name:       prop.Name,
				IsBlobHash: schema.IsBlobHashDataType(prop.DataType),
			})
			continue
		}

		// With source properties set, non-text values (numbers, bools, dates,
		// objects and their arrays) get vectorized too, so compare them as well.
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
	oldProps := oldObject.Schema.(map[string]any)
	var newProps map[string]any
	if object.Properties == nil {
		newProps = make(map[string]any)
	} else {
		newProps = object.Properties.(map[string]any)
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
			// Compare as strings so different Go types for the same value match.
			if renderSourceValue(valOld) != renderSourceValue(valNew) {
				return true, nil
			}
			continue
		}

		if propStruct.IsArray {
			// empty strings do not have type information saved with them - the new value can also come from disk if
			// an update happens
			if _, ok := valOld.([]any); ok && len(valOld.([]any)) == 0 {
				valOld = []string{}
			}
			if _, ok := valNew.([]any); ok && len(valNew.([]any)) == 0 {
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
			// For BlobHash properties, the stored (old) value is a hash while
			// the incoming (new) value is the raw base64 data. Hash the new
			// value so we compare hashes consistently.
			if propStruct.IsBlobHash {
				if newStr, ok := valNew.(string); ok {
					if !schema.IsLikelySHA256Hash(newStr) {
						valNew = schema.HashBlob(newStr)
					}
				}
			}
			if valOld != valNew {
				return true, nil
			}
		}
	}
	return false, oldObject
}
