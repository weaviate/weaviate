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

package modules

import (
	"context"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

func reVectorize(ctx context.Context, cfg moduletools.ClassConfig, mod modulecapabilities.Vectorizer, object *models.Object, class *models.Class, findObjectFn modulecapabilities.FindObjectFn) (bool, models.AdditionalProperties, []float32, models.Vectors) {
	textProps, mediaProps, err := mod.VectorizedProperties(cfg)
	if err != nil {
		return true, nil, nil, nil
	}

	type compareProps struct {
		Name    string
		IsArray bool
	}
	propsToCmpare := make([]compareProps, 0)

	for _, prop := range class.Properties {
		if len(prop.DataType) > 1 {
			continue // multi cref
		}

		if prop.ModuleConfig != nil {
			if modConfig, ok := prop.ModuleConfig.(map[string]interface{})[class.Vectorizer]; ok {
				if skip, ok2 := modConfig.(map[string]interface{})["skip"]; ok2 && skip == true {
					continue
				}
			}
		}

		if (prop.DataType[0] == schema.DataTypeText.String() || prop.DataType[0] == schema.DataTypeTextArray.String()) && textProps {
			propsToCmpare = append(propsToCmpare, compareProps{Name: prop.Name, IsArray: schema.IsArrayDataType(prop.DataType)})
			continue
		}

		for _, mediaProp := range mediaProps {
			if mediaProp == prop.Name {
				propsToCmpare = append(propsToCmpare, compareProps{Name: prop.Name, IsArray: schema.IsArrayDataType(prop.DataType)})
			}
		}
	}

	// if no properties to compare, we can skip the comparison. Return vectors of old object if present
	if len(propsToCmpare) == 0 {
		oldObject, err := findObjectFn(ctx, class.Class, object.ID, nil, additional.Properties{}, object.Tenant)
		if err != nil || oldObject == nil {
			return true, nil, nil, nil
		}
		return false, oldObject.AdditionalProperties, oldObject.Vector, oldObject.Vectors
	}

	returnProps := make(search.SelectProperties, 0, len(propsToCmpare))
	for _, prop := range propsToCmpare {
		returnProps = append(returnProps, search.SelectProperty{Name: prop.Name, IsPrimitive: true, IsObject: false})
	}
	oldObject, err := findObjectFn(ctx, class.Class, object.ID, returnProps, additional.Properties{}, object.Tenant)
	if err != nil || oldObject == nil {
		return true, nil, nil, nil
	}
	oldProps := oldObject.Schema.(models.Object).Properties.(map[string]interface{})
	newProps := object.Properties.(map[string]interface{})
	for _, propStruct := range propsToCmpare {
		valNew, isPresentNew := newProps[propStruct.Name]
		valOld, isPresentOld := oldProps[propStruct.Name]

		if isPresentNew != isPresentOld {
			return true, nil, nil, nil
		}

		if !isPresentNew {
			continue
		}

		if propStruct.IsArray {
			for i, val := range valOld.([]string) {
				if val != valNew.([]string)[i] {
					return true, nil, nil, nil
				}
			}
		} else {
			if valOld != valNew {
				return true, nil, nil, nil
			}
		}
	}

	return false, oldObject.AdditionalProperties, oldObject.Vector, oldObject.Vectors
}
