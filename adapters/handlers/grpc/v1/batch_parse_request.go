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

package v1

import (
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/usecases/byteops"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

const BEACON_START = "weaviate://localhost/"

func sliceToInterface[T any](values []T) []interface{} {
	tmpArray := make([]interface{}, len(values))
	for k := range values {
		tmpArray[k] = values[k]
	}
	return tmpArray
}

func BatchFromProto(req *pb.BatchObjectsRequest, authorizedGetClass func(string, string) (*models.Class, error)) ([]*models.Object, map[int]int, map[int]error) {
	objectsBatch := req.Objects
	objs := make([]*models.Object, 0, len(objectsBatch))
	objOriginalIndex := make(map[int]int)
	objectErrors := make(map[int]error, len(objectsBatch))

	insertCounter := 0
	for i, obj := range objectsBatch {
		var props map[string]interface{}

		obj.Collection = schema.UppercaseClassName(obj.Collection)
		class, err := authorizedGetClass(obj.Collection, obj.Tenant)
		if err != nil {
			objectErrors[i] = err
			continue
		}

		if obj.Properties != nil {
			props = extractPrimitiveProperties(&pb.ObjectPropertiesValue{
				NonRefProperties:       obj.Properties.NonRefProperties,
				BooleanArrayProperties: obj.Properties.BooleanArrayProperties,
				NumberArrayProperties:  obj.Properties.NumberArrayProperties,
				TextArrayProperties:    obj.Properties.TextArrayProperties,
				IntArrayProperties:     obj.Properties.IntArrayProperties,
				ObjectProperties:       obj.Properties.ObjectProperties,
				ObjectArrayProperties:  obj.Properties.ObjectArrayProperties,
				EmptyListProps:         obj.Properties.EmptyListProps,
			})
			// If class is not in schema, continue as there is no ref to extract
			if class != nil {
				if err := extractSingleRefTarget(class, obj.Properties.SingleTargetRefProps, props); err != nil {
					objectErrors[i] = err
					continue
				}
				if err := extractMultiRefTarget(class, obj.Properties.MultiTargetRefProps, props); err != nil {
					objectErrors[i] = err
					continue
				}
			}
		}

		if _, err := uuid.Parse(obj.Uuid); err != nil {
			objectErrors[i] = err
			continue
		}

		var vector []float32 = nil
		// bytes vector has precedent for being more efficient
		if len(obj.VectorBytes) > 0 {
			vector = byteops.Fp32SliceFromBytes(obj.VectorBytes)
		} else if len(obj.Vector) > 0 {
			vector = obj.Vector
		}

		var vectors models.Vectors = nil
		if len(obj.Vectors) > 0 {
			parsedVectors := make(map[string][]float32)
			parsedMultiVectors := make(map[string][][]float32)
			for _, vec := range obj.Vectors {
				switch vec.Type {
				case *pb.Vectors_VECTOR_TYPE_UNSPECIFIED.Enum(), *pb.Vectors_VECTOR_TYPE_SINGLE_FP32.Enum():
					parsedVectors[vec.Name] = byteops.Fp32SliceFromBytes(vec.VectorBytes)
				case *pb.Vectors_VECTOR_TYPE_MULTI_FP32.Enum():
					out, err := byteops.Fp32SliceOfSlicesFromBytes(vec.VectorBytes)
					if err != nil {
						objectErrors[i] = err
						continue
					}
					parsedMultiVectors[vec.Name] = out
				default:
					// do nothing
				}
			}
			vectors = make(models.Vectors, len(parsedVectors)+len(parsedMultiVectors))
			for targetVector, vector := range parsedVectors {
				vectors[targetVector] = vector
			}
			for targetVector, multiVector := range parsedMultiVectors {
				vectors[targetVector] = multiVector
			}
		}

		objOriginalIndex[insertCounter] = i
		objs = append(objs, &models.Object{
			Class:      obj.Collection,
			Tenant:     obj.Tenant,
			Vector:     vector,
			Properties: props,
			ID:         strfmt.UUID(obj.Uuid),
			Vectors:    vectors,
		})
		insertCounter += 1
	}
	return objs[:insertCounter], objOriginalIndex, objectErrors
}

func extractSingleRefTarget(class *models.Class, properties []*pb.BatchObject_SingleTargetRefProps, props map[string]interface{}) error {
	for _, refSingle := range properties {
		propName := refSingle.GetPropName()
		prop, err := schema.GetPropertyByName(class, propName)
		if err != nil {
			return err
		}
		if len(prop.DataType) > 1 {
			return fmt.Errorf("target is a multi-target reference, need single target %v", prop.DataType)
		}
		toClass := prop.DataType[0]
		beacons := make([]interface{}, len(refSingle.Uuids))
		for j, uuid := range refSingle.Uuids {
			beacons[j] = map[string]interface{}{"beacon": BEACON_START + toClass + "/" + uuid}
		}
		props[propName] = beacons
	}
	return nil
}

func extractMultiRefTarget(class *models.Class, properties []*pb.BatchObject_MultiTargetRefProps, props map[string]interface{}) error {
	for _, refMulti := range properties {
		propName := refMulti.GetPropName()
		prop, err := schema.GetPropertyByName(class, propName)
		if err != nil {
			return err
		}
		if len(prop.DataType) < 2 {
			return fmt.Errorf("target is a single-target reference, need multi-target %v", prop.DataType)
		}
		beacons := make([]interface{}, len(refMulti.Uuids))
		refMulti.TargetCollection = schema.UppercaseClassName(refMulti.TargetCollection)
		for j, uid := range refMulti.Uuids {
			beacons[j] = map[string]interface{}{"beacon": BEACON_START + refMulti.TargetCollection + "/" + uid}
		}
		props[propName] = beacons
	}
	return nil
}

func extractPrimitiveProperties(properties *pb.ObjectPropertiesValue) map[string]interface{} {
	var props map[string]interface{}
	if properties.NonRefProperties != nil {
		props = properties.NonRefProperties.AsMap()
	} else {
		props = make(map[string]interface{})
	}

	// arrays cannot be part of a GRPC map, so we need to handle each type separately
	for j := range properties.BooleanArrayProperties {
		props[properties.BooleanArrayProperties[j].PropName] = sliceToInterface(properties.BooleanArrayProperties[j].Values)
	}

	for j := range properties.NumberArrayProperties {
		inputValuesBytes := properties.NumberArrayProperties[j].ValuesBytes
		var values []float64

		if len(inputValuesBytes) > 0 {
			values = byteops.Fp64SliceFromBytes(inputValuesBytes)
		} else {
			values = properties.NumberArrayProperties[j].Values
		}

		props[properties.NumberArrayProperties[j].PropName] = sliceToInterface(values)
	}

	for j := range properties.TextArrayProperties {
		props[properties.TextArrayProperties[j].PropName] = sliceToInterface(properties.TextArrayProperties[j].Values)
	}

	for j := range properties.IntArrayProperties {
		props[properties.IntArrayProperties[j].PropName] = sliceToInterface(properties.IntArrayProperties[j].Values)
	}

	for j := range properties.ObjectProperties {
		props[properties.ObjectProperties[j].PropName] = extractPrimitiveProperties(properties.ObjectProperties[j].Value)
	}

	for _, prop := range properties.ObjectArrayProperties {
		nested := make([]interface{}, len(prop.Values))
		for k := range prop.Values {
			nested[k] = extractPrimitiveProperties(prop.Values[k])
		}
		props[prop.PropName] = nested
	}

	for _, propName := range properties.EmptyListProps {
		props[propName] = []interface{}{}
	}

	return props
}
