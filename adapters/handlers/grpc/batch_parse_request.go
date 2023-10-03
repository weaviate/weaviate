//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package grpc

import (
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol"
)

const BEACON_START = "weaviate://localhost/"

func sliceToInterface[T any](values []T) []interface{} {
	tmpArray := make([]interface{}, len(values))
	for k := range values {
		tmpArray[k] = values[k]
	}
	return tmpArray
}

func batchFromProto(req *pb.BatchObjectsRequest, scheme schema.Schema) ([]*models.Object, map[int]int, map[int]error) {
	objectsBatch := req.Objects
	objs := make([]*models.Object, 0, len(objectsBatch))
	objOriginalIndex := make(map[int]int)
	objectErrors := make(map[int]error, len(objectsBatch))

	insertCounter := 0
	for i, obj := range objectsBatch {
		class := scheme.GetClass(schema.ClassName(obj.Collection))
		var props map[string]interface{}
		if obj.Properties != nil {
			props = extractPrimitiveProperties(obj.Properties)
			if err := extractSingleRefTarget(class, obj, props); err != nil {
				objectErrors[i] = err
				continue
			}
			if err := extractMultiRefTarget(class, obj, props); err != nil {
				objectErrors[i] = err
				continue
			}
		}

		_, err := uuid.Parse(obj.Uuid)
		if err != nil {
			objectErrors[i] = err
			continue
		}
		objOriginalIndex[insertCounter] = i
		objs = append(objs, &models.Object{
			Class:      obj.Collection,
			Tenant:     obj.Tenant,
			Vector:     obj.Vector,
			Properties: props,
			ID:         strfmt.UUID(obj.Uuid),
		})
		insertCounter += 1
	}
	return objs[:insertCounter], objOriginalIndex, objectErrors
}

func extractSingleRefTarget(class *models.Class, obj *pb.BatchObject, props map[string]interface{}) error {
	for _, refSingle := range obj.Properties.SingleTargetRefProps {
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

func extractMultiRefTarget(class *models.Class, obj *pb.BatchObject, props map[string]interface{}) error {
	for _, refMulti := range obj.Properties.MultiTargetRefProps {
		propName := refMulti.GetPropName()
		prop, err := schema.GetPropertyByName(class, propName)
		if err != nil {
			return err
		}
		if len(prop.DataType) < 2 {
			return fmt.Errorf("target is a single-target reference, need multi-target %v", prop.DataType)
		}
		beacons := make([]interface{}, len(refMulti.Uuids))
		for j, uuid := range refMulti.Uuids {
			beacons[j] = map[string]interface{}{"beacon": BEACON_START + refMulti.TargetCollection + "/" + uuid}
		}
		props[propName] = beacons
	}
	return nil
}

func extractPrimitiveProperties(properties *pb.BatchObject_Properties) map[string]interface{} {
	var props map[string]interface{}
	if properties.NonRefProperties != nil {
		props = properties.NonRefProperties.AsMap()
	} else {
		props = make(map[string]interface{})
	}

	// arrays cannot be part of a GRPC map, so we need to handle each type separately
	if properties.BooleanArrayProperties != nil {
		for j := range properties.BooleanArrayProperties {
			props[properties.BooleanArrayProperties[j].PropName] = sliceToInterface(properties.BooleanArrayProperties[j].Values)
		}
	}

	if properties.NumberArrayProperties != nil {
		for j := range properties.NumberArrayProperties {
			props[properties.NumberArrayProperties[j].PropName] = sliceToInterface(properties.NumberArrayProperties[j].Values)
		}
	}

	if properties.TextArrayProperties != nil {
		for j := range properties.TextArrayProperties {
			props[properties.TextArrayProperties[j].PropName] = sliceToInterface(properties.TextArrayProperties[j].Values)
		}
	}

	if properties.IntArrayProperties != nil {
		for j := range properties.IntArrayProperties {
			props[properties.IntArrayProperties[j].PropName] = sliceToInterface(properties.IntArrayProperties[j].Values)
		}
	}

	if properties.ObjectProperties != nil {
		for j := range properties.ObjectProperties {
			props[properties.ObjectProperties[j].PropName] = extractPrimitiveProperties(properties.ObjectProperties[j].Value)
		}
	}

	if properties.ObjectArrayProperties != nil {
		for j := range properties.ObjectArrayProperties {
			nested := make([]interface{}, len(properties.ObjectArrayProperties[j].Values))
			for k := range properties.ObjectArrayProperties[j].Values {
				nested[k] = extractPrimitiveProperties(properties.ObjectArrayProperties[j].Values[k])
			}
			props[properties.ObjectArrayProperties[j].PropName] = nested
		}
	}

	return props
}
