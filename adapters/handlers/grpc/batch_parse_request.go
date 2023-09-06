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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc"
)

const BEACON_START = "weaviate://localhost/"

func sliceToInterface[T any](values []T) []interface{} {
	tmpArray := make([]interface{}, len(values))
	for k := range values {
		tmpArray[k] = values[k]
	}
	return tmpArray
}

func batchFromProto(req *pb.BatchObjectsRequest, scheme schema.Schema) ([]*models.Object, error) {
	objectsBatch := req.Objects
	objs := make([]*models.Object, len(objectsBatch))
	for i, obj := range objectsBatch {
		class := scheme.GetClass(schema.ClassName(obj.ClassName))
		var props map[string]interface{}
		if obj.Properties != nil {
			if obj.Properties.NonRefProperties != nil {
				props = obj.Properties.NonRefProperties.AsMap()
			} else {
				props = make(map[string]interface{})
			}

			// arrays cannot be part of a GRPC map, so we need to handle each type separately
			if obj.Properties.BooleanArrayProperties != nil {
				for j := range obj.Properties.BooleanArrayProperties {
					props[obj.Properties.BooleanArrayProperties[j].PropName] = sliceToInterface(obj.Properties.BooleanArrayProperties[j].Value)
				}
			}

			if obj.Properties.NumberArrayProperties != nil {
				for j := range obj.Properties.NumberArrayProperties {
					props[obj.Properties.NumberArrayProperties[j].PropName] = sliceToInterface(obj.Properties.NumberArrayProperties[j].Value)
				}
			}

			if obj.Properties.TextArrayProperties != nil {
				for j := range obj.Properties.TextArrayProperties {
					props[obj.Properties.TextArrayProperties[j].PropName] = sliceToInterface(obj.Properties.TextArrayProperties[j].Value)
				}
			}

			if obj.Properties.IntArrayProperties != nil {
				for j := range obj.Properties.IntArrayProperties {
					props[obj.Properties.IntArrayProperties[j].PropName] = sliceToInterface(obj.Properties.IntArrayProperties[j].Value)
				}
			}

			if err := extractSingleRefTarget(class, obj, props); err != nil {
				return nil, err
			}
			if err := extractMultiRefTarget(class, obj, props); err != nil {
				return nil, err
			}
		}

		objs[i] = &models.Object{
			Class:      obj.ClassName,
			Tenant:     obj.Tenant,
			Vector:     obj.Vector,
			Properties: props,
			ID:         strfmt.UUID(obj.Uuid),
		}
	}
	return objs, nil
}

func extractSingleRefTarget(class *models.Class, obj *pb.BatchObject, props map[string]interface{}) error {
	for _, refSingle := range obj.Properties.RefPropsSingle {
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
	for _, refMulti := range obj.Properties.RefPropsMulti {
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
