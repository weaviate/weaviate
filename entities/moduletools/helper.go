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

package moduletools

import "github.com/weaviate/weaviate/entities/models"

func PropertiesListToMap(props models.PropertySchema) map[string]interface{} {
	propsTyped, ok := props.([]*models.Property)
	if !ok {
		propsMap, ok := props.(map[string]interface{})
		if ok {
			return propsMap
		}
	}
	propertiesMap := map[string]interface{}{}
	for _, prop := range propsTyped {
		propertiesMap[prop.Name] = prop
	}
	return propertiesMap
}
