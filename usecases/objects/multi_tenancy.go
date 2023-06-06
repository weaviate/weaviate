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

package objects

import "github.com/weaviate/weaviate/entities/models"

// ParseTenantKeyFromObject extract the value of the tenant key if it exists
func ParseTenantKeyFromObject(tenantKeyName string, o *models.Object) string {
	if props, _ := o.Properties.(map[string]interface{}); props != nil {
		if rawVal := props[tenantKeyName]; rawVal != nil {
			if key, _ := rawVal.(string); key != "" {
				return key
			}
		}
	}
	return ""
}
