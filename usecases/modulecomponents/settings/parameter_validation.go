//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package settings

import (
	"fmt"
	"reflect"
)

// ValidateAllowedValues checks if the given value is in the parameter's AllowedValues.
// Returns an error if the value is not allowed, nil otherwise.
func ValidateAllowedValues(paramKey string, param ParameterDef, value interface{}) error {
	if param.AllowedValues == nil {
		return nil // No restrictions
	}

	// Handle different types of AllowedValues
	allowedSlice := reflect.ValueOf(param.AllowedValues)
	if allowedSlice.Kind() != reflect.Slice {
		return nil // Invalid AllowedValues configuration, skip validation
	}

	// Check if value is in the allowed slice
	for i := 0; i < allowedSlice.Len(); i++ {
		allowed := allowedSlice.Index(i).Interface()
		if reflect.DeepEqual(value, allowed) {
			return nil // Found match
		}
	}

	// Value not found in allowed values
	return fmt.Errorf("invalid %s: %v, allowed values are: %v", param.JSONKey, value, param.AllowedValues)
}
