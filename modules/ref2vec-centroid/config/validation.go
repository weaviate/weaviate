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

package config

import (
	"errors"
	"fmt"
)

var errInvalidConfig = errors.New("invalid config")

func Validate(cfg *Config) error {
	// referencePropertiesField is a required field
	class := cfg.class.Class()
	refProps, ok := class[referencePropertiesField]
	if !ok {
		return fmt.Errorf("%w: must have at least one value in the %q field",
			errInvalidConfig, referencePropertiesField)
	}

	propSlice, ok := refProps.([]interface{})
	if !ok {
		return fmt.Errorf("%w: expected array for field %q, got %T",
			errInvalidConfig, referencePropertiesField, refProps)
	}

	if len(propSlice) == 0 {
		return fmt.Errorf("%w: must have at least one value in the %q field",
			errInvalidConfig, referencePropertiesField)
	}

	// all provided property names must be strings
	for _, prop := range propSlice {
		if _, ok := prop.(string); !ok {
			return fmt.Errorf("%w: expected %q to contain strings, found %T: %+v",
				errInvalidConfig, referencePropertiesField, prop, refProps)
		}
	}

	return nil
}
