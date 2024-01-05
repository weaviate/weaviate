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

package vectorizer

import (
	"errors"

	"github.com/weaviate/weaviate/entities/moduletools"
)

type classSettings struct {
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg}
}

func (ic *classSettings) ImageField(property string) bool {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return false
	}

	imageFields, ok := ic.cfg.Class()["imageFields"]
	if !ok {
		return false
	}

	imageFieldsArray, ok := imageFields.([]interface{})
	if !ok {
		return false
	}

	fieldNames := make([]string, len(imageFieldsArray))
	for i, value := range imageFieldsArray {
		fieldNames[i] = value.(string)
	}

	for i := range fieldNames {
		if fieldNames[i] == property {
			return true
		}
	}

	return false
}

func (ic *classSettings) Validate() error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	imageFields, ok := ic.cfg.Class()["imageFields"]
	if !ok {
		return errors.New("imageFields not present")
	}

	imageFieldsArray, ok := imageFields.([]interface{})
	if !ok {
		return errors.New("imageFields must be an array")
	}

	if len(imageFieldsArray) == 0 {
		return errors.New("must contain at least one image field name in imageFields")
	}

	for _, value := range imageFieldsArray {
		v, ok := value.(string)
		if !ok {
			return errors.New("imageField must be a string")
		}
		if len(v) == 0 {
			return errors.New("imageField values cannot be empty")
		}
	}

	return nil
}
