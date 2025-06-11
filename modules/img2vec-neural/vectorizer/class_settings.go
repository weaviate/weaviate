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

type ClsSettings struct {
	Cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *ClsSettings {
	return &ClsSettings{Cfg: cfg}
}

func (ic *ClsSettings) Properties() ([]string, error) {
	if ic.Cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return nil, errors.New("empty config")
	}

	imageFields, ok := ic.Cfg.Class()["imageFields"]
	if !ok {
		return nil, errors.New("imageFields not present")
	}

	imageFieldsArray, ok := imageFields.([]interface{})
	if !ok {
		return nil, errors.New("imageFields must be an array")
	}

	fieldNames := make([]string, len(imageFieldsArray))
	for i, value := range imageFieldsArray {
		fieldNames[i] = value.(string)
	}
	return fieldNames, nil
}

func (ic *ClsSettings) ImageField(property string) bool {
	fieldNames, err := ic.Properties()
	if err != nil {
		return false
	}
	for i := range fieldNames {
		if fieldNames[i] == property {
			return true
		}
	}

	return false
}

func (ic *ClsSettings) Validate() error {
	if ic.Cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	imageFields, ok := ic.Cfg.Class()["imageFields"]
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
