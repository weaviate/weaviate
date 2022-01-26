//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package vectorizer

import (
	"github.com/pkg/errors"

	"github.com/semi-technologies/weaviate/entities/moduletools"
)

const (
	DefaultOpenAIDocumentType    = "text"
	DefaultOpenAIModel           = "ada"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
)

type classSettings struct {
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg}
}

func (ic *classSettings) PropertyIndexed(propName string) bool {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return DefaultPropertyIndexed
	}

	vcn, ok := ic.cfg.Property(propName)["skip"]
	if !ok {
		return DefaultPropertyIndexed
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultPropertyIndexed
	}

	return !asBool
}

func (ic *classSettings) VectorizePropertyName(propName string) bool {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return DefaultVectorizePropertyName
	}
	vcn, ok := ic.cfg.Property(propName)["vectorizePropertyName"]
	if !ok {
		return DefaultVectorizePropertyName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultVectorizePropertyName
	}

	return asBool
}

func (ic *classSettings) Model() string {
	return ic.getProperty("model", DefaultOpenAIModel)
}

func (ic *classSettings) Type() string {
	return ic.getProperty("type", DefaultOpenAIDocumentType)
}

func (ic *classSettings) VectorizeClassName() bool {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return DefaultVectorizeClassName
	}

	vcn, ok := ic.cfg.Class()["vectorizeClassName"]
	if !ok {
		return DefaultVectorizeClassName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultVectorizeClassName
	}

	return asBool
}

func (ic *classSettings) Validate() error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	// TODO: implement validation
	// _, err := ic.Model()
	// if err != nil {
	// 	return err
	// }
	// _, err = ic.Type()
	// if err != nil {
	// 	return err
	// }

	return nil
}

func (ic *classSettings) getProperty(name, defaultValue string) string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return defaultValue
	}

	model, ok := ic.cfg.Class()[name]
	if ok {
		asString, ok := model.(string)
		if ok {
			return asString
		}
	}

	return defaultValue
}
