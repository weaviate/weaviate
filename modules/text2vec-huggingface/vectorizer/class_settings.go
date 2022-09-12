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
	"fmt"

	"github.com/pkg/errors"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema"
)

const (
	DefaultHuggingFaceModel      = "sentence-transformers/msmarco-bert-base-dot-v5"
	DefaultOptionWaitForModel    = false
	DefaultOptionUseGPU          = false
	DefaultOptionUseCache        = true
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

func (ic *classSettings) PassageModel() string {
	model := ic.getPassageModel()
	if model == "" {
		return DefaultHuggingFaceModel
	}
	return model
}

func (ic *classSettings) QueryModel() string {
	model := ic.getQueryModel()
	if model == "" {
		return DefaultHuggingFaceModel
	}
	return model
}

func (ic *classSettings) OptionWaitForModel() bool {
	return ic.getOptionOrDefault("waitForModel", DefaultOptionWaitForModel)
}

func (ic *classSettings) OptionUseGPU() bool {
	return ic.getOptionOrDefault("useGPU", DefaultOptionUseGPU)
}

func (ic *classSettings) OptionUseCache() bool {
	return ic.getOptionOrDefault("useCache", DefaultOptionUseCache)
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

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	err := ic.validateClassSettings()
	if err != nil {
		return err
	}

	err = ic.validateIndexState(class, ic)
	if err != nil {
		return err
	}

	return nil
}

func (ic *classSettings) validateClassSettings() error {
	model := ic.getProperty("model")
	passageModel := ic.getProperty("passageModel")
	queryModel := ic.getProperty("queryModel")

	if model != "" && (passageModel != "" || queryModel != "") {
		return errors.New("only one setting must be set either 'model' or 'passageModel' with 'queryModel'")
	}

	if model == "" {
		if passageModel != "" && queryModel == "" {
			return errors.New("'passageModel' is set, but 'queryModel' is empty")
		}
		if passageModel == "" && queryModel != "" {
			return errors.New("'queryModel' is set, but 'passageModel' is empty")
		}
	}
	return nil
}

func (ic *classSettings) getPassageModel() string {
	model := ic.getProperty("model")
	if model == "" {
		model = ic.getProperty("passageModel")
	}
	return model
}

func (ic *classSettings) getQueryModel() string {
	model := ic.getProperty("model")
	if model == "" {
		model = ic.getProperty("queryModel")
	}
	return model
}

func (ic *classSettings) getOption(option string) *bool {
	if ic.cfg != nil {
		options, ok := ic.cfg.Class()["options"]
		if ok {
			asMap, ok := options.(map[string]interface{})
			if ok {
				option, ok := asMap[option]
				if ok {
					asBool, ok := option.(bool)
					if ok {
						return &asBool
					}
				}
			}
		}
	}
	return nil
}

func (ic *classSettings) getOptionOrDefault(option string, defaultValue bool) bool {
	optionValue := ic.getOption(option)
	if optionValue != nil {
		return *optionValue
	}
	return defaultValue
}

func (ic *classSettings) getProperty(name string) string {
	if ic.cfg != nil {
		model, ok := ic.cfg.Class()[name]
		if ok {
			asString, ok := model.(string)
			if ok {
				return asString
			}
		}
	}
	return ""
}

func (cv *classSettings) validateIndexState(class *models.Class, settings ClassSettings) error {
	if settings.VectorizeClassName() {
		// if the user chooses to vectorize the classname, vector-building will
		// always be possible, no need to investigate further

		return nil
	}

	// search if there is at least one indexed, string/text prop. If found pass
	// validation
	for _, prop := range class.Properties {
		if len(prop.DataType) < 1 {
			return errors.Errorf("property %s must have at least one datatype: "+
				"got %v", prop.Name, prop.DataType)
		}

		if prop.DataType[0] != string(schema.DataTypeString) &&
			prop.DataType[0] != string(schema.DataTypeText) {
			// we can only vectorize text-like props
			continue
		}

		if settings.PropertyIndexed(prop.Name) {
			// found at least one, this is a valid schema
			return nil
		}
	}

	return fmt.Errorf("invalid properties: didn't find a single property which is " +
		"of type string or text and is not excluded from indexing. In addition the " +
		"class name is excluded from vectorization as well, meaning that it cannot be " +
		"used to determine the vector position. To fix this, set 'vectorizeClassName' " +
		"to true if the class name is contextionary-valid. Alternatively add at least " +
		"contextionary-valid text/string property which is not excluded from " +
		"indexing.")
}
