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
	"strings"

	"github.com/pkg/errors"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema"
)

const (
	DefaultOpenAIDocumentType    = "text"
	DefaultOpenAIModel           = "ada"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
)

var availableOpenAITypes = []string{"text", "code"}

var availableOpenAIModels = []string{
	"ada",     // supports 001 and 002
	"babbage", // only suppports 001
	"curie",   // only suppports 001
	"davinci", // only suppports 001
}

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

func (ic *classSettings) ModelVersion() string {
	defaultVersion := PickDefaultModelVersion(ic.Model(), ic.Type())
	return ic.getProperty("modelVersion", defaultVersion)
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

	docType := ic.Type()
	if !ic.validateOpenAISetting(docType, availableOpenAITypes) {
		return errors.Errorf("wrong OpenAI type name, available model names are: %v", availableOpenAITypes)
	}

	model := ic.Model()
	if !ic.validateOpenAISetting(model, availableOpenAIModels) {
		return errors.Errorf("wrong OpenAI model name, available model names are: %v", availableOpenAIModels)
	}

	version := ic.ModelVersion()
	if err := ic.validateModelVersion(version, model, docType); err != nil {
		return err
	}

	err := ic.validateIndexState(class, ic)
	if err != nil {
		return err
	}

	return nil
}

func (ic *classSettings) validateModelVersion(version, model, docType string) error {
	if version == "001" {
		// no restrictions
		return nil
	}

	if version == "002" {
		// only ada/davinci 002
		if model != "ada" && model != "davinci" {
			return fmt.Errorf("unsupported version %s", version)
		}
	}

	if version == "003" && model != "davinci" {
		// only davinci 003
		return fmt.Errorf("unsupported version %s", version)
	}

	if version != "002" && version != "003" {
		// all other fallback
		return fmt.Errorf("model %s is only available in version 001", model)
	}

	if docType != "text" {
		return fmt.Errorf("ada-002 no longer distinguishes between text/code, use 'text' for all use cases")
	}

	return nil
}

func (ic *classSettings) validateOpenAISetting(value string, availableValues []string) bool {
	for i := range availableValues {
		if value == availableValues[i] {
			return true
		}
	}
	return false
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
			return strings.ToLower(asString)
		}
	}

	return defaultValue
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

func PickDefaultModelVersion(model, docType string) string {
	if model == "ada" && docType == "text" {
		return "002"
	}

	// for all other combinations stick with "001"
	return "001"
}
