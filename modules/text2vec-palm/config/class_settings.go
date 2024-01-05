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
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/modules/text2vec-palm/vectorizer"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

const (
	apiEndpointProperty = "apiEndpoint"
	projectIDProperty   = "projectId"
	modelIDProperty     = "modelId"
	titleProperty       = "titleProperty"
)

const (
	DefaultVectorizeClassName     = false
	DefaultPropertyIndexed        = true
	DefaultVectorizePropertyName  = false
	DefaultApiEndpoint            = "us-central1-aiplatform.googleapis.com"
	DefaultModelID                = "textembedding-gecko@001"
	DefaulGenerativeAIApiEndpoint = "generativelanguage.googleapis.com"
	DefaulGenerativeAIModelID     = "embedding-gecko-001"
)

var availablePalmModels = []string{
	DefaultModelID,
	"textembedding-gecko@latest",
	"textembedding-gecko-multilingual@latest",
	"textembedding-gecko@003",
	"textembedding-gecko@002",
	"textembedding-gecko-multilingual@001",
	"textembedding-gecko@001",
}

var availableGenerativeAIModels = []string{
	DefaulGenerativeAIModelID,
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

	var errorMessages []string

	apiEndpoint := ic.ApiEndpoint()
	model := ic.ModelID()
	if apiEndpoint == DefaulGenerativeAIApiEndpoint {
		if model != "" && !ic.validatePalmSetting(model, availableGenerativeAIModels) {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong %s available Generative AI model names are: %v", modelIDProperty, availableGenerativeAIModels))
		}
	} else {
		projectID := ic.ProjectID()
		if projectID == "" {
			errorMessages = append(errorMessages, fmt.Sprintf("%s cannot be empty", projectIDProperty))
		}
		if model != "" && !ic.validatePalmSetting(model, availablePalmModels) {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong %s available model names are: %v", modelIDProperty, availablePalmModels))
		}
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("%s", strings.Join(errorMessages, ", "))
	}

	err := ic.validateIndexState(class, ic)
	if err != nil {
		return err
	}

	return nil
}

func (ic *classSettings) validatePalmSetting(value string, availableValues []string) bool {
	for i := range availableValues {
		if value == availableValues[i] {
			return true
		}
	}
	return false
}

func (ic *classSettings) getStringProperty(name, defaultValue string) string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return defaultValue
	}

	value, ok := ic.cfg.ClassByModuleName("text2vec-palm")[name]
	if ok {
		asString, ok := value.(string)
		if ok {
			return asString
		}
	}
	return defaultValue
}

func (cv *classSettings) validateIndexState(class *models.Class, settings vectorizer.ClassSettings) error {
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
		"indexing")
}

func (ic *classSettings) getDefaultModel(apiEndpoint string) string {
	if apiEndpoint == DefaulGenerativeAIApiEndpoint {
		return DefaulGenerativeAIModelID
	}
	return DefaultModelID
}

// PaLM params
func (ic *classSettings) ApiEndpoint() string {
	return ic.getStringProperty(apiEndpointProperty, DefaultApiEndpoint)
}

func (ic *classSettings) ProjectID() string {
	return ic.getStringProperty(projectIDProperty, "")
}

func (ic *classSettings) ModelID() string {
	return ic.getStringProperty(modelIDProperty, ic.getDefaultModel(ic.ApiEndpoint()))
}

func (ic *classSettings) TitleProperty() string {
	return ic.getStringProperty(titleProperty, "")
}
