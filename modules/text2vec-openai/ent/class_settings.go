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

package ent

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	DefaultOpenAIDocumentType    = "text"
	DefaultOpenAIModel           = "ada"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	DefaultBaseURL               = "https://api.openai.com"
)

const (
	TextEmbedding3Small = "text-embedding-3-small"
	TextEmbedding3Large = "text-embedding-3-large"
)

var (
	TextEmbedding3SmallDefaultDimensions int64 = 1536
	TextEmbedding3LargeDefaultDimensions int64 = 3072
)

var availableOpenAITypes = []string{"text", "code"}

var availableV3Models = []string{
	// new v3 models
	TextEmbedding3Small,
	TextEmbedding3Large,
}

var availableV3ModelsDimensions = map[string][]int64{
	TextEmbedding3Small: {512, TextEmbedding3SmallDefaultDimensions},
	TextEmbedding3Large: {256, 1024, TextEmbedding3LargeDefaultDimensions},
}

var availableOpenAIModels = []string{
	"ada",     // supports 001 and 002
	"babbage", // only supports 001
	"curie",   // only supports 001
	"davinci", // only supports 001
}

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg)}
}

func (cs *classSettings) Model() string {
	return cs.getProperty("model", DefaultOpenAIModel)
}

func (cs *classSettings) Type() string {
	return cs.getProperty("type", DefaultOpenAIDocumentType)
}

func (cs *classSettings) ModelVersion() string {
	defaultVersion := PickDefaultModelVersion(cs.Model(), cs.Type())
	return cs.getProperty("modelVersion", defaultVersion)
}

func (cs *classSettings) ResourceName() string {
	return cs.getProperty("resourceName", "")
}

func (cs *classSettings) BaseURL() string {
	return cs.getProperty("baseURL", DefaultBaseURL)
}

func (cs *classSettings) DeploymentID() string {
	return cs.getProperty("deploymentId", "")
}

func (cs *classSettings) IsAzure() bool {
	return cs.ResourceName() != "" && cs.DeploymentID() != ""
}

func (cs *classSettings) Dimensions() *int64 {
	defaultValue := PickDefaultDimensions(cs.Model())
	return cs.getPropertyAsInt("dimensions", defaultValue)
}

func (cs *classSettings) Validate(class *models.Class) error {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	if err := cs.BaseClassSettings.Validate(); err != nil {
		return err
	}

	docType := cs.Type()
	if !validateOpenAISetting[string](docType, availableOpenAITypes) {
		return errors.Errorf("wrong OpenAI type name, available model names are: %v", availableOpenAITypes)
	}

	availableModels := append(availableOpenAIModels, availableV3Models...)
	model := cs.Model()
	if !validateOpenAISetting[string](model, availableModels) {
		return errors.Errorf("wrong OpenAI model name, available model names are: %v", availableModels)
	}

	dimensions := cs.Dimensions()
	if dimensions != nil {
		if !validateOpenAISetting[string](model, availableV3Models) {
			return errors.Errorf("dimensions setting can only be used with V3 embedding models: %v", availableV3Models)
		}
		availableDimensions := availableV3ModelsDimensions[model]
		if !validateOpenAISetting[int64](*dimensions, availableDimensions) {
			return errors.Errorf("wrong dimensions setting for %s model, available dimensions are: %v", model, availableDimensions)
		}
	}

	version := cs.ModelVersion()
	if err := cs.validateModelVersion(version, model, docType); err != nil {
		return err
	}

	err := cs.validateAzureConfig(cs.ResourceName(), cs.DeploymentID())
	if err != nil {
		return err
	}

	err = cs.validateIndexState(class, cs.VectorizeClassName(), cs.PropertyIndexed)
	if err != nil {
		return err
	}

	return nil
}

func (cs *classSettings) validateModelVersion(version, model, docType string) error {
	for i := range availableV3Models {
		if model == availableV3Models[i] {
			return nil
		}
	}

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

func (cs *classSettings) getProperty(name, defaultValue string) string {
	return cs.BaseClassSettings.GetPropertyAsString(name, defaultValue)
}

func (cs *classSettings) getPropertyAsInt(name string, defaultValue *int64) *int64 {
	return cs.BaseClassSettings.GetPropertyAsInt64(name, defaultValue)
}

func (cs *classSettings) validateIndexState(class *models.Class, vectorizeClassName bool, propIndexed func(string) bool) error {
	if vectorizeClassName {
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

		if prop.DataType[0] != string(schema.DataTypeText) {
			// we can only vectorize text-like props
			continue
		}

		if propIndexed(prop.Name) {
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

func (cs *classSettings) validateAzureConfig(resourceName string, deploymentId string) error {
	if (resourceName == "" && deploymentId != "") || (resourceName != "" && deploymentId == "") {
		return fmt.Errorf("both resourceName and deploymentId must be provided")
	}
	return nil
}

func validateOpenAISetting[T string | int64](value T, availableValues []T) bool {
	for i := range availableValues {
		if value == availableValues[i] {
			return true
		}
	}
	return false
}

func PickDefaultModelVersion(model, docType string) string {
	for i := range availableV3Models {
		if model == availableV3Models[i] {
			return ""
		}
	}
	if model == "ada" && docType == "text" {
		return "002"
	}
	// for all other combinations stick with "001"
	return "001"
}

func PickDefaultDimensions(model string) *int64 {
	if model == TextEmbedding3Small {
		return &TextEmbedding3SmallDefaultDimensions
	}
	if model == TextEmbedding3Large {
		return &TextEmbedding3LargeDefaultDimensions
	}
	return nil
}
