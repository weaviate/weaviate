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
	"strings"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	DefaultOpenAIDocumentType    = "text"
	DefaultOpenAIModel           = "text-embedding-3-small"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	DefaultBaseURL               = "https://api.openai.com"
	DefaultApiVersion            = "2024-02-01"
	LowerCaseInput               = false
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

var availableApiVersions = []string{
	"2022-12-01",
	"2023-03-15-preview",
	"2023-05-15",
	"2023-06-01-preview",
	"2023-07-01-preview",
	"2023-08-01-preview",
	"2023-09-01-preview",
	"2023-12-01-preview",
	"2024-02-15-preview",
	"2024-03-01-preview",
	"2024-02-01",
}

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg, false)}
}

func (cs *classSettings) Model() string {
	return cs.BaseClassSettings.GetPropertyAsString("model", DefaultOpenAIModel)
}

func (cs *classSettings) Type() string {
	return cs.BaseClassSettings.GetPropertyAsString("type", DefaultOpenAIDocumentType)
}

func (cs *classSettings) ModelVersion() string {
	defaultVersion := PickDefaultModelVersion(cs.Model(), cs.Type())
	return cs.BaseClassSettings.GetPropertyAsString("modelVersion", defaultVersion)
}

func (cs *classSettings) ModelStringForAction(action string) string {
	if strings.HasPrefix(cs.Model(), "text-embedding-3") || cs.IsThirdPartyProvider() {
		// indicates that we handle v3 models
		return cs.Model()
	}
	if cs.ModelVersion() == "002" {
		return cs.getModel002String(cs.Model())
	}
	return cs.getModel001String(cs.Type(), cs.Model(), action)
}

func (v *classSettings) getModel001String(docType, model, action string) string {
	modelBaseString := "%s-search-%s-%s-001"
	if action == "document" {
		if docType == "code" {
			return fmt.Sprintf(modelBaseString, docType, model, "code")
		}
		return fmt.Sprintf(modelBaseString, docType, model, "doc")

	} else {
		if docType == "code" {
			return fmt.Sprintf(modelBaseString, docType, model, "text")
		}
		return fmt.Sprintf(modelBaseString, docType, model, "query")
	}
}

func (v *classSettings) getModel002String(model string) string {
	modelBaseString := "text-embedding-%s-002"
	return fmt.Sprintf(modelBaseString, model)
}

func (cs *classSettings) ResourceName() string {
	return cs.BaseClassSettings.GetPropertyAsString("resourceName", "")
}

func (cs *classSettings) BaseURL() string {
	return cs.BaseClassSettings.GetPropertyAsString("baseURL", DefaultBaseURL)
}

func (cs *classSettings) DeploymentID() string {
	return cs.BaseClassSettings.GetPropertyAsString("deploymentId", "")
}

func (cs *classSettings) ApiVersion() string {
	return cs.BaseClassSettings.GetPropertyAsString("apiVersion", DefaultApiVersion)
}

func (cs *classSettings) IsThirdPartyProvider() bool {
	return !(strings.Contains(cs.BaseURL(), "api.openai.com") || cs.IsAzure())
}

func (cs *classSettings) IsAzure() bool {
	return cs.BaseClassSettings.GetPropertyAsBool("isAzure", false) || (cs.ResourceName() != "" && cs.DeploymentID() != "")
}

func (cs *classSettings) Dimensions() *int64 {
	defaultValue := PickDefaultDimensions(cs.Model())
	if cs.IsAzure() {
		defaultValue = nil
	}
	return cs.BaseClassSettings.GetPropertyAsInt64("dimensions", defaultValue)
}

func (cs *classSettings) Validate(class *models.Class) error {
	if err := cs.BaseClassSettings.Validate(class); err != nil {
		return err
	}

	docType := cs.Type()
	if !basesettings.ValidateSetting[string](docType, availableOpenAITypes) {
		return errors.Errorf("wrong OpenAI type name, available model names are: %v", availableOpenAITypes)
	}

	model := cs.Model()
	// only validate models for openAI endpoints
	if !cs.IsThirdPartyProvider() {
		availableModels := append(availableOpenAIModels, availableV3Models...)
		if !basesettings.ValidateSetting[string](model, availableModels) {
			return errors.Errorf("wrong OpenAI model name, available model names are: %v", availableModels)
		}
	}

	dimensions := cs.Dimensions()
	if !cs.IsThirdPartyProvider() && dimensions != nil {
		if !basesettings.ValidateSetting[string](model, availableV3Models) {
			return errors.Errorf("dimensions setting can only be used with V3 embedding models: %v", availableV3Models)
		}
		availableDimensions := availableV3ModelsDimensions[model]
		if !basesettings.ValidateSetting[int64](*dimensions, availableDimensions) {
			return errors.Errorf("wrong dimensions setting for %s model, available dimensions are: %v", model, availableDimensions)
		}
	}

	version := cs.ModelVersion()
	if err := cs.validateModelVersion(version, model, docType); err != nil {
		return err
	}

	if cs.IsAzure() {
		err := cs.validateAzureConfig(cs.ResourceName(), cs.DeploymentID(), cs.ApiVersion())
		if err != nil {
			return err
		}
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

func (cs *classSettings) validateAzureConfig(resourceName, deploymentId, apiVersion string) error {
	if (resourceName == "" && deploymentId != "") || (resourceName != "" && deploymentId == "") {
		return fmt.Errorf("both resourceName and deploymentId must be provided")
	}
	if !basesettings.ValidateSetting[string](apiVersion, availableApiVersions) {
		return errors.Errorf("wrong Azure OpenAI apiVersion setting, available api versions are: %v", availableApiVersions)
	}
	return nil
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
