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

package ent

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	DefaultMorphDocumentType     = "text"
	DefaultMorphModel            = "morph-embedding-v3"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	DefaultBaseURL               = "https://api.morphllm.com"
	DefaultApiVersion            = "v1"
	LowerCaseInput               = false
)

const (
	MorphEmbeddingV2 = "morph-embedding-v2"
	MorphEmbeddingV3 = "morph-embedding-v3"
)

var (
	MorphEmbeddingV2DefaultDimensions int64 = 1536
	MorphEmbeddingV3DefaultDimensions int64 = 1024
)

var availableMorphTypes = []string{"text", "code"}

var availableMorphModels = []string{
	// Morph embedding models
	MorphEmbeddingV2,
	MorphEmbeddingV3,
}

var availableMorphModelsDimensions = map[string][]int64{
	MorphEmbeddingV2: {MorphEmbeddingV2DefaultDimensions},
	MorphEmbeddingV3: {MorphEmbeddingV3DefaultDimensions},
}

// Legacy models - not used for Morph
var availableOpenAIModels = []string{}

var availableApiVersions = []string{
	"v1",
}

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg, false)}
}

func (cs *classSettings) Model() string {
	return cs.BaseClassSettings.GetPropertyAsString("model", DefaultMorphModel)
}

func (cs *classSettings) Type() string {
	return cs.BaseClassSettings.GetPropertyAsString("type", DefaultMorphDocumentType)
}

func (cs *classSettings) ModelVersion() string {
	defaultVersion := PickDefaultModelVersion(cs.Model(), cs.Type())
	return cs.BaseClassSettings.GetPropertyAsString("modelVersion", defaultVersion)
}

func (cs *classSettings) ModelStringForAction(action string) string {
	// Morph models are simple - just return the model name
	return cs.Model()
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
	return true // Morph is always a third-party provider
}

func (cs *classSettings) IsAzure() bool {
	return false // Morph doesn't support Azure
}

func (cs *classSettings) Dimensions() *int64 {
	defaultValue := PickDefaultDimensions(cs.Model())
	return cs.BaseClassSettings.GetPropertyAsInt64("dimensions", defaultValue)
}

func (cs *classSettings) Validate(class *models.Class) error {
	if err := cs.BaseClassSettings.Validate(class); err != nil {
		return err
	}

	docType := cs.Type()
	if !basesettings.ValidateSetting(docType, availableMorphTypes) {
		return errors.Errorf("wrong Morph type name, available type names are: %v", availableMorphTypes)
	}

	model := cs.Model()
	if !basesettings.ValidateSetting(model, availableMorphModels) {
		return errors.Errorf("wrong Morph model name, available model names are: %v", availableMorphModels)
	}

	dimensions := cs.Dimensions()
	if dimensions != nil {
		availableDimensions := availableMorphModelsDimensions[model]
		if len(availableDimensions) > 0 && !basesettings.ValidateSetting(*dimensions, availableDimensions) {
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
	// Morph models don't use versions, so no validation needed
	return nil
}

func (cs *classSettings) validateAzureConfig(resourceName, deploymentId, apiVersion string) error {
	// Morph doesn't support Azure, so no validation needed
	return nil
}

func PickDefaultModelVersion(model, docType string) string {
	// Morph models don't use versions
	return ""
}

func PickDefaultDimensions(model string) *int64 {
	if model == MorphEmbeddingV2 {
		return &MorphEmbeddingV2DefaultDimensions
	}
	if model == MorphEmbeddingV3 {
		return &MorphEmbeddingV3DefaultDimensions
	}
	return nil
}
