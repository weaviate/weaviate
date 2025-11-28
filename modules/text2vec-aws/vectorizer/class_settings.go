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

package vectorizer

import (
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

// Default values for service cannot be changed before we solve how old classes
// that have the defaults NOT set will handle the change
const (
	DefaultVectorizeClassName    = false
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	DefaultService               = "bedrock"

	// Parameter keys for accessing the Parameters map
	ParamService       = "Service"
	ParamRegion        = "Region"
	ParamModel         = "Model"
	ParamEndpoint      = "Endpoint"
	ParamTargetModel   = "TargetModel"
	ParamTargetVariant = "TargetVariant"
)

var availableAWSServices = []string{
	"bedrock",
	"sagemaker",
}

// Parameters defines all configuration parameters for text2vec-aws
var Parameters = map[string]basesettings.ParameterDef{
	ParamService: {
		JSONKey:       "service",
		DefaultValue:  DefaultService,
		Description:   "AWS service to use (bedrock or sagemaker)",
		Required:      false,
		AllowedValues: availableAWSServices,
	},
	ParamRegion: {
		JSONKey:      "region",
		DefaultValue: "",
		Description:  "AWS region",
		Required:     true,
	},
	ParamModel: {
		JSONKey:      "model",
		DefaultValue: "",
		Description:  "Model identifier (required for bedrock, not allowed for sagemaker)",
		Required:     false, // Conditionally required based on service
	},
	ParamEndpoint: {
		JSONKey:      "endpoint",
		DefaultValue: "",
		Description:  "Sagemaker endpoint name (required for sagemaker, not allowed for bedrock)",
		Required:     false, // Conditionally required based on service
	},
	ParamTargetModel: {
		JSONKey:      "targetModel",
		DefaultValue: "",
		Description:  "Target model for Sagemaker inference",
		Required:     false,
	},
	ParamTargetVariant: {
		JSONKey:      "targetVariant",
		DefaultValue: "",
		Description:  "Target variant for Sagemaker inference",
		Required:     false,
	},
}

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg, false)}
}

func (ic *classSettings) Validate(class *models.Class) error {
	var errorMessages []string
	if err := ic.BaseClassSettings.Validate(class); err != nil {
		errorMessages = append(errorMessages, err.Error())
	}

	service := ic.Service()
	if err := basesettings.ValidateAllowedValues(ParamService, Parameters[ParamService], service); err != nil {
		errorMessages = append(errorMessages, err.Error())
	}
	region := ic.Region()
	if region == "" {
		errorMessages = append(errorMessages, fmt.Sprintf("%s cannot be empty", Parameters[ParamRegion].JSONKey))
	}

	if isBedrock(service) {
		model := ic.Model()
		if model == "" {
			errorMessages = append(errorMessages, fmt.Sprintf("%s has to be defined", Parameters[ParamModel].JSONKey))
		}
		endpoint := ic.Endpoint()
		if endpoint != "" {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong configuration: %s, not applicable to %s", endpoint, service))
		}
	}

	if isSagemaker(service) {
		endpoint := ic.Endpoint()
		if endpoint == "" {
			errorMessages = append(errorMessages, fmt.Sprintf("%s cannot be empty", Parameters[ParamEndpoint].JSONKey))
		}
		model := ic.Model()
		if model != "" {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong configuration: %s, not applicable to %s. did you mean %s", Parameters[ParamModel].JSONKey, service, Parameters[ParamTargetModel].JSONKey))
		}
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("%s", strings.Join(errorMessages, ", "))
	}

	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) string {
	return ic.BaseClassSettings.GetPropertyAsString(name, defaultValue)
}

// Aws params
func (ic *classSettings) Service() string {
	return ic.getStringProperty(Parameters[ParamService].JSONKey, DefaultService)
}

func (ic *classSettings) Region() string {
	return ic.getStringProperty(Parameters[ParamRegion].JSONKey, "")
}

func (ic *classSettings) Model() string {
	return ic.getStringProperty(Parameters[ParamModel].JSONKey, "")
}

func (ic *classSettings) Endpoint() string {
	return ic.getStringProperty(Parameters[ParamEndpoint].JSONKey, "")
}

func (ic *classSettings) TargetModel() string {
	return ic.getStringProperty(Parameters[ParamTargetModel].JSONKey, "")
}

func (ic *classSettings) TargetVariant() string {
	return ic.getStringProperty(Parameters[ParamTargetVariant].JSONKey, "")
}

func isSagemaker(service string) bool {
	return service == "sagemaker"
}

func isBedrock(service string) bool {
	return service == "bedrock"
}
