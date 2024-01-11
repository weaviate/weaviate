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
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

const (
	serviceProperty       = "service"
	regionProperty        = "region"
	modelProperty         = "model"
	endpointProperty      = "endpoint"
	targetModelProperty   = "targetModel"
	targetVariantProperty = "targetVariant"
)

const (
	DefaultVectorizeClassName    = false
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	DefaultService               = "bedrock"
)

var availableAWSServices = []string{
	"bedrock",
}

var availableAWSBedrockModels = []string{
	"amazon.titan-embed-text-v1",
	"cohere.embed-english-v3",
	"cohere.embed-multilingual-v3",
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

	service := ic.Service()
	if service == "" || !ic.validatAvailableAWSSetting(service, availableAWSServices) {
		errorMessages = append(errorMessages, fmt.Sprintf("wrong %s, available services are: %v", serviceProperty, availableAWSServices))
	}
	region := ic.Region()
	if region == "" {
		errorMessages = append(errorMessages, fmt.Sprintf("%s cannot be empty", regionProperty))
	}

	if isBedrock(service) {
		model := ic.Model()
		if model == "" || !ic.validatAvailableAWSSetting(model, availableAWSBedrockModels) {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong %s, available models are: %v", modelProperty, availableAWSBedrockModels))
		}
		endpoint := ic.Endpoint()
		if endpoint != "" {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong configuration: %s, not applicable to %s", endpoint, service))
		}
	}

	if isSagemaker(service) {
		endpoint := ic.Endpoint()
		if endpoint == "" {
			errorMessages = append(errorMessages, fmt.Sprintf("%s cannot be empty", endpointProperty))
		}
		model := ic.Model()
		if model != "" {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong configuration: %s, not applicable to %s. did you mean %s", modelProperty, service, targetModelProperty))
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

func (ic *classSettings) validatAvailableAWSSetting(value string, availableValues []string) bool {
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

	value, ok := ic.cfg.ClassByModuleName("text2vec-aws")[name]
	if ok {
		asString, ok := value.(string)
		if ok {
			return asString
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
		"indexing")
}

// Aws params
func (ic *classSettings) Service() string {
	return ic.getStringProperty(serviceProperty, DefaultService)
}

func (ic *classSettings) Region() string {
	return ic.getStringProperty(regionProperty, "")
}

func (ic *classSettings) Model() string {
	return ic.getStringProperty(modelProperty, "")
}

func (ic *classSettings) Endpoint() string {
	return ic.getStringProperty(endpointProperty, "")
}

func (ic *classSettings) TargetModel() string {
	return ic.getStringProperty(targetModelProperty, "")
}

func (ic *classSettings) TargetVariant() string {
	return ic.getStringProperty(targetVariantProperty, "")
}

func isSagemaker(service string) bool {
	return service == "sagemaker"
}

func isBedrock(service string) bool {
	return service == "bedrock"
}
