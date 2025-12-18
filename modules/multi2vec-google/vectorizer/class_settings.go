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
	"slices"
	"strings"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	locationProperty             = "location"
	projectIDProperty            = "projectId"
	modelIDProperty              = "modelId"
	dimensionsProperty           = "dimensions"
	videoIntervalSecondsProperty = "videoIntervalSeconds"
)

const (
	DefaultVectorizeClassName    = false
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	DefaultApiEndpoint           = "us-central1-aiplatform.googleapis.com"
	DefaultModelID               = "multimodalembedding@001"
)

var (
	defaultDimensions1408         = int64(1408)
	availableDimensions           = []int64{128, 256, 512, defaultDimensions1408}
	defaultVideoIntervalSeconds   = int64(120)
	availableVideoIntervalSeconds = []int64{4, 8, 15, defaultVideoIntervalSeconds}
)

var fields = []string{basesettings.TextFieldsProperty, basesettings.ImageFieldsProperty, basesettings.VideoFieldsProperty}

type classSettings struct {
	base *basesettings.BaseClassMultiModalSettings
	cfg  moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{
		cfg:  cfg,
		base: basesettings.NewBaseClassMultiModalSettingsWithAltNames(cfg, false, "multi2vec-google", []string{"multi2vec-palm"}, nil),
	}
}

// Google params
func (ic *classSettings) Location() string {
	return ic.getStringProperty(locationProperty, "")
}

func (ic *classSettings) ProjectID() string {
	return ic.getStringProperty(projectIDProperty, "")
}

func (ic *classSettings) ModelID() string {
	return ic.getStringProperty(modelIDProperty, DefaultModelID)
}

func (ic *classSettings) Dimensions() int64 {
	return ic.getInt64Property(dimensionsProperty, defaultDimensions1408)
}

func (ic *classSettings) VideoIntervalSeconds() int64 {
	return ic.getInt64Property(videoIntervalSecondsProperty, defaultVideoIntervalSeconds)
}

// CLIP module specific settings
func (ic *classSettings) ImageField(property string) bool {
	return ic.base.ImageField(property)
}

func (ic *classSettings) ImageFieldsWeights() ([]float32, error) {
	return ic.base.ImageFieldsWeights()
}

func (ic *classSettings) TextField(property string) bool {
	return ic.base.TextField(property)
}

func (ic *classSettings) TextFieldsWeights() ([]float32, error) {
	return ic.base.TextFieldsWeights()
}

func (ic *classSettings) VideoField(property string) bool {
	return ic.base.VideoField(property)
}

func (ic *classSettings) VideoFieldsWeights() ([]float32, error) {
	return ic.base.VideoFieldsWeights()
}

func (ic *classSettings) Properties() ([]string, error) {
	return ic.base.VectorizableProperties(fields)
}

func (ic *classSettings) Validate() error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	var errorMessages []string

	model := ic.ModelID()
	location := ic.Location()
	if location == "" {
		errorMessages = append(errorMessages, "location setting needs to be present")
	}

	projectID := ic.ProjectID()
	if projectID == "" {
		errorMessages = append(errorMessages, "projectId setting needs to be present")
	}

	dimensions := ic.Dimensions()
	if !validateSetting(dimensions, availableDimensions) {
		return errors.Errorf("wrong dimensions setting for %s model, available dimensions are: %v", model, availableDimensions)
	}

	videoIntervalSeconds := ic.VideoIntervalSeconds()
	if !validateSetting(videoIntervalSeconds, availableVideoIntervalSeconds) {
		return errors.Errorf("wrong videoIntervalSeconds setting for %s model, available videoIntervalSeconds are: %v", model, availableVideoIntervalSeconds)
	}

	_, videoFieldsOk := ic.base.GetSettings()[basesettings.VideoFieldsProperty]
	if videoFieldsOk && dimensions != defaultDimensions1408 {
		errorMessages = append(errorMessages, fmt.Sprintf("%s support only %d dimensions setting", basesettings.VideoFieldsProperty, defaultDimensions1408))
	}

	if err := ic.base.ValidateMultiModal(fields); err != nil {
		errorMessages = append(errorMessages, err.Error())
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("%s", strings.Join(errorMessages, ", "))
	}

	return nil
}

// Helper methods
func (ic *classSettings) getStringProperty(name, defaultValue string) string {
	return ic.base.GetPropertyAsString(name, defaultValue)
}

func (ic *classSettings) getInt64Property(name string, defaultValue int64) int64 {
	if val := ic.base.GetPropertyAsInt64(name, &defaultValue); val != nil {
		return *val
	}
	return defaultValue
}

func validateSetting[T string | int64](value T, availableValues []T) bool {
	return slices.Contains(availableValues, value)
}
