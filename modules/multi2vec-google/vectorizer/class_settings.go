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

type classSettings struct {
	base *basesettings.BaseClassSettings
	cfg  moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{
		cfg:  cfg,
		base: basesettings.NewBaseClassSettingsWithAltNames(cfg, false, "multi2vec-google", []string{"multi2vec-palm"}, nil),
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
	return ic.field("imageFields", property)
}

func (ic *classSettings) ImageFieldsWeights() ([]float32, error) {
	return ic.getFieldsWeights("image")
}

func (ic *classSettings) TextField(property string) bool {
	return ic.field("textFields", property)
}

func (ic *classSettings) TextFieldsWeights() ([]float32, error) {
	return ic.getFieldsWeights("text")
}

func (ic *classSettings) VideoField(property string) bool {
	return ic.field("videoFields", property)
}

func (ic *classSettings) VideoFieldsWeights() ([]float32, error) {
	return ic.getFieldsWeights("video")
}

func (ic *classSettings) Properties() ([]string, error) {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return nil, errors.New("empty config")
	}
	props := make([]string, 0)

	fields := []string{"textFields", "imageFields", "videoFields"}

	for _, field := range fields {
		fields, ok := ic.base.GetSettings()[field]
		if !ok {
			continue
		}

		fieldsArray, ok := fields.([]interface{})
		if !ok {
			return nil, errors.Errorf("%s must be an array", field)
		}

		for _, value := range fieldsArray {
			v, ok := value.(string)
			if !ok {
				return nil, errors.Errorf("%s must be a string", field)
			}
			props = append(props, v)
		}
	}
	return props, nil
}

func (ic *classSettings) field(name, property string) bool {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return false
	}

	fields, ok := ic.base.GetSettings()[name]
	if !ok {
		return false
	}

	fieldsArray, ok := fields.([]interface{})
	if !ok {
		return false
	}

	fieldNames := make([]string, len(fieldsArray))
	for i, value := range fieldsArray {
		fieldNames[i] = value.(string)
	}

	for i := range fieldNames {
		if fieldNames[i] == property {
			return true
		}
	}

	return false
}

func (ic *classSettings) getStringProperty(name, defaultValue string) string {
	return ic.base.GetPropertyAsString(name, defaultValue)
}

func (ic *classSettings) getInt64Property(name string, defaultValue int64) int64 {
	if val := ic.base.GetPropertyAsInt64(name, &defaultValue); val != nil {
		return *val
	}
	return defaultValue
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

	imageFields, imageFieldsOk := ic.cfg.Class()["imageFields"]
	textFields, textFieldsOk := ic.cfg.Class()["textFields"]
	videoFields, videoFieldsOk := ic.cfg.Class()["videoFields"]
	if !imageFieldsOk && !textFieldsOk && !videoFieldsOk {
		errorMessages = append(errorMessages, "textFields or imageFields or videoFields setting needs to be present")
	}

	if videoFieldsOk && dimensions != defaultDimensions1408 {
		errorMessages = append(errorMessages, fmt.Sprintf("videoFields support only %d dimensions setting", defaultDimensions1408))
	}

	if imageFieldsOk {
		imageFieldsCount, err := ic.validateFields("image", imageFields)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}
		err = ic.validateWeights("image", imageFieldsCount)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}
	}

	if textFieldsOk {
		textFieldsCount, err := ic.validateFields("text", textFields)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}
		err = ic.validateWeights("text", textFieldsCount)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}
	}

	if videoFieldsOk {
		videoFieldsCount, err := ic.validateFields("video", videoFields)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}
		err = ic.validateWeights("video", videoFieldsCount)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("%s", strings.Join(errorMessages, ", "))
	}

	return nil
}

func (ic *classSettings) validateFields(name string, fields interface{}) (int, error) {
	fieldsArray, ok := fields.([]interface{})
	if !ok {
		return 0, errors.Errorf("%sFields must be an array", name)
	}

	if len(fieldsArray) == 0 {
		return 0, errors.Errorf("must contain at least one %s field name in %sFields", name, name)
	}

	for _, value := range fieldsArray {
		v, ok := value.(string)
		if !ok {
			return 0, errors.Errorf("%sField must be a string", name)
		}
		if len(v) == 0 {
			return 0, errors.Errorf("%sField values cannot be empty", name)
		}
	}

	return len(fieldsArray), nil
}

func (ic *classSettings) validateWeights(name string, count int) error {
	weights, ok := ic.getWeights(name)
	if ok {
		if len(weights) != count {
			return errors.Errorf("weights.%sFields does not equal number of %sFields", name, name)
		}
		_, err := ic.getWeightsArray(weights)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ic *classSettings) getWeights(name string) ([]interface{}, bool) {
	weights, ok := ic.base.GetSettings()["weights"]
	if ok {
		weightsObject, ok := weights.(map[string]interface{})
		if ok {
			fieldWeights, ok := weightsObject[fmt.Sprintf("%sFields", name)]
			if ok {
				fieldWeightsArray, ok := fieldWeights.([]interface{})
				if ok {
					return fieldWeightsArray, ok
				}
			}
		}
	}

	return nil, false
}

func (ic *classSettings) getWeightsArray(weights []interface{}) ([]float32, error) {
	weightsArray := make([]float32, len(weights))
	for i := range weights {
		weight, err := ic.getNumber(weights[i])
		if err != nil {
			return nil, err
		}
		weightsArray[i] = weight
	}
	return weightsArray, nil
}

func (ic *classSettings) getFieldsWeights(name string) ([]float32, error) {
	weights, ok := ic.getWeights(name)
	if ok {
		return ic.getWeightsArray(weights)
	}
	return nil, nil
}

func (ic *classSettings) getNumber(in interface{}) (float32, error) {
	return ic.base.GetNumber(in)
}

func validateSetting[T string | int64](value T, availableValues []T) bool {
	return slices.Contains(availableValues, value)
}
