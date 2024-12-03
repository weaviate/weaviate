package vectorizer

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	DefaultVoyageAIModel = "voyage-large-2"
	DefaultBaseURL       = "https://api.voyageai.com/v1"
)

type classSettings struct {
	cfg  moduletools.ClassConfig
	base *basesettings.BaseClassSettings
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, base: basesettings.NewBaseClassSettings(cfg)}
}

func (cs *classSettings) Validate(class *models.Class) error {
	if cs.cfg == nil {
		return errors.New("empty config")
	}

	if err := cs.validateIndexState(class); err != nil {
		return err
	}

	return nil
}

func (cs *classSettings) validateIndexState(class *models.Class) error {
	// search if there is at least one indexed field
	for _, prop := range class.Properties {
		if len(prop.DataType) < 1 {
			return errors.Errorf("property %s must have at least one datatype: "+
				"got %v", prop.Name, prop.DataType)
		}

		if cs.ImageField(prop.Name) || cs.TextField(prop.Name) {
			return nil
		}
	}

	return fmt.Errorf("no valid image or text properties found in schema")
}

func (cs *classSettings) getProperty(name, defaultValue string) string {
	return cs.base.GetPropertyAsString(name, defaultValue)
}

func (cs *classSettings) Model() string {
	return cs.getProperty("model", DefaultVoyageAIModel)
}

func (cs *classSettings) BaseURL() string {
	return cs.getProperty("baseURL", DefaultBaseURL)
}

func (cs *classSettings) ImageField(property string) bool {
	return cs.field("imageFields", property)
}

func (cs *classSettings) TextField(property string) bool {
	return cs.field("textFields", property)
}

func (cs *classSettings) ImageFieldsWeights() ([]float32, error) {
	return cs.getFieldsWeights("image")
}

func (cs *classSettings) TextFieldsWeights() ([]float32, error) {
	return cs.getFieldsWeights("text")
}

func (cs *classSettings) field(name, property string) bool {
	if cs.cfg == nil {
		return false
	}

	fields, ok := cs.cfg.Class()[name]
	if !ok {
		return false
	}

	fieldsArray, ok := fields.([]interface{})
	if !ok {
		return false
	}

	for _, value := range fieldsArray {
		if value.(string) == property {
			return true
		}
	}

	return false
}

func (cs *classSettings) getFieldsWeights(name string) ([]float32, error) {
	weights, ok := cs.getWeights(name)
	if ok {
		return cs.getWeightsArray(weights)
	}
	return nil, nil
}

func (cs *classSettings) getWeights(name string) ([]interface{}, bool) {
	weights, ok := cs.cfg.Class()["weights"]
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

func (cs *classSettings) getWeightsArray(weights []interface{}) ([]float32, error) {
	weightsArray := make([]float32, len(weights))
	for i := range weights {
		weight, err := cs.base.GetNumber(weights[i])
		if err != nil {
			return nil, err
		}
		weightsArray[i] = weight
	}
	return weightsArray, nil
}
