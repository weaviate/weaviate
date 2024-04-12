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

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	DefaultJinaAIDocumentType    = "text"
	DefaultJinaAIModel           = "jina-embeddings-v2-base-en"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	DefaultBaseURL               = "https://api.jina.ai"
)

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg)}
}

func (cs *classSettings) Model() string {
	return cs.getProperty("model", DefaultJinaAIModel)
}

func (cs *classSettings) BaseURL() string {
	return cs.getProperty("baseURL", DefaultBaseURL)
}

func (cs *classSettings) Validate(class *models.Class) error {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	if err := cs.BaseClassSettings.ValidateClassSettings(); err != nil {
		return err
	}

	err := cs.validateIndexState(class, cs)
	if err != nil {
		return err
	}

	return nil
}

func (cs *classSettings) getProperty(name, defaultValue string) string {
	return cs.BaseClassSettings.GetPropertyAsString(name, defaultValue)
}

func (cs *classSettings) validateIndexState(class *models.Class, settings ClassSettings) error {
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

		if prop.DataType[0] != string(schema.DataTypeText) {
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
