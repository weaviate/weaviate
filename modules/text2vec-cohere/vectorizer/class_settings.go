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
	DefaultBaseURL               = "https://api.cohere.ai"
	DefaultCohereModel           = "embed-multilingual-v3.0"
	DefaultTruncate              = "END"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
)

var (
	availableCohereModels = []string{
		"medium",
		"large", "small", "multilingual-22-12",
		"embed-english-v2.0", "embed-english-light-v2.0", "embed-multilingual-v2.0",
		"embed-english-v3.0", "embed-english-light-v3.0", "embed-multilingual-v3.0", "embed-multilingual-light-v3.0",
	}
	experimetnalCohereModels = []string{"multilingual-2210-alpha"}
	availableTruncates       = []string{"NONE", "START", "END", "LEFT", "RIGHT"}
)

type classSettings struct {
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg}
}

func (cs *classSettings) PropertyIndexed(propName string) bool {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return DefaultPropertyIndexed
	}

	vcn, ok := cs.cfg.Property(propName)["skip"]
	if !ok {
		return DefaultPropertyIndexed
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultPropertyIndexed
	}

	return !asBool
}

func (cs *classSettings) VectorizePropertyName(propName string) bool {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return DefaultVectorizePropertyName
	}
	vcn, ok := cs.cfg.Property(propName)["vectorizePropertyName"]
	if !ok {
		return DefaultVectorizePropertyName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultVectorizePropertyName
	}

	return asBool
}

func (cs *classSettings) Model() string {
	return cs.getProperty("model", DefaultCohereModel)
}

func (cs *classSettings) Truncate() string {
	return cs.getProperty("truncate", DefaultTruncate)
}

func (cs *classSettings) BaseURL() string {
	return cs.getProperty("baseURL", DefaultBaseURL)
}

func (cs *classSettings) VectorizeClassName() bool {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return DefaultVectorizeClassName
	}

	vcn, ok := cs.cfg.Class()["vectorizeClassName"]
	if !ok {
		return DefaultVectorizeClassName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultVectorizeClassName
	}

	return asBool
}

func (cs *classSettings) Validate(class *models.Class) error {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	model := cs.Model()
	if !cs.validateCohereSetting(model, append(availableCohereModels, experimetnalCohereModels...)) {
		return errors.Errorf("wrong Cohere model name, available model names are: %v", availableCohereModels)
	}
	truncate := cs.Truncate()
	if !cs.validateCohereSetting(truncate, availableTruncates) {
		return errors.Errorf("wrong truncate type, available types are: %v", availableTruncates)
	}

	err := cs.validateIndexState(class, cs)
	if err != nil {
		return err
	}

	return nil
}

func (cs *classSettings) validateCohereSetting(value string, availableValues []string) bool {
	for i := range availableValues {
		if value == availableValues[i] {
			return true
		}
	}
	return false
}

func (cs *classSettings) getProperty(name, defaultValue string) string {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return defaultValue
	}

	model, ok := cs.cfg.Class()[name]
	if ok {
		asString, ok := model.(string)
		if ok {
			if name == "truncate" {
				return asString
			} else {
				return strings.ToLower(asString)
			}
		}
	}

	return defaultValue
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
		"indexing")
}
