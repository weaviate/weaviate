//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/camelcase"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
)

func (m *Manager) validateClassNameUniqueness(className string) error {
	for _, otherClass := range m.state.SchemaFor().Classes {
		if className == otherClass.Class {
			return fmt.Errorf("Name '%s' already used as a name for an Object class", className)
		}
	}

	return nil
}

// Check that the format of the name is correct
// Check that the name is acceptable according to the contextionary
func (m *Manager) validateClassName(ctx context.Context, className string, vectorizeClass bool) error {
	_, err := schema.ValidateClassName(className)
	if err != nil {
		return err
	}

	// class name
	if !vectorizeClass {
		// if the user chooses not to vectorize the class, we don't need to check
		// if its c11y-valid or not
		return nil
	}

	// TODO: everything that follows should be part of the text2vec-contextionary
	// module
	camelParts := camelcase.Split(className)
	stopWordsFound := 0
	for _, part := range camelParts {
		word := strings.ToLower(part)
		sw, err := m.stopwordDetector.IsStopWord(ctx, word)
		if err != nil {
			return fmt.Errorf("could not check stopword: %v", err)
		}

		if sw {
			stopWordsFound++
			continue
		}

		present, err := m.c11yClient.IsWordPresent(ctx, word)
		if err != nil {
			return fmt.Errorf("could not check word presence: %v", err)
		}

		if !present {
			return fmt.Errorf("Could not find the word '%s' from the class name '%s' in the contextionary. Consider using keywords to define the semantic meaning of this class.", word, className)
		}
	}

	if len(camelParts) == stopWordsFound {
		return fmt.Errorf("the className '%s' only consists of stopwords and is therefore not a valid class name. "+
			"Make sure at least one word in the classname is not a stop word", className)
	}

	return nil
}

func validatePropertyNameUniqueness(propertyName string, class *models.Class) error {
	for _, otherProperty := range class.Properties {
		if propertyName == otherProperty.Name {
			return fmt.Errorf("Name '%s' already in use as a property name for class '%s'", propertyName, class.Class)
		}
	}

	return nil
}

// Check that the format of the name is correct
// Check that the name is acceptable according to the contextionary
func (m *Manager) validatePropertyName(ctx context.Context, className string,
	propertyName string, moduleConfig interface{}) error {
	_, err := schema.ValidatePropertyName(propertyName)
	if err != nil {
		return err
	}

	// TODO: everything below this line is specific to the text2vec-contextionary
	// module and should be moved there
	if !m.vectorizePropertyName(moduleConfig) {
		// user does not want to vectorize this property name, so we don't have to
		// validate it
		return nil
	}

	camelParts := camelcase.Split(propertyName)
	stopWordsFound := 0
	for _, part := range camelParts {
		word := strings.ToLower(part)
		sw, err := m.stopwordDetector.IsStopWord(ctx, word)
		if err != nil {
			return fmt.Errorf("could not check stopword: %v", err)
		}

		if sw {
			stopWordsFound++
			continue
		}

		present, err := m.c11yClient.IsWordPresent(ctx, word)
		if err != nil {
			return fmt.Errorf("could not check word presence: %v", err)
		}

		if !present {
			return fmt.Errorf("Could not find the word '%s' from the property '%s' in the class name '%s' in the contextionary. Consider using keywords to define the semantic meaning of this class.", word, propertyName, className)
		}
	}

	if len(camelParts) == stopWordsFound {
		return fmt.Errorf("the propertyName '%s' only consists of stopwords and is therefore not a valid property name. "+
			"Make sure at least one word in the propertyname is not a stop word", propertyName)
	}

	return nil
}

// TODO: This validates text2vec-contextionary specific logic
func (m *Manager) vectorizePropertyName(in interface{}) bool {
	defaultValue := false

	if in == nil {
		return defaultValue
	}

	asMap, ok := in.(map[string]interface{})
	if !ok {
		return defaultValue
	}

	t2vc, ok := asMap["text2vec-contextionary"]
	if !ok {
		return defaultValue
	}

	t2vcMap, ok := t2vc.(map[string]interface{})
	if !ok {
		return defaultValue
	}

	vec, ok := t2vcMap["vectorizePropertyName"]
	if !ok {
		return defaultValue
	}

	asBool, ok := vec.(bool)
	if !ok {
		return defaultValue
	}

	return asBool
}

// TODO: This validates text2vec-contextionary specific logic
func (m *Manager) indexPropertyInVectorIndex(in interface{}) bool {
	defaultValue := true

	if in == nil {
		return defaultValue
	}

	asMap, ok := in.(map[string]interface{})
	if !ok {
		return defaultValue
	}

	t2vc, ok := asMap["text2vec-contextionary"]
	if !ok {
		return defaultValue
	}

	t2vcMap, ok := t2vc.(map[string]interface{})
	if !ok {
		return defaultValue
	}

	skip, ok := t2vcMap["skip"]
	if !ok {
		return defaultValue
	}

	skipBool, ok := skip.(bool)
	if !ok {
		return defaultValue
	}

	return !skipBool
}

// TODO: This validates text2vec-contextionary specific logic
//
// Generally the user is free to "noindex" as many properties as they want.
// However, we need to be able to build a vector from every object imported. If
// the user decides not to index the classname and additionally no-indexes all
// usabled (i.e. text/string) properties, we know for a fact, that we won't be
// able to build a vector. In this case we should fail early and deny
// validation.
func (m *Manager) validatePropertyIndexState(ctx context.Context, class *models.Class) error {
	if class.Vectorizer != "text2vec-contextionary" {
		// this is text2vec-contextionary specific, so skip in other cases
		return nil
	}

	if VectorizeClassName(class) {
		// if the user chooses to vectorize the classname, vector-building will
		// always be possible, no need to investigate further

		return nil
	}

	// search if there is at least one indexed, string/text prop. If found pass validation
	for _, prop := range class.Properties {
		if len(prop.DataType) < 1 {
			return fmt.Errorf("property %s must have at least one datatype, got %v", prop.Name, prop.DataType)
		}

		if prop.DataType[0] != string(schema.DataTypeString) &&
			prop.DataType[0] != string(schema.DataTypeText) {
			continue
		}

		if m.indexPropertyInVectorIndex(prop.ModuleConfig) {
			// found at least one, this is a valid schema
			return nil
		}
	}

	return fmt.Errorf("invalid properties: didn't find a single property which is of type string or text " +
		"and is not excluded from indexing. In addition the class name is excluded from vectorization as well, " +
		"meaning that it cannot be used to determine the vector position. To fix this, set " +
		"'vectorizeClassName' to true if the class name is contextionary-valid. Alternatively add at least " +
		"contextionary-valid text/string property which is not excluded from indexing.")
}

func (m *Manager) validateVectorSettings(ctx context.Context, class *models.Class) error {
	if err := m.validateVectorizer(ctx, class); err != nil {
		return err
	}

	if err := m.validateVectorIndex(ctx, class); err != nil {
		return err
	}

	return nil
}

func (m *Manager) validateVectorizer(ctx context.Context, class *models.Class) error {
	switch class.Vectorizer {
	case config.VectorizerModuleNone, config.VectorizerModuleText2VecContextionary:
		return nil
	default:
		return errors.Errorf("unrecognized or unsupported vectorizer %q", class.Vectorizer)
	}
}

func (m *Manager) validateVectorIndex(ctx context.Context, class *models.Class) error {
	switch class.VectorIndexType {
	case "hnsw":
		return nil
	default:
		return errors.Errorf("unrecognized or unsupported vectorIndexType %q",
			class.VectorIndexType)
	}
}
