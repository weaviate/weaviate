//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/camelcase"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/network/crossrefs"
)

func (m *Manager) validateClassNameUniqueness(className string) error {
	for _, otherClass := range m.state.SchemaFor(kind.Action).Classes {
		if className == otherClass.Class {
			return fmt.Errorf("Name '%s' already used as a name for an Action class", className)
		}
	}

	for _, otherClass := range m.state.SchemaFor(kind.Thing).Classes {
		if className == otherClass.Class {
			return fmt.Errorf("Name '%s' already used as a name for a Thing class", className)
		}
	}

	return nil
}

// Check that the format of the name is correct
// Check that the name is acceptable according to the contextionary
func (m *Manager) validateClassNameAndKeywords(ctx context.Context, knd kind.Kind, className string, keywords models.Keywords, vectorizeClass bool) error {
	_, err := schema.ValidateClassName(className)
	if err != nil {
		return err
	}

	// keywords
	stopWordsFound := 0
	for _, keyword := range keywords {
		word := strings.ToLower(keyword.Keyword)
		sw, err := m.stopwordDetector.IsStopWord(ctx, word)
		if err != nil {
			return fmt.Errorf("could not check stopword: %v", err)
		}

		if sw {
			stopWordsFound++
			continue
		}

		if err := validateWeight(keyword); err != nil {
			return fmt.Errorf("invalid keyword %s: %v", keyword.Keyword, err)
		}

		present, err := m.c11yClient.IsWordPresent(ctx, word)
		if err != nil {
			return fmt.Errorf("could not check word presence: %v", err)
		}

		if !present {
			return fmt.Errorf("Could not find the keyword '%s' for class '%s' in the contextionary", word, className)
		}
	}
	if len(keywords) > 0 && len(keywords) == stopWordsFound {
		return fmt.Errorf("all keywords for class '%s' are stopwords and are therefore not a valid list of keywords. "+
			"Make sure at least one keyword in the list is not a stop word", className)
	}

	//class name
	if vectorizeClass == false {
		// if the user chooses not to vectorize the class, we don't need to check
		// if its c11y-valid or not
		return nil
	}

	camelParts := camelcase.Split(className)
	stopWordsFound = 0
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

func validateWeight(keyword *models.KeywordsItems0) error {
	if 0 <= keyword.Weight && keyword.Weight <= 1 {
		return nil
	}

	return fmt.Errorf("weight must be between 0 and 1, but got %v", keyword.Weight)
}

// Check that the format of the name is correct
// Check that the name is acceptable according to the contextionary
func (m *Manager) validatePropertyNameAndKeywords(ctx context.Context, className string, propertyName string, keywords models.Keywords, vectorizeProperty bool) error {
	_, err := schema.ValidatePropertyName(propertyName)
	if err != nil {
		return err
	}

	stopWordsFound := 0
	for _, keyword := range keywords {
		word := strings.ToLower(keyword.Keyword)
		sw, err := m.stopwordDetector.IsStopWord(ctx, word)
		if err != nil {
			return fmt.Errorf("could not check stopword: %v", err)
		}

		if sw {
			stopWordsFound++
			continue
		}

		if err := validateWeight(keyword); err != nil {
			return fmt.Errorf("invalid keyword %s: %v", keyword.Keyword, err)
		}

		present, err := m.c11yClient.IsWordPresent(ctx, word)
		if err != nil {
			return fmt.Errorf("could not check word presence: %v", err)
		}

		if !present {
			return fmt.Errorf("Could not find the keyword '%s' for property '%s' in the class '%s' in the contextionary", word, propertyName, className)
		}
	}
	if len(keywords) > 0 && len(keywords) == stopWordsFound {
		return fmt.Errorf("all keywords for propertyName '%s' are stopwords and are therefore not a valid list of keywords. "+
			"Make sure at least one keyword in the list is not a stop word", propertyName)
	}

	if vectorizeProperty == false {
		// user does not want to vectorize this property name, so we don't have to
		// validate it
		return nil
	}

	camelParts := camelcase.Split(propertyName)
	stopWordsFound = 0
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

func (m *Manager) validateNetworkCrossRefs(dataTypes []string) error {
	for _, dataType := range dataTypes {
		if !schema.ValidNetworkClassName(dataType) {
			// we don't know anything about the validity of non-network-refs
			// that's the concern of a separate validation
			continue
		}

		if m.network == nil {
			return fmt.Errorf(
				"schema contains network-cross-ref '%s', but no network is configured", dataType)
		}

		peers, err := m.network.ListPeers()
		if err != nil {
			return fmt.Errorf(
				"schema contains network-cross-ref '%s', but peers cannot be retrieved: %s", dataType, err)
		}

		networkClass, err := crossrefs.ParseClass(dataType)
		if err != nil {
			return err
		}

		if ok, err := peers.HasClass(networkClass); !ok {
			return err
		}
	}

	return nil
}

// Generally the user is free to "noindex" as many properties as they want.
// However, we need to be able to build a vector from every object imported. If
// the user decides not to index the classname and additionally no-indexes all
// usabled (i.e. text/string) properties, we know for a fact, that we won't be
// able to build a vector. In this case we should fail early and deny
// validation.
func (m *Manager) validatePropertyIndexState(ctx context.Context, class *models.Class) error {
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

		if prop.Index == nil || *prop.Index == true {
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
