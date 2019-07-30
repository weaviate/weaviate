//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
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
func (m *Manager) validateClassNameAndKeywords(ctx context.Context, knd kind.Kind, className string, keywords models.Keywords) error {
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
func (m *Manager) validatePropertyNameAndKeywords(ctx context.Context, className string, propertyName string, keywords models.Keywords) error {
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
