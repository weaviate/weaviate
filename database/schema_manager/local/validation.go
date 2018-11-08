package local

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/fatih/camelcase"
)

// Validate if a class can be added to the schema
func (l *localSchemaManager) validateCanAddClass(knd kind.Kind, class *models.SemanticSchemaClass) error {
	// First check if there is a name clash.
	err := l.validateClassNameUniqueness(class.Class)
	if err != nil {
		return err
	}

	err = l.validateClassNameOrKeywordsCorrect(knd, class.Class, class.Keywords)
	if err != nil {
		return err
	}

	// Check properties
	foundNames := map[string]bool{}
	for _, property := range class.Properties {
		err = l.validatePropertyNameOrKeywordsCorrect(class.Class, property.Name, property.Keywords)
		if err != nil {
			return err
		}

		if foundNames[property.Name] == true {
			return fmt.Errorf("Name '%s' already in use as a property name for class '%s'", property.Name, class.Class)
		}

		foundNames[property.Name] = true

		// Validate data type of property.
		schema := l.GetSchema()
		err, _ := (&schema).FindPropertyDataType(property.AtDataType)
		if err != nil {
			return fmt.Errorf("Data type fo property '%s' is invalid; %v", property.Name, err)
		}
	}

	// all is fine!
	return nil
}

func (l *localSchemaManager) validateClassNameUniqueness(className string) error {
	for _, otherClass := range l.schemaState.SchemaFor(kind.ACTION_KIND).Classes {
		if className == otherClass.Class {
			return fmt.Errorf("Name '%s' already used as a name for an Action class", className)
		}
	}

	for _, otherClass := range l.schemaState.SchemaFor(kind.THING_KIND).Classes {
		if className == otherClass.Class {
			return fmt.Errorf("Name '%s' already used as a name for a Thing class", className)
		}
	}

	return nil
}

// Check that the format of the name is correct
// Check that the name is acceptable according to the contextionary
func (l *localSchemaManager) validateClassNameOrKeywordsCorrect(knd kind.Kind, className string, keywords models.SemanticSchemaKeywords) error {
	err, _ := schema.ValidateClassName(className)
	if err != nil {
		return err
	}

	if len(keywords) > 0 {
		for _, keyword := range keywords {
			word := strings.ToLower(keyword.Keyword)
			if l.contextionary != nil {
				idx := l.contextionary.WordToItemIndex(word)
				if !idx.IsPresent() {
					return fmt.Errorf("Could not find the keyword '%s' for class '%s' in the contextionary", word, className)
				}
			}
		}
	} else {
		camelParts := camelcase.Split(className)
		for _, part := range camelParts {
			word := strings.ToLower(part)
			if l.contextionary != nil {
				idx := l.contextionary.WordToItemIndex(word)
				if !idx.IsPresent() {
					return fmt.Errorf("Could not find the word '%s' from the class name '%s' in the contextionary. Consider using keywords to define the semantic meaning of this class.", word, className)
				}
			}
		}
	}

	return nil
}

// Verify if we can add the passed property to the passed in class.
// We need the total schema state to be able to check that references etc are valid.
func (l *localSchemaManager) validateCanAddProperty(property *models.SemanticSchemaClassProperty, class *models.SemanticSchemaClass) error {
	// Verify format of property.
	err, _ := schema.ValidatePropertyName(property.Name)
	if err != nil {
		return err
	}

	// First check if there is a name clash.
	err = validatePropertyNameUniqueness(property.Name, class)
	if err != nil {
		return err
	}

	err = l.validatePropertyNameOrKeywordsCorrect(class.Class, property.Name, property.Keywords)
	if err != nil {
		return err
	}

	// Validate data type of property.
	schema := l.GetSchema()
	_, err = (&schema).FindPropertyDataType(property.AtDataType)
	if err != nil {
		return fmt.Errorf("Data type fo property '%s' is invalid; %v", property.Name, err)
	}

	// all is fine!
	return nil
}

func validatePropertyNameUniqueness(propertyName string, class *models.SemanticSchemaClass) error {
	for _, otherProperty := range class.Properties {
		if propertyName == otherProperty.Name {
			return fmt.Errorf("Name '%s' already in use as a property name for class '%s'", propertyName, class.Class)
		}
	}

	return nil
}

// Check that the format of the name is correct
// Check that the name is acceptable according to the contextionary
func (l *localSchemaManager) validatePropertyNameOrKeywordsCorrect(className string, propertyName string, keywords models.SemanticSchemaKeywords) error {
	err, _ := schema.ValidatePropertyName(propertyName)
	if err != nil {
		return err
	}

	if len(keywords) > 0 {
		for _, keyword := range keywords {
			word := strings.ToLower(keyword.Keyword)
			if l.contextionary != nil {
				idx := l.contextionary.WordToItemIndex(word)
				if !idx.IsPresent() {
					return fmt.Errorf("Could not find the keyword '%s' for property '%s' in the class '%s' in the contextionary", word, propertyName, className)
				}
			}
		}
	} else {
		camelParts := camelcase.Split(propertyName)
		for _, part := range camelParts {
			word := strings.ToLower(part)
			if l.contextionary != nil {
				idx := l.contextionary.WordToItemIndex(word)
				if !idx.IsPresent() {
					return fmt.Errorf("Could not find the word '%s' from the property '%s' in the class name '%s' in the contextionary. Consider using keywords to define the semantic meaning of this class.", word, propertyName, className)
				}
			}
		}
	}

	return nil
}
