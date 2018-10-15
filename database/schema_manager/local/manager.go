package local

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
	"github.com/creativesoftwarefdn/weaviate/schema/kind"

	"github.com/go-openapi/strfmt"
)

func validateClassNameUniqueness(className string, schemaState *localSchemaState) error {
	for _, otherClass := range schemaState.SchemaFor(kind.ACTION_KIND).Classes {
		if className == otherClass.Class {
			return fmt.Errorf("Name '%s' already used as a name for an Action class", className)
		}
	}

	for _, otherClass := range schemaState.SchemaFor(kind.THING_KIND).Classes {
		if className == otherClass.Class {
			return fmt.Errorf("Name '%s' already used as a name for a Thing class", className)
		}
	}

	return nil
}

// TODO extract to library.
// Validate if a class can be added to the schema
func validateCanAddClass(knd kind.Kind, class *models.SemanticSchemaClass, schemaState *localSchemaState) error {
	// First check if there is a name clash.
	err := validateClassNameUniqueness(class.Class, schemaState)
	if err != nil {
		return err
	}

	// TODO validate name against contextionary / keywords.
	// TODO validate properties:
	//  - against contextionary / keywords.
	//  - primitive types
	//  - relation types.

	// all is fine!
	return nil
}

func validatePropertyNameUniqueness(propertyName string, class *models.SemanticSchemaClass) error {
	for _, otherProperty := range class.Properties {
		if propertyName == otherProperty.Name {
			return fmt.Errorf("Name '%s' already in use as a property name", propertyName)
		}
	}

	return nil
}

// Verify if we can add the passed property to the passed in class.
// We need the total schema state to be able to check that references etc are valid.
func validateCanAddProperty(property *models.SemanticSchemaClassProperty, class *models.SemanticSchemaClass, schemaState *localSchemaState) error {
	// First check if there is a name clash.
	err := validatePropertyNameUniqueness(property.Name, class)
	if err != nil {
		return err
	}

	// TODO validate name against contextionary / keywords.
	// TODO validate properties:
	//  - against contextionary / keywords.
	//  - primitive types
	//  - relation types.

	// all is fine!
	return nil
}

func (l *LocalSchemaManager) GetSchema() database.Schema {
	return database.Schema{
		Actions: l.schemaState.ActionSchema,
		Things:  l.schemaState.ThingSchema,
	}
}

func (l *LocalSchemaManager) UpdateMeta(kind kind.Kind, atContext strfmt.URI, maintainer strfmt.Email, name string) error {
	semanticSchema := l.schemaState.SchemaFor(kind)
	semanticSchema.AtContext = atContext
	semanticSchema.Maintainer = maintainer
	semanticSchema.Name = name

	return l.saveToDisk()
}

func (l *LocalSchemaManager) AddClass(kind kind.Kind, class *models.SemanticSchemaClass) error {
	err := validateCanAddClass(kind, class, &l.schemaState)
	if err != nil {
		return err
	} else {
		// TODO keep it sorted.
		semanticSchema := l.schemaState.SchemaFor(kind)
		semanticSchema.Classes = append(semanticSchema.Classes, class)
		return l.saveToDisk()
	}
}

func (l *LocalSchemaManager) DropClass(kind kind.Kind, className string) error {
	// TODO keep it sorted.
	semanticSchema := l.schemaState.SchemaFor(kind)

	var classIdx int = -1
	for idx, class := range semanticSchema.Classes {
		if class.Class == className {
			classIdx = idx
			break
		}
	}

	if classIdx == -1 {
		return fmt.Errorf("Could not find class '%s'", className)
	}

	semanticSchema.Classes[classIdx] = semanticSchema.Classes[len(semanticSchema.Classes)-1]
	semanticSchema.Classes[len(semanticSchema.Classes)-1] = nil // to prevent leaking this pointer.
	semanticSchema.Classes = semanticSchema.Classes[:len(semanticSchema.Classes)-1]

	return l.saveToDisk()
}

func (l *LocalSchemaManager) UpdateClass(kind kind.Kind, className string, newClassName *string, newKeywords *models.SemanticSchemaKeywords) error {
	semanticSchema := l.schemaState.SchemaFor(kind)

	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	// First validate the request
	if newClassName != nil {
		err = validateClassNameUniqueness(*newClassName, &l.schemaState)
		if err != nil {
			return err
		}
	}

	if newKeywords != nil {
		// TODO validate keywords in contextionary
	}

	// Validated! Now apply the changes.
	if newClassName != nil {
		class.Class = *newClassName
	}

	if newKeywords != nil {
		class.Keywords = *newKeywords
	}

	return nil
}

func (l *LocalSchemaManager) AddProperty(kind kind.Kind, className string, prop *models.SemanticSchemaClassProperty) error {
	semanticSchema := l.schemaState.SchemaFor(kind)
	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	err = validateCanAddProperty(prop, class, &l.schemaState)
	if err != nil {
		return err
	}

	class.Properties = append(class.Properties, prop)

	return nil
}

func (l *LocalSchemaManager) UpdateProperty(kind kind.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaKeywords) error {
	semanticSchema := l.schemaState.SchemaFor(kind)
	class, err := schema.GetClassByName(semanticSchema, className)

	if err != nil {
		return err
	}

	prop, err := schema.GetPropertyByName(class, propName)
	if err != nil {
		return err
	}

	if newName != nil {
		// verify uniqueness
		err = validatePropertyNameUniqueness(*newName, class)
		if err != nil {
			return err
		}
	}

	if newKeywords != nil {
		// TODO: validate keywords.
	}

	// The updates are validated. Apply them.

	if newName != nil {
		prop.Name = *newName
	}

	if newKeywords != nil {
		prop.Keywords = *newKeywords
	}

	return nil
}

func (l *LocalSchemaManager) DropProperty(kind kind.Kind, className string, propName string) error {
	semanticSchema := l.schemaState.SchemaFor(kind)
	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	var propIdx int = -1
	for idx, prop := range class.Properties {
		if prop.Name == propName {
			propIdx = idx
			break
		}
	}

	if propIdx == -1 {
		return fmt.Errorf("Could not find property '%s'", propName)
	}

	class.Properties[propIdx] = class.Properties[len(class.Properties)-1]
	class.Properties[len(class.Properties)-1] = nil // to prevent leaking this pointer.
	class.Properties = class.Properties[:len(class.Properties)-1]

	return l.saveToDisk()
}
