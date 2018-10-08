package local

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema/kind"
)

// TODO extract to library.
// Validate if a class can be added to the schema
func validateCanAddClass(knd kind.Kind, class *models.SemanticSchemaClass, schemaState *localSchemaState) error {
	// First check if there is a name clash.
	fmt.Printf("schema state: %#v\n", schemaState)
	fmt.Printf("action schema: %#v\n", schemaState.SchemaFor(kind.ACTION_KIND))
	for _, otherClass := range schemaState.SchemaFor(kind.ACTION_KIND).Classes {
		if class.Class == otherClass.Class {
			return fmt.Errorf("Class name already used.")
		}
	}
	for _, otherClass := range schemaState.SchemaFor(kind.THING_KIND).Classes {
		if class.Class == otherClass.Class {
			return fmt.Errorf("Class name already used.")
		}
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

func (l *LocalSchemaManager) AddClass(kind kind.Kind, class *models.SemanticSchemaClass) error {
	err := validateCanAddClass(kind, class, &l.schemaState)
	if err != nil {
		return err
	} else {
		// TODO keep it sorted.
		semanticSchema := l.schemaState.SchemaFor(kind)
		semanticSchema.Classes = append(semanticSchema.Classes, class)
		l.saveToDisk()
		return nil
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

	l.saveToDisk()

	return nil
}

func (l *LocalSchemaManager) AddProperty(kind kind.Kind, className string, prop models.SemanticSchemaClassProperty) error {
	return nil
}

func (l *LocalSchemaManager) UpdateProperty(kind kind.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaClassKeywords) error {
	return nil
}

func (l *LocalSchemaManager) DropProperty(kind kind.Kind, className string, propName string) error {
	return nil
}
