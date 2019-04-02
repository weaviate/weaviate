/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package etcd

import (
	"context"
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/go-openapi/strfmt"
)

func (l *etcdSchemaManager) GetSchema() schema.Schema {
	return schema.Schema{
		Actions: l.schemaState.ActionSchema,
		Things:  l.schemaState.ThingSchema,
	}
}

func (l *etcdSchemaManager) UpdateMeta(ctx context.Context, kind kind.Kind,
	atContext strfmt.URI, maintainer strfmt.Email, name string) error {
	semanticSchema := l.schemaState.SchemaFor(kind)
	semanticSchema.AtContext = atContext
	semanticSchema.Maintainer = maintainer
	semanticSchema.Name = name

	return l.saveSchema(ctx)
}

func (l *etcdSchemaManager) SetContextionary(context contextionary.Contextionary) {
	l.contextionary = context
}

func (l *etcdSchemaManager) AddClass(ctx context.Context, kind kind.Kind, class *models.SemanticSchemaClass) error {
	err := l.validateCanAddClass(kind, class)
	if err != nil {
		return err
	} else {
		semanticSchema := l.schemaState.SchemaFor(kind)
		semanticSchema.Classes = append(semanticSchema.Classes, class)
		err := l.saveSchema(ctx)
		if err != nil {
			return err
		}

		return l.connectorMigrator.AddClass(ctx, kind, class)
	}
}

func (l *etcdSchemaManager) DropClass(ctx context.Context, kind kind.Kind, className string) error {
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

	err := l.saveSchema(ctx)
	if err != nil {
		return err
	}

	return l.connectorMigrator.DropClass(ctx, kind, className)
}

func (l *etcdSchemaManager) UpdateClass(ctx context.Context, kind kind.Kind, className string, newClassName *string, newKeywords *models.SemanticSchemaKeywords) error {
	semanticSchema := l.schemaState.SchemaFor(kind)

	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	classNameAfterUpdate := className
	keywordsAfterUpdate := class.Keywords

	// First validate the request
	if newClassName != nil {
		err = l.validateClassNameUniqueness(*newClassName)
		classNameAfterUpdate = *newClassName
		if err != nil {
			return err
		}
	}

	if newKeywords != nil {
		keywordsAfterUpdate = *newKeywords
	}

	// Validate name / keywords in contextionary
	if err = l.validateClassNameOrKeywordsCorrect(kind, classNameAfterUpdate, keywordsAfterUpdate); err != nil {
		return err
	}

	// Validated! Now apply the changes.
	class.Class = classNameAfterUpdate
	class.Keywords = keywordsAfterUpdate

	err = l.saveSchema(ctx)

	if err != nil {
		return nil
	}

	return l.connectorMigrator.UpdateClass(ctx, kind, className, newClassName, newKeywords)
}

func (l *etcdSchemaManager) AddProperty(ctx context.Context, kind kind.Kind, className string, prop *models.SemanticSchemaClassProperty) error {
	semanticSchema := l.schemaState.SchemaFor(kind)
	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	err = l.validateCanAddProperty(prop, class)
	if err != nil {
		return err
	}

	class.Properties = append(class.Properties, prop)

	err = l.saveSchema(ctx)

	if err != nil {
		return nil
	}

	return l.connectorMigrator.AddProperty(ctx, kind, className, prop)
}

func (l *etcdSchemaManager) UpdateProperty(ctx context.Context, kind kind.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaKeywords) error {
	semanticSchema := l.schemaState.SchemaFor(kind)
	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	prop, err := schema.GetPropertyByName(class, propName)
	if err != nil {
		return err
	}

	propNameAfterUpdate := propName
	keywordsAfterUpdate := prop.Keywords

	if newName != nil {
		// verify uniqueness
		err = validatePropertyNameUniqueness(*newName, class)
		propNameAfterUpdate = *newName
		if err != nil {
			return err
		}
	}

	if newKeywords != nil {
		keywordsAfterUpdate = *newKeywords
	}

	// Validate name / keywords in contextionary
	l.validatePropertyNameOrKeywordsCorrect(className, propNameAfterUpdate, keywordsAfterUpdate)

	// Validated! Now apply the changes.
	prop.Name = propNameAfterUpdate
	prop.Keywords = keywordsAfterUpdate

	err = l.saveSchema(ctx)

	if err != nil {
		return nil
	}

	return l.connectorMigrator.UpdateProperty(ctx, kind, className, propName, newName, newKeywords)
}

func (l *etcdSchemaManager) UpdatePropertyAddDataType(ctx context.Context, kind kind.Kind, className string, propName string, newDataType string) error {
	semanticSchema := l.schemaState.SchemaFor(kind)
	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	prop, err := schema.GetPropertyByName(class, propName)
	if err != nil {
		return err
	}

	if dataTypeAlreadyContained(prop.AtDataType, newDataType) {
		return nil
	}

	prop.AtDataType = append(prop.AtDataType, newDataType)
	err = l.saveSchema(ctx)

	if err != nil {
		return nil
	}

	return l.connectorMigrator.UpdatePropertyAddDataType(ctx, kind, className, propName, newDataType)
}

func dataTypeAlreadyContained(haystack []string, needle string) bool {
	for _, hay := range haystack {
		if hay == needle {
			return true
		}
	}
	return false
}

func (l *etcdSchemaManager) DropProperty(ctx context.Context, kind kind.Kind, className string, propName string) error {
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

	err = l.saveSchema(ctx)

	if err != nil {
		return nil
	}

	return l.connectorMigrator.DropProperty(ctx, kind, className, propName)
}

func (l *etcdSchemaManager) GetPropsOfType(propType string) []schema.ClassAndProperty {
	var result []schema.ClassAndProperty

	result = append(
		extractAllOfPropType(l.schemaState.ActionSchema.Classes, propType),
		extractAllOfPropType(l.schemaState.ThingSchema.Classes, propType)...,
	)

	return result
}

func extractAllOfPropType(classes []*models.SemanticSchemaClass, propType string) []schema.ClassAndProperty {
	var result []schema.ClassAndProperty
	for _, class := range classes {
		for _, prop := range class.Properties {
			if prop.AtDataType[0] == propType {
				result = append(result, schema.ClassAndProperty{
					ClassName:    schema.ClassName(class.Class),
					PropertyName: schema.PropertyName(prop.Name),
				})
			}
		}
	}

	return result
}
