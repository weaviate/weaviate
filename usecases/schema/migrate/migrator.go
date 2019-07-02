// Package migrate provides a simple composer tool, which implements the
// Migrator interface and can take in any number of migrators which themselves
// have to implement the interface
package migrate

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// Migrator represents both the input and output interface of the Composer
type Migrator interface {
	AddClass(ctx context.Context, kind kind.Kind, class *models.SemanticSchemaClass) error
	DropClass(ctx context.Context, kind kind.Kind, className string) error
	UpdateClass(ctx context.Context, kind kind.Kind, className string,
		newClassName *string, newKeywords *models.SemanticSchemaKeywords) error

	AddProperty(ctx context.Context, kind kind.Kind, className string,
		prop *models.SemanticSchemaClassProperty) error
	DropProperty(ctx context.Context, kind kind.Kind, className string,
		propertyName string) error
	UpdateProperty(ctx context.Context, kind kind.Kind, className string,
		propName string, newName *string, newKeywords *models.SemanticSchemaKeywords) error
	UpdatePropertyAddDataType(ctx context.Context, kind kind.Kind, className string, propName string, newDataType string) error
}
