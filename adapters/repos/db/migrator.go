package db

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

type Migrator struct {
	db *DB
}

func (m *Migrator) AddClass(ctx context.Context, kind kind.Kind, class *models.Class) error {
	panic("not implemented") // TODO: Implement
}

func (m *Migrator) DropClass(ctx context.Context, kind kind.Kind, className string) error {
	panic("not implemented") // TODO: Implement
}

func (m *Migrator) UpdateClass(ctx context.Context, kind kind.Kind, className string, newClassName *string, newKeywords *models.Keywords) error {
	panic("not implemented") // TODO: Implement
}

func (m *Migrator) AddProperty(ctx context.Context, kind kind.Kind, className string, prop *models.Property) error {
	panic("not implemented") // TODO: Implement
}

func (m *Migrator) DropProperty(ctx context.Context, kind kind.Kind, className string, propertyName string) error {
	panic("not implemented") // TODO: Implement
}

func (m *Migrator) UpdateProperty(ctx context.Context, kind kind.Kind, className string, propName string, newName *string, newKeywords *models.Keywords) error {
	panic("not implemented") // TODO: Implement
}

func (m *Migrator) UpdatePropertyAddDataType(ctx context.Context, kind kind.Kind, className string, propName string, newDataType string) error {
	panic("not implemented") // TODO: Implement
}

func NewMigrator(db *DB) *Migrator {
	return &Migrator{db: db}
}
