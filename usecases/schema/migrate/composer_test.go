package migrate

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_Composer(t *testing.T) {
	m1 := &mockMigrator{}
	m2 := &mockMigrator{}

	composer := New(m1, m2)

	t.Run("adding a class - no errors", func(t *testing.T) {
		ctx := context.Background()
		class := &models.SemanticSchemaClass{Class: "Foo"}
		kind := kind.Thing
		m1.On("AddClass", ctx, kind, class).Return(nil)
		m2.On("AddClass", ctx, kind, class).Return(nil)

		err := composer.AddClass(ctx, kind, class)
		assert.Nil(t, err)
		m1.AssertExpectations(t)
		m2.AssertExpectations(t)
	})
}

type mockMigrator struct {
	mock.Mock
}

func (m *mockMigrator) AddClass(ctx context.Context, kind kind.Kind, class *models.SemanticSchemaClass) error {
	args := m.Called(ctx, kind, class)
	return args.Error(0)
}

func (m *mockMigrator) DropClass(ctx context.Context, kind kind.Kind, className string) error {
	args := m.Called(ctx, kind, className)
	return args.Error(0)
}

func (m *mockMigrator) UpdateClass(ctx context.Context, kind kind.Kind, className string, newClassName *string, newKeywords *models.SemanticSchemaKeywords) error {
	args := m.Called(ctx, kind, className, newClassName, newKeywords)
	return args.Error(0)
}

func (m *mockMigrator) AddProperty(ctx context.Context, kind kind.Kind, className string, prop *models.SemanticSchemaClassProperty) error {
	args := m.Called(ctx, kind, className, prop)
	return args.Error(0)
}

func (m *mockMigrator) DropProperty(ctx context.Context, kind kind.Kind, className string, propertyName string) error {
	args := m.Called(ctx, kind, className, propertyName)
	return args.Error(0)
}

func (m *mockMigrator) UpdateProperty(ctx context.Context, kind kind.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaKeywords) error {
	args := m.Called(ctx, kind, className, propName, newName, newKeywords)
	return args.Error(0)
}

func (m *mockMigrator) UpdatePropertyAddDataType(ctx context.Context, kind kind.Kind, className string, propName string, newDataType string) error {
	args := m.Called(ctx, kind, className, propName, newDataType)
	return args.Error(0)
}
