//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package migrate

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/mock"
)

type mockMigrator struct {
	mock.Mock
}

func (m *mockMigrator) AddClass(ctx context.Context, class *models.Class) error {
	args := m.Called(ctx, class)
	return args.Error(0)
}

func (m *mockMigrator) DropClass(ctx context.Context, className string) error {
	args := m.Called(ctx, className)
	return args.Error(0)
}

func (m *mockMigrator) UpdateClass(ctx context.Context, className string, newClassName *string) error {
	args := m.Called(ctx, className, newClassName)
	return args.Error(0)
}

func (m *mockMigrator) AddProperty(ctx context.Context, className string, prop *models.Property) error {
	args := m.Called(ctx, className, prop)
	return args.Error(0)
}

func (m *mockMigrator) DropProperty(ctx context.Context, className string, propertyName string) error {
	args := m.Called(ctx, className, propertyName)
	return args.Error(0)
}

func (m *mockMigrator) UpdateProperty(ctx context.Context, className string, propName string, newName *string) error {
	args := m.Called(ctx, className, propName, newName)
	return args.Error(0)
}

func (m *mockMigrator) UpdatePropertyAddDataType(ctx context.Context, className string, propName string, newDataType string) error {
	args := m.Called(ctx, className, propName, newDataType)
	return args.Error(0)
}
