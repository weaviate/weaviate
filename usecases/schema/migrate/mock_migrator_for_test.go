//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package migrate

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/stretchr/testify/mock"
)

type mockMigrator struct {
	mock.Mock
}

func (m *mockMigrator) AddClass(ctx context.Context, kind kind.Kind, class *models.Class) error {
	args := m.Called(ctx, kind, class)
	return args.Error(0)
}

func (m *mockMigrator) DropClass(ctx context.Context, kind kind.Kind, className string) error {
	args := m.Called(ctx, kind, className)
	return args.Error(0)
}

func (m *mockMigrator) UpdateClass(ctx context.Context, kind kind.Kind, className string, newClassName *string, newKeywords *models.Keywords) error {
	args := m.Called(ctx, kind, className, newClassName, newKeywords)
	return args.Error(0)
}

func (m *mockMigrator) AddProperty(ctx context.Context, kind kind.Kind, className string, prop *models.Property) error {
	args := m.Called(ctx, kind, className, prop)
	return args.Error(0)
}

func (m *mockMigrator) DropProperty(ctx context.Context, kind kind.Kind, className string, propertyName string) error {
	args := m.Called(ctx, kind, className, propertyName)
	return args.Error(0)
}

func (m *mockMigrator) UpdateProperty(ctx context.Context, kind kind.Kind, className string, propName string, newName *string, newKeywords *models.Keywords) error {
	args := m.Called(ctx, kind, className, propName, newName, newKeywords)
	return args.Error(0)
}

func (m *mockMigrator) UpdatePropertyAddDataType(ctx context.Context, kind kind.Kind, className string, propName string, newDataType string) error {
	args := m.Called(ctx, kind, className, propName, newDataType)
	return args.Error(0)
}
