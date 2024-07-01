//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package fakes

import (
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/models"
)

type MockParser struct {
	mock.Mock
}

func NewMockParser() *MockParser {
	return &MockParser{}
}

func (m *MockParser) ParseClass(class *models.Class) error {
	args := m.Called(class)
	return args.Error(0)
}

func (m *MockParser) ParseClassUpdate(class, update *models.Class) (*models.Class, error) {
	args := m.Called(class)
	return update, args.Error(1)
}
