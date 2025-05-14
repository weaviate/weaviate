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

import "github.com/stretchr/testify/mock"

type MockReplicationsDeleter struct {
	mock.Mock
}

func NewMockReplicationsDeleter() *MockReplicationsDeleter {
	return &MockReplicationsDeleter{}
}

func (m *MockReplicationsDeleter) DeleteReplicationsByCollection(collection string) error {
	args := m.Called(collection)
	return args.Error(0)
}

func (m *MockReplicationsDeleter) DeleteReplicationsByTenants(collection string, tenants []string) error {
	args := m.Called(collection, tenants)
	return args.Error(0)
}
