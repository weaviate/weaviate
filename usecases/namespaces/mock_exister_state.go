//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package namespaces

import (
	"testing"

	"github.com/stretchr/testify/mock"

	api "github.com/weaviate/weaviate/cluster/proto/api"
)

// NewMockExisterInState returns an Exister whose GetNamespace reports each
// named namespace in the given state and every other name as missing.
func NewMockExisterInState(t *testing.T, states map[string]api.NamespaceState) *MockExister {
	t.Helper()
	m := &MockExister{}
	m.Test(t)
	m.On("GetNamespace", mock.AnythingOfType("string")).Return(
		func(name string) api.Namespace {
			return api.Namespace{Name: name, HomeNodes: []string{"node-1"}, State: states[name]}
		},
		func(name string) bool {
			_, ok := states[name]
			return ok
		},
	).Maybe()
	return m
}
