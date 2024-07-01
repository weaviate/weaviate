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

type MockAddressResolver struct {
	f func(id string) string
}

func NewMockAddressResolver(f func(id string) string) *MockAddressResolver {
	return &MockAddressResolver{f: f}
}

func (m *MockAddressResolver) NodeAddress(id string) string {
	if m.f != nil {
		return m.f(id)
	}
	return "127.0.0.1"
}
