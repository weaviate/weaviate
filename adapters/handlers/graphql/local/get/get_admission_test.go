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

package get

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	test_helper "github.com/weaviate/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/queryadmission"
)

// TestGetClass_AdmissionShedMapsToRateLimit pins that ErrOverloaded maps to
// "429 Too many requests" (not a generic 500) at the GraphQL Get ingress.
func TestGetClass_AdmissionShedMapsToRateLimit(t *testing.T) {
	t.Parallel()
	resolver := newMockResolver()

	expectedParams := dto.GetParams{
		ClassName:  "SomeAction",
		Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
	}

	// Wrapped, not the raw sentinel, to also exercise errors.Is traversal.
	shed := fmt.Errorf("shard search: %w", queryadmission.ErrOverloaded)
	resolver.On("GetClass", expectedParams).
		Return(test_helper.EmptyList(), shed).Once()

	result := resolver.Resolve("{ Get { SomeAction { intField } } }")

	require.Len(t, result.Errors, 1)
	require.Contains(t, result.Errors[0].Error(), "429 Too many requests",
		"admission shed must surface as the rate-limit error at the GraphQL ingress")
	require.NotContains(t, result.Errors[0].Error(), "node overloaded",
		"raw ErrOverloaded message must not leak to the client")
	resolver.AssertExpectations(t)
}
