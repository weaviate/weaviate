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

package searchparams

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHNSWValidate(t *testing.T) {
	t.Parallel()

	assert.NoError(t, (*HNSW)(nil).Validate())
	assert.NoError(t, (&HNSW{}).Validate())

	ef := 32
	assert.NoError(t, (&HNSW{EF: &ef}).Validate())

	bad := 0
	err := (&HNSW{EF: &bad}).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "between 1 and")

	negative := -1
	require.Error(t, (&HNSW{EF: &negative}).Validate())

	tooBig := MaxEF + 1
	err = (&HNSW{EF: &tooBig}).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "between 1 and")

	maxOK := MaxEF
	assert.NoError(t, (&HNSW{EF: &maxOK}).Validate())
}

func TestHNSWSearchContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, ok := HNSWSearchEF(WithHNSWSearch(ctx, nil))
	assert.False(t, ok, "a nil override leaves the configured ef in place")

	_, ok = HNSWSearchEF(WithHNSWSearch(ctx, &HNSW{}))
	assert.False(t, ok)

	ef := 64
	got, ok := HNSWSearchEF(WithHNSWSearch(ctx, &HNSW{EF: &ef}))
	require.True(t, ok)
	assert.Equal(t, 64, got)

	ptr := HNSWSearchEFValue(WithHNSWSearch(ctx, &HNSW{EF: &ef}))
	require.NotNil(t, ptr)
	assert.Equal(t, 64, *ptr)
	assert.Nil(t, HNSWSearchEFValue(ctx))
}
