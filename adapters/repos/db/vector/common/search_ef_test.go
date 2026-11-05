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

package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHonorsSearchEF(t *testing.T) {
	t.Parallel()

	assert.True(t, HonorsSearchEF(IndexTypeHNSW))
	assert.True(t, HonorsSearchEF(IndexTypeDynamic))
	assert.False(t, HonorsSearchEF(IndexTypeFlat))
	assert.False(t, HonorsSearchEF(IndexTypeHFresh))
	assert.False(t, HonorsSearchEF(IndexTypeNoop))
}

func TestUnsupportedSearchEF(t *testing.T) {
	t.Parallel()

	err := UnsupportedSearchEF("", IndexTypeFlat)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"default"`)
	assert.Contains(t, err.Error(), "flat")

	err = UnsupportedSearchEF("title", IndexTypeHFresh)
	assert.Contains(t, err.Error(), `"title"`)
	assert.Contains(t, err.Error(), "hfresh")
}
