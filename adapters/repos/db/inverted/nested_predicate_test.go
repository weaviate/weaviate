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

package inverted

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
)

// TestNestedProperty_HasMetaEntries_AnchorOnly pins the Anchors branch of the
// HasMetaEntries OR expression. When Idx and Exists are both empty but Anchors
// is non-empty, HasMetaEntries must still return true. The struct literal is
// valid here because this file is in package inverted and can reach the
// unexported result field directly.
func TestNestedProperty_HasMetaEntries_AnchorOnly(t *testing.T) {
	np := NestedProperty{result: &nested.AssignResult{
		Anchors: []nested.AnchorEntry{
			{Path: "addresses", Position: nested.ElemIdx(4)},
		},
	}}
	assert.True(t, np.HasMetaEntries(),
		"Anchors-only AssignResult must report HasMetaEntries=true")
}
