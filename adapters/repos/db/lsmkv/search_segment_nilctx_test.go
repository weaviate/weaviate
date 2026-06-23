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

package lsmkv

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// DoBlockMaxWand must tolerate a nil ctx (some callers pass none). Empty results
// reach both ctx-use sites: the pre-loop ctx.Done() and the normal-return
// AnnotateSlowQueryLog. Either one panics on a nil ctx without the guards.
func TestDoBlockMaxWand_NilContext(t *testing.T) {
	logger := logrus.New()

	//nolint:staticcheck // SA1012: passing a nil ctx is the behaviour under test.
	heap, err := DoBlockMaxWand(nil, 10, Terms{}, 1.0, false, 0, 1, logger)
	require.NoError(t, err)
	require.NotNil(t, heap)
	require.Equal(t, 0, heap.Len())
}
