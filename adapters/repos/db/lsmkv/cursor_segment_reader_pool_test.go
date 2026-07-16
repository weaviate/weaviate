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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// Reuse is non-deterministic (sync.Pool); assert only the release contract here.
func TestSegmentCursorReaderReleaseContract(t *testing.T) {
	c := &segmentCursorReplaceReusable{
		preadReader: acquireSegmentCursorReader(bytes.NewReader(nil)),
		preadOffset: &offsetReader{},
	}
	require.NotNil(t, c.preadReader)

	c.releaseReader()
	require.Nil(t, c.preadReader, "releaseReader must clear the reader")
	require.Nil(t, c.preadOffset)

	require.NotPanics(t, c.releaseReader, "releaseReader must be idempotent (no double-Put)")
}
