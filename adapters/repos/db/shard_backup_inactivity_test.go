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

package db

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTransferActivityReader_ResetsPerChunk pins the fix for the per-file (not
// per-chunk) reset gap: streaming a single large segment in halt-for-duration
// fallback mode must count every chunk read as activity, so a segment that takes
// longer than the inactivity timeout to stream does not trip a spurious
// force-resume mid-stream.
func TestTransferActivityReader_ResetsPerChunk(t *testing.T) {
	const payload = "the quick brown fox jumps over the lazy dog"
	resets := 0
	r := &transferActivityReader{
		ReadCloser: io.NopCloser(strings.NewReader(payload)),
		reset:      func() { resets++ },
	}

	buf := make([]byte, 8)
	readCalls := 0
	for {
		_, err := r.Read(buf)
		readCalls++
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	assert.Greater(t, readCalls, 1, "payload must span multiple chunks for this to be meaningful")
	assert.Equal(t, readCalls, resets,
		"the watchdog must be reset on every chunk read, not just at file open")
}
