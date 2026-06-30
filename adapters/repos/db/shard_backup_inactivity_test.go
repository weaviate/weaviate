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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShard_ReArmOrResumeAfterInactivity pins the fix for the halt watchdog's
// reset-vs-fire race: when the inactivity timer fires but a concurrent
// MayResetTransferInactivityTimer has pushed the deadline into the future (real
// transfer activity that raced the firing and could not drain the already-latched
// tick), the watchdog must re-arm and keep waiting instead of force-resuming
// maintenance cycles mid-stream.
func TestShard_ReArmOrResumeAfterInactivity(t *testing.T) {
	t.Run("deadline in the future re-arms and does not resume (the race)", func(t *testing.T) {
		// A non-zero halt count means a transfer is in progress. A future deadline
		// stands in for a reset that landed in the reset-vs-fire window.
		timer := time.NewTimer(time.Hour)
		require.True(t, timer.Stop(), "fresh timer should be stoppable")
		s := &Shard{
			haltForTransferInactivityTimer:   timer,
			haltForTransferInactivityTimeout: time.Hour,
			haltForTransferDeadline:          time.Now().Add(time.Hour),
			haltForTransferCount:             1,
		}

		resumed := s.reArmOrResumeAfterInactivity()

		assert.False(t, resumed, "must not resume while the deadline is still in the future")
		assert.Equal(t, 1, s.haltForTransferCount,
			"force-resume must not run: halt count must be left untouched")
	})

	t.Run("deadline reset further out is honoured across repeated firings", func(t *testing.T) {
		timer := time.NewTimer(time.Hour)
		require.True(t, timer.Stop())
		s := &Shard{
			haltForTransferInactivityTimer:   timer,
			haltForTransferInactivityTimeout: time.Hour,
			haltForTransferDeadline:          time.Now().Add(time.Hour),
			haltForTransferCount:             3,
		}

		for i := 0; i < 5; i++ {
			require.False(t, s.reArmOrResumeAfterInactivity())
		}
		assert.Equal(t, 3, s.haltForTransferCount)
	})

	t.Run("elapsed deadline resumes", func(t *testing.T) {
		timer := time.NewTimer(time.Hour)
		require.True(t, timer.Stop())
		s := &Shard{
			haltForTransferInactivityTimer:   timer,
			haltForTransferInactivityTimeout: 10 * time.Millisecond,
			haltForTransferDeadline:          time.Now().Add(-time.Second),
			haltForTransferCount:             0,
		}

		resumed := s.reArmOrResumeAfterInactivity()

		assert.True(t, resumed, "a genuinely elapsed deadline must resume and exit the watchdog")
	})
}

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
