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

package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var reindexSentinels = []struct {
	name string
	err  error
	text string
}{
	{name: "backup gate", err: ErrBackupBlockedByInFlightReindex, text: "backup blocked: runtime-reindex in flight"},
	{name: "restore gate", err: ErrReindexInFlight, text: "runtime-reindex in flight in the cluster"},
	{
		name: "restore gate, undetermined", err: ErrReindexActivityUndetermined,
		text: "restore blocked: whether a runtime-reindex is in flight could not be determined",
	},
	{
		name: "commit-time overlap observed",
		err:  ErrReindexOverlappedBackup,
		text: "backup blocked: a runtime-reindex overlapped this backup",
	},
	{
		name: "commit-time overlap unanswerable",
		err:  ErrReindexOverlapUndetermined,
		text: "backup blocked: the runtime-reindex overlap could not be determined",
	},
	{
		name: "canCommit, overlap check cannot answer",
		err:  ErrReindexOverlapCheckUnanswerable,
		text: "backup blocked: the runtime-reindex overlap check cannot answer",
	},
}

func TestReindexSentinelTexts(t *testing.T) {
	for _, s := range reindexSentinels {
		t.Run(s.name, func(t *testing.T) {
			require.EqualError(t, s.err, s.text)
			assert.NotContains(t, s.text, "%", "a sentinel must not be a format template")
			assert.NotContains(t, s.text, `"`, "a sentinel must not carry a quoted identifier")
		})
	}
}

func TestReindexSentinelsAreDistinguishable(t *testing.T) {
	for _, s := range reindexSentinels {
		t.Run(s.name, func(t *testing.T) {
			for _, other := range reindexSentinels {
				if other.name == s.name {
					continue
				}
				require.NotErrorIs(t, s.err, other.err,
					"%s must not match %s", s.name, other.name)
			}
		})
	}
}

// Whatever goes in, the phrase the coordinator relabels a FAILED participant
// CANCELLED on is not in what comes out.
func TestCancelSafeTextLeavesNoCancelPhrase(t *testing.T) {
	phrase := context.Canceled.Error()
	tests := []struct {
		name, text string
	}{
		{name: "empty"},
		{name: "nothing to scrub", text: "no space left on device"},
		{name: "only the phrase", text: phrase},
		// The replacement ends in "context", so a trailing word re-forms the phrase.
		{name: "a trailing word re-forms it", text: "s3: " + phrase + " canceled while uploading"},
		{name: "many separate occurrences", text: strings.Repeat(phrase+" and ", 50)},
		// Long enough that a loop making no progress would not finish in time.
		{name: "adversarially re-forming, at length", text: phrase + strings.Repeat(" canceled", 5000)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CancelSafeText(tt.text)
			assert.NotContains(t, got, phrase, "the phrase must not survive any input")
			assert.Equal(t, got, CancelSafeText(got), "scrubbing a scrubbed text changes nothing")
			if !strings.Contains(tt.text, phrase) {
				assert.Equal(t, tt.text, got, "text with nothing to scrub is left alone")
			}
		})
	}
}

func TestReindexBlockedErrorChains(t *testing.T) {
	const msg = `backup blocked: runtime-reindex in flight: collection "Movies" is migrating`
	blocked := ReindexBlockedError{Msg: msg}
	unrelated := errors.New("raft: leader unreachable")
	tests := []struct {
		name string
		err  error
	}{
		{name: "bare", err: blocked},
		{name: "wrapped once", err: fmt.Errorf("canCommit: %w", blocked)},
		{name: "wrapped twice", err: fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", blocked))},
		{name: "joined with an unrelated failure", err: errors.Join(blocked, unrelated)},
		{name: "joined then wrapped", err: fmt.Errorf("canCommit: %w", errors.Join(blocked, unrelated))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ErrorIs(t, tt.err, ErrBackupBlockedByInFlightReindex,
				"the 422 mapping matches this sentinel and nothing else")
			require.NotErrorIs(t, tt.err, ErrReindexInFlight)
			var recovered ReindexBlockedError
			require.ErrorAs(t, tt.err, &recovered,
				"errors.As must yield the publishable text without walking the chain")
			require.Equal(t, msg, recovered.Msg)
		})
	}
	t.Run("an unrelated failure survives the join", func(t *testing.T) {
		joined := errors.Join(blocked, unrelated)
		require.ErrorIs(t, joined, unrelated,
			"a refusal on one collection must not swallow a failure on another")
		require.True(t, strings.HasPrefix(joined.Error(), msg),
			"the joined message must lead with the refusal, not with the other cause")
	})
}
