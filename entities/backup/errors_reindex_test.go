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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reindexSentinels is every sentinel a reindex refusal can carry, with
// the literal each one is pinned to. Callers across the RPC boundary
// match these with errors.Is, so a reworded text is a compatible change
// and a renamed or merged sentinel is not — the literals are here to
// make the second kind red.
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
}

func TestReindexSentinelTexts(t *testing.T) {
	for _, s := range reindexSentinels {
		t.Run(s.name, func(t *testing.T) {
			require.EqualError(t, s.err, s.text)
			// A sentinel is a constant, never a template. A format verb
			// here would mean some call site interpolates a shard id or a
			// node name into a value clients match on.
			assert.NotContains(t, s.text, "%", "a sentinel must not be a format template")
			// The gates quote every identifier they name. A quote in the
			// sentinel itself means an identifier was baked into it.
			assert.NotContains(t, s.text, `"`, "a sentinel must not carry a quoted identifier")
		})
	}
}

// TestReindexSentinelsAreDistinguishable pins that the two sentinels never
// cross: a caller mapping a refusal to an HTTP status branches on exactly
// one of them.
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

// TestReindexBlockedErrorChains pins that the publishable text survives
// every shape a refusal takes on its way out: wrapped by the storage
// layer, joined with an unrelated failure on another collection, and
// wrapped again by the coordinator.
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
			assert.NotContains(t, recovered.Msg, "shard ",
				"the publishable text names the collection, never the shard")
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
