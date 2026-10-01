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

package objects

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatchSimpleObjectJSONRoundTrip pins what survives the cluster wire: a
// per-object delete error reaches the coordinator as text, and a payload from
// a peer without the codec still decodes.
func TestBatchSimpleObjectJSONRoundTrip(t *testing.T) {
	t.Run("a slot's error survives the round trip", func(t *testing.T) {
		in := BatchSimpleObjects{
			{UUID: "11111111-1111-1111-1111-111111111111"},
			{UUID: "22222222-2222-2222-2222-222222222222", Err: errors.New("shard is read-only")},
		}

		encoded, err := json.Marshal(in)
		require.NoError(t, err)

		var out BatchSimpleObjects
		require.NoError(t, json.Unmarshal(encoded, &out),
			"one errored slot must not fail the whole slice")

		require.Len(t, out, 2)
		require.Equal(t, in[0].UUID, out[0].UUID)
		require.NoError(t, out[0].Err, "a deleted object carries no error")
		require.Equal(t, in[1].UUID, out[1].UUID,
			"the failed object is still named, so the caller knows which one")
		require.ErrorContains(t, out[1].Err, "shard is read-only")
	})

	t.Run("an errored slot still fails an old peer's decode", func(t *testing.T) {
		encoded, err := json.Marshal(BatchSimpleObjects{
			{UUID: "33333333-3333-3333-3333-333333333333", Err: errors.New("disk failure")},
		})
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"ErrMsg":"disk failure"`,
			"the text is what a node with the codec reads")

		// the shape a node without the codec decodes into: Err is an interface
		// there, so the legacy {} fails its unmarshal — which is the point. A
		// nil Err would tell that node the object was deleted when it was not.
		var legacy []struct {
			UUID strfmt.UUID `json:"UUID"`
			Err  error       `json:"Err"`
		}
		require.Error(t, json.Unmarshal(encoded, &legacy),
			"a failed delete must not decode as a success on a node without the codec")
	})

	t.Run("a slot that succeeded decodes anywhere", func(t *testing.T) {
		encoded, err := json.Marshal(BatchSimpleObjects{
			{UUID: "44444444-4444-4444-4444-444444444444"},
		})
		require.NoError(t, err)
		require.NotContains(t, string(encoded), `"Err"`,
			"no legacy key for a slot that carries no error")

		var legacy []struct {
			UUID strfmt.UUID `json:"UUID"`
			Err  error       `json:"Err"`
		}
		require.NoError(t, json.Unmarshal(encoded, &legacy),
			"a node without the codec still reads the objects that were deleted")
		require.Len(t, legacy, 1)
		require.Equal(t, strfmt.UUID("44444444-4444-4444-4444-444444444444"), legacy[0].UUID)
		require.NoError(t, legacy[0].Err)
	})
}

// TestTruncateBatchErrMsg pins the cap a flush failure multiplies by the batch size, and that a
// capped message is still valid UTF-8.
func TestTruncateBatchErrMsg(t *testing.T) {
	tests := []struct {
		name    string
		msg     string
		wantLen int
		wantCut bool
	}{
		{name: "a short message is untouched", msg: "shard is read-only", wantLen: 18},
		{name: "a message at the cap is untouched", msg: strings.Repeat("a", maxBatchErrMsg), wantLen: maxBatchErrMsg},
		{
			name: "a long message is cut to the cap", msg: strings.Repeat("a", 10_000),
			wantLen: maxBatchErrMsg + len("…"), wantCut: true,
		},
		{
			// cutting mid-rune would put U+FFFD on the wire: 3-byte runes straddle byte 256
			name: "a multi-byte message is cut on a rune boundary", msg: strings.Repeat("→", 10_000),
			wantLen: maxBatchErrMsg - maxBatchErrMsg%3 + len("…"), wantCut: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateBatchErrMsg(tt.msg)
			assert.Len(t, got, tt.wantLen)
			assert.True(t, utf8.ValidString(got), "a capped message must stay valid UTF-8")
			assert.Equal(t, tt.wantCut, strings.HasSuffix(got, "…"))
		})
	}
}

// TestMarshalCapsAFailedSlot pins the wire size of a slot whose error flushWALs wrote into every
// entry of the batch.
func TestMarshalCapsAFailedSlot(t *testing.T) {
	encoded, err := json.Marshal(BatchSimpleObject{
		UUID: "55555555-5555-5555-5555-555555555555",
		Err:  errors.New(strings.Repeat("b", 100_000)),
	})
	require.NoError(t, err)
	assert.Less(t, len(encoded), 512,
		"one uncapped message is multiplied by the batch size on an already-failing response")

	var out BatchSimpleObject
	require.NoError(t, json.Unmarshal(encoded, &out))
	require.Error(t, out.Err, "capping the text must not turn a failed slot into a success")
	assert.NotErrorIs(t, out.Err, ErrRemoteDeleteUnreadable, "a capped message still reaches the coordinator")
}
