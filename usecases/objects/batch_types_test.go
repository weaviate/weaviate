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
	"testing"

	"github.com/go-openapi/strfmt"
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

	t.Run("a legacy payload decodes", func(t *testing.T) {
		cases := []struct {
			name     string
			payload  string
			wantErr  bool
			wantText string
		}{
			{
				name:    "a legacy nil error",
				payload: `[{"UUID":"11111111-1111-1111-1111-111111111111","Err":null}]`,
			},
			{
				name:     "a legacy error, which marshalled to an empty object",
				payload:  `[{"UUID":"22222222-2222-2222-2222-222222222222","Err":{}}]`,
				wantErr:  true,
				wantText: "could not transmit",
			},
		}

		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				var out BatchSimpleObjects
				require.NoError(t, json.Unmarshal([]byte(tt.payload), &out))
				require.Len(t, out, 1)
				require.NotEmpty(t, out[0].UUID)
				if !tt.wantErr {
					require.NoError(t, out[0].Err)
					return
				}
				require.ErrorContains(t, out[0].Err, tt.wantText)
			})
		}
	})

	t.Run("an errored slot still fails an old peer's decode", func(t *testing.T) {
		encoded, err := json.Marshal(BatchSimpleObjects{
			{UUID: "33333333-3333-3333-3333-333333333333", Err: errors.New("disk failure")},
		})
		require.Contains(t, string(encoded), `"ErrMsg":"disk failure"`,
			"the text is what a node with the codec reads")
		require.NoError(t, err)

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
