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

package storobj

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestParseAndExtractTextPropDecodesStoredStrings(t *testing.T) {
	values := []string{"Denim & Dash", "<tag>", "quote\"slash\\", "line\nwith\ttab", "café 世界 😀", `literal \u0026`, ""}
	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			obj := FromObject(&models.Object{
				Class: "Text", ID: strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
				Properties: map[string]interface{}{"scalar": value, "array": []string{value, "plain"}},
			}, nil, nil, nil)
			data, err := obj.MarshalBinary()
			require.NoError(t, err)
			scalar, ok, err := ParseAndExtractTextProp(data, "scalar")
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, []string{value}, scalar)
			array, ok, err := ParseAndExtractProperty(data, "array")
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, []string{value, "plain"}, array)
			// Callers reuse the storage buffer after extraction.
			for i := range data {
				data[i] = 0
			}
			require.Equal(t, []string{value}, scalar)
			require.Equal(t, []string{value, "plain"}, array)
		})
	}
}

// Replace only the serialized properties, retaining a real storage envelope.
func storedRawProperties(t *testing.T, properties string) []byte {
	t.Helper()
	obj := FromObject(&models.Object{
		Class: "Text", ID: strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
		CreationTimeUnix: 123, LastUpdateTimeUnix: 456,
		Properties: map[string]interface{}{"placeholder": "value"},
	}, nil, nil, nil)
	data, err := obj.MarshalBinary()
	require.NoError(t, err)
	old, err := extractPropsBytes(data)
	require.NoError(t, err)
	start := bytes.Index(data, old)
	require.Greater(t, start, 4)
	result := append([]byte{}, data[:start]...)
	binary.LittleEndian.PutUint32(result[start-4:start], uint32(len(properties)))
	result = append(result, properties...)
	return append(result, data[start+len(old):]...)
}

func TestParseAndExtractTextPropJSONTokens(t *testing.T) {
	cases := []struct {
		name, raw string
		want      []string
	}{
		{"unicode", `{"value":"\u4e16\u754c\ud83d\ude00"}`, []string{"世界😀"}},
		{"array", `{"value":["a\n","\u0026","\\u0026"]}`, []string{"a\n", "&", `\u0026`}},
		{"number", `{"value":42.5}`, []string{"42.5"}},
		{"bool", `{"value":true}`, []string{"true"}},
		{"null", `{"value":null}`, []string{"null"}},
		{"missing", `{}`, []string{}},
		{"empty array", `{"value":[]}`, []string{}},
		{"date", `{"value":"2024-02-20T00:00:00Z"}`, []string{"2024-02-20T00:00:00Z"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := storedRawProperties(t, tc.raw)
			got, ok, err := ParseAndExtractProperty(data, "value")
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.want, got)
		})
	}
	for _, raw := range []string{`{"value":"\q"}`, `{"value":["valid","\uZZZZ"]}`, `{"value":["valid",]}`} {
		t.Run(raw, func(t *testing.T) {
			got, ok, err := ParseAndExtractTextProp(storedRawProperties(t, raw), "value")
			require.Error(t, err)
			require.False(t, ok)
			require.Nil(t, got)
		})
	}
	data := storedRawProperties(t, `{"numbers":[1,2.5],"bools":[true,false]}`)
	numbers, ok, err := ParseAndExtractNumberArrayProp(data, "numbers")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []float64{1, 2.5}, numbers)
	bools, ok, err := ParseAndExtractBoolArrayProp(data, "bools")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []bool{true, false}, bools)
	for key, want := range map[string]string{"id": "73f2eb5f-5abf-447a-81ca-74b1dd168247", "_id": "73f2eb5f-5abf-447a-81ca-74b1dd168247", "_creationTimeUnix": "123", "_lastUpdateTimeUnix": "456"} {
		got, ok, err := ParseAndExtractProperty(data, key)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []string{want}, got)
	}
}

func BenchmarkParseAndExtractTextProp(b *testing.B) {
	for _, value := range []string{"plain", "Denim & Dash", "quote\"slash\\line\n"} {
		b.Run(value, func(b *testing.B) {
			obj := FromObject(&models.Object{
				Class:      "Text",
				ID:         strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
				Properties: map[string]interface{}{"name": value},
			}, nil, nil, nil)
			data, err := obj.MarshalBinary()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := ParseAndExtractTextProp(data, "name"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
