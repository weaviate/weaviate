package test

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/ugorji/go/codec"
)

// Create a minimized request log and encode it to CBOR, then assert whether it matches the expected value.
func TestCborEncode(t *testing.T) {
	t.Parallel()

	// setup
	minimizedLog := `[{"n": "upbeat-aquatic-pen", "t": "REST", "i": "weaviate.something.or.other", "a": 1, "w": 1550745544}]`

	outputTransformer := telemetry.NewOutputTransformer()

	actual, err := outputTransformer.EncodeAsCBOR(&minimizedLog)

	expected := []uint8{129, 165, 97, 97, 1, 97, 105, 120, 27, 119, 101, 97, 118, 105, 97, 116, 101, 46, 115, 111, 109, 101, 116, 104, 105, 110, 103, 46, 111, 114, 46, 111, 116, 104, 101, 114, 97, 110, 114, 117, 112, 98, 101, 97, 116, 45, 97, 113, 117, 97, 116, 105, 99, 45, 112, 101, 110, 97, 116, 100, 82, 69, 83, 84, 97, 119, 26, 92, 110, 127, 200}

	// test
	assert.Equal(t, nil, err)
	assert.Equal(t, expected, actual)
}

// Decode a record in CBOR-format, then assert whether the decoded field values match the expected values.
// (this comparison relies on the decoding itself going well. This test is more of a smoke test than a unit test)
func TestCborDecoding(t *testing.T) {
	t.Parallel()

	// setup
	encoded := []uint8{129, 165, 97, 97, 1, 97, 105, 120, 27, 119, 101, 97, 118, 105, 97, 116, 101, 46, 115, 111, 109, 101, 116, 104, 105, 110, 103, 46, 111, 114, 46, 111, 116, 104, 101, 114, 97, 110, 114, 117, 112, 98, 101, 97, 116, 45, 97, 113, 117, 97, 116, 105, 99, 45, 112, 101, 110, 97, 116, 100, 82, 69, 83, 84, 97, 119, 26, 92, 110, 127, 200}

	decoded := make([]map[string]interface{}, 1)

	handle := new(codec.CborHandle)

	dec := codec.NewDecoderBytes(encoded, handle)

	err := dec.Decode(decoded)

	record := decoded[0]
	n := record["n"].(string)
	ty := record["t"].(string)
	i := record["i"].(string)
	a := int(record["a"].(uint64))
	w := int64(record["w"].(uint64))

	// test
	assert.Equal(t, nil, err)
	assert.Equal(t, "upbeat-aquatic-pen", n)
	assert.Equal(t, "REST", ty)
	assert.Equal(t, "weaviate.something.or.other", i)
	assert.Equal(t, 1, a)
	assert.Equal(t, int64(1550745544), w)
}
