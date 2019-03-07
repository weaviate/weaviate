package test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/2tvenom/cbor"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/stretchr/testify/assert"
)

// Create a minimized request log, encode it to CBOR and decode it again, then assert whether the result matches the expected value.
func TestCborEncode(t *testing.T) {
	t.Parallel()

	// setup
	minimizedLog := `[{"n": "upbeat-aquatic-pen", "t": "REST", "i": "weaviate.something.or.other", "a": 1, "w": 1550745544}]`

	outputTransformer := telemetry.NewOutputTransformer()

	encoded, err := outputTransformer.EncodeAsCBOR(&minimizedLog)

	var result string
	var buffTest bytes.Buffer

	encoder := cbor.NewEncoder(&buffTest)
	ok, err := encoder.Unmarshal(encoded, &result)

	if !ok {
		fmt.Printf("Error Unmarshal %s", err)
		return
	}

	assert.Equal(t, minimizedLog, result)
}
