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

package shared

import (
	"bytes"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

func TestOverwriteRawCompressionRoundTrip(t *testing.T) {
	mkRaw := func(id strfmt.UUID) []byte {
		o := storobj.FromObject(
			&models.Object{
				ID:    id,
				Class: "Paragraph",
				Properties: map[string]interface{}{
					"contents": "the quick brown fox jumps over the lazy dog, repeatedly and verbosely",
				},
				LastUpdateTimeUnix: 12345,
			},
			[]float32{0.1, 0.2, 0.3, 0.4}, nil, nil,
		)
		b, err := o.MarshalBinary()
		require.NoError(t, err)
		return b
	}

	input := []*objects.VObject{
		{StaleUpdateTime: 111, RawBytes: mkRaw("73f2eb5f-5abf-447a-81ca-74b1dd168241")},
		{StaleUpdateTime: 222, RawBytes: mkRaw("73f2eb5f-5abf-447a-81ca-74b1dd168242")},
	}

	payload, err := IndicesPayloads.VersionedObjectList.MarshalRaw(input)
	require.NoError(t, err)

	compressed := CompressOverwriteRaw(payload)
	assert.False(t, bytes.Equal(payload, compressed))

	restored, err := DecompressOverwriteRaw(compressed)
	require.NoError(t, err)
	assert.Equal(t, payload, restored)

	got, err := IndicesPayloads.VersionedObjectList.UnmarshalRaw(restored)
	require.NoError(t, err)
	require.Len(t, got, len(input))
	for i := range input {
		assert.Equal(t, input[i].StaleUpdateTime, got[i].StaleUpdateTime)
		assert.Equal(t, input[i].RawBytes, got[i].RawBytes)
	}
}

func TestDecompressOverwriteRawRejectsGarbage(t *testing.T) {
	_, err := DecompressOverwriteRaw([]byte("not zstd"))
	require.Error(t, err)
}
