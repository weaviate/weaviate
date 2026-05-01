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

package columnar

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnarHeaderRoundTrip(t *testing.T) {
	h := &ColumnarHeader{
		NumRows: 42,
		Schema: Schema{
			Columns: []Column{
				{Name: "popularity", Type: ColumnTypeFloat32},
				{Name: "created_at", Type: ColumnTypeInt64},
			},
		},
	}

	buf := h.MarshalBinary()
	got, n, err := UnmarshalColumnarHeader(buf)
	require.NoError(t, err)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, h.NumRows, got.NumRows)
	assert.Equal(t, len(h.Schema.Columns), len(got.Schema.Columns))
	for i := range h.Schema.Columns {
		assert.Equal(t, h.Schema.Columns[i].Name, got.Schema.Columns[i].Name)
		assert.Equal(t, h.Schema.Columns[i].Type, got.Schema.Columns[i].Type)
	}
}

func TestSchemaRowWidth(t *testing.T) {
	s := Schema{Columns: []Column{
		{Name: "a", Type: ColumnTypeFloat32},
		{Name: "b", Type: ColumnTypeInt64},
	}}
	assert.Equal(t, 12, s.RowWidth())
}

func TestEncodeDecodeFunctions(t *testing.T) {
	buf := make([]byte, 12)
	EncodeFloat32(buf, 0, 3.14)
	EncodeInt64(buf, 4, -99)
	assert.InDelta(t, float32(3.14), DecodeFloat32(buf, 0), 0.001)
	assert.Equal(t, int64(-99), DecodeInt64(buf, 4))
}
