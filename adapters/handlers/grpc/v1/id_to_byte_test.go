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

package v1

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/objects"
)

// uuidCases pins the leading-zero-byte truncation bug across zero-byte counts 0-3 plus the extremes.
var uuidCases = []struct {
	name string
	id   string
}{
	{name: "no leading zero byte", id: "a4de3ca0-6975-464f-b23b-adddd83630d7"},
	{name: "one leading zero byte", id: "00de3ca0-6975-464f-b23b-adddd83630d7"},
	{name: "two leading zero bytes", id: "00003ca0-6975-464f-b23b-adddd83630d7"},
	{name: "three leading zero bytes", id: "000000a0-6975-464f-b23b-adddd83630d7"},
	{name: "leading zero nibble only", id: "04de3ca0-6975-464f-b23b-adddd83630d7"},
	{name: "nil uuid", id: "00000000-0000-0000-0000-000000000000"},
	{name: "max uuid", id: "ffffffff-ffff-ffff-ffff-ffffffffffff"},
}

func TestIdToByteRoundTrip(t *testing.T) {
	for _, tt := range uuidCases {
		t.Run(tt.name, func(t *testing.T) {
			want, err := uuid.MustParse(tt.id).MarshalBinary()
			require.NoError(t, err)

			got, gotStr, err := idToByte(strfmt.UUID(tt.id))
			require.NoError(t, err)
			require.Equal(t, tt.id, gotStr)
			require.Len(t, got, 16, "IdAsBytes must always be 16 bytes")
			require.Equal(t, want, got)

			parsed, err := uuid.FromBytes(got)
			require.NoError(t, err, "clients reconstruct the UUID from the raw bytes")
			require.Equal(t, tt.id, parsed.String())
		})
	}
}

func TestIdToByteRejectsNonUUID(t *testing.T) {
	tests := []struct {
		name string
		id   any
	}{
		{name: "wrong type", id: "a4de3ca0-6975-464f-b23b-adddd83630d7"},
		{name: "not a uuid", id: strfmt.UUID("not-a-uuid")},
		{name: "empty", id: strfmt.UUID("")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := idToByte(tt.id)
			require.Error(t, err)
		})
	}
}

func TestBatchDeleteReplyUUIDRoundTrip(t *testing.T) {
	objs := make(objects.BatchSimpleObjects, 0, len(uuidCases))
	for _, tt := range uuidCases {
		objs = append(objs, objects.BatchSimpleObject{UUID: strfmt.UUID(tt.id)})
	}

	reply, err := batchDeleteReplyFromObjects(objects.BatchDeleteResult{Objects: objs}, true)
	require.NoError(t, err)
	require.Len(t, reply.Objects, len(uuidCases))

	for i, tt := range uuidCases {
		t.Run(tt.name, func(t *testing.T) {
			want, err := uuid.MustParse(tt.id).MarshalBinary()
			require.NoError(t, err)
			require.Len(t, reply.Objects[i].Uuid, 16)
			require.Equal(t, want, reply.Objects[i].Uuid)
		})
	}
}
