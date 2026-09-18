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

package db

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// TestUUIDFromDocIDWithLookup covers the four states a doc id lookup can land in. The
// caller has to tell them apart: a missing row means the object was deleted since the
// doc id was read and the id is prunable, while a read error means the store could not
// answer and the resolve fails.
func TestUUIDFromDocIDWithLookup(t *testing.T) {
	const (
		docID = uint64(42)
		id    = strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506001")
	)

	row, err := storobj.FromObject(&models.Object{
		Class:      "ThingForDeleteLimit",
		ID:         id,
		Properties: map[string]interface{}{"stringProp": "element 1"},
	}, []float32{1, 2, 3}, nil, nil).MarshalBinary()
	require.NoError(t, err)

	readErr := errors.New("segment read failed")

	tests := []struct {
		name string
		// row is what the lookup returns; nil means the object row is gone.
		row []byte
		// err is what the lookup returns instead of a row.
		err error
		// wantUUID is empty unless the lookup returned a readable object row.
		wantUUID  strfmt.UUID
		wantFound bool
		// wantErrContains is empty when the call must not fail.
		wantErrContains string
	}{
		{
			name:      "present",
			row:       row,
			wantUUID:  id,
			wantFound: true,
		},
		{
			name: "absent",
		},
		{
			name:            "read error",
			err:             readErr,
			wantErrContains: "get object by doc id",
		},
		{
			name:            "corrupt row",
			row:             row[:8],
			wantErrContains: "parse and extract property",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lookup := func(_ context.Context, pos int, seckey, buffer []byte) ([]byte, []byte, error) {
				require.Equal(t, helpers.ObjectsBucketLSMDocIDSecondaryIndex, pos)
				require.Equal(t, docID, binary.LittleEndian.Uint64(seckey),
					"the doc id goes in as the little-endian secondary key")
				return tt.row, buffer, tt.err
			}

			uuid, _, found, err := uuidFromDocIDWithLookup(context.Background(),
				lookup, docID, make([]byte, 8), nil)

			if tt.wantErrContains != "" {
				require.ErrorContains(t, err, tt.wantErrContains)
				require.False(t, found)
				if tt.err != nil {
					require.ErrorIs(t, err, tt.err,
						"the error a caller sees wraps the one the store returned")
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantFound, found)
			require.Equal(t, tt.wantUUID, uuid)
		})
	}
}
