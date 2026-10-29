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

package lsmkv

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// A log written by either version has to replay into the other, so the record
// must stay byte-identical to the MapPair it replaces.
func TestInvertedRecordBytesMatchMapPair(t *testing.T) {
	tests := []struct {
		name      string
		docID     uint64
		tf        float32
		propLen   float32
		tombstone bool
	}{
		{name: "posting", docID: 42, tf: 3, propLen: 7},
		{name: "posting with max doc id", docID: math.MaxUint64, tf: 1, propLen: 1},
		{name: "tombstone", docID: 42, tombstone: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, tt.docID)

			mp := MapPair{Key: key, Tombstone: tt.tombstone}
			if !tt.tombstone {
				mp.Value = make([]byte, 8)
				binary.LittleEndian.PutUint32(mp.Value[0:4], math.Float32bits(tt.tf))
				binary.LittleEndian.PutUint32(mp.Value[4:8], math.Float32bits(tt.propLen))
			}
			want, err := mp.Bytes()
			require.NoError(t, err)

			pair := invertedPair{docID: tt.docID, tombstone: tt.tombstone}
			if !tt.tombstone {
				pair.tfBits = math.Float32bits(tt.tf)
				pair.propLenBits = math.Float32bits(tt.propLen)
			}

			var buf [invertedRecordPostingLen]byte
			n := pair.encodeCommitLog(buf[:])
			require.Equal(t, want, buf[:n])

			var decoded invertedPair
			require.NoError(t, decoded.decodeCommitLog(buf[:n], tt.tombstone))
			require.Equal(t, pair, decoded)
		})
	}
}

func TestInvertedRecordDecodeRejects(t *testing.T) {
	posting := func() []byte {
		var buf [invertedRecordPostingLen]byte
		invertedPair{docID: 7, tfBits: 1, propLenBits: 2}.encodeCommitLog(buf[:])
		return buf[:]
	}

	tests := []struct {
		name      string
		record    []byte
		tombstone bool
		wantErr   string
	}{
		{
			name:    "shorter than the header",
			record:  posting()[:10],
			wantErr: "want at least 12 bytes",
		},
		{
			name: "key length below 8",
			record: func() []byte {
				r := posting()
				binary.LittleEndian.PutUint16(r[0:2], 4)
				return r
			}(),
			wantErr: "key length must be 8",
		},
		{
			name: "key length above 8",
			record: func() []byte {
				r := posting()
				binary.LittleEndian.PutUint16(r[0:2], 16)
				return r
			}(),
			wantErr: "key length must be 8",
		},
		{
			name: "value length below 8",
			record: func() []byte {
				r := posting()
				binary.LittleEndian.PutUint16(r[10:12], 4)
				return r[:16]
			}(),
			wantErr: "value length must be 8",
		},
		{
			name: "value length above 8",
			record: func() []byte {
				r := append(posting(), 0, 0, 0, 0)
				binary.LittleEndian.PutUint16(r[10:12], 12)
				return r
			}(),
			wantErr: "value length must be 8",
		},
		{
			name: "value prefix claims more bytes than the record holds",
			record: func() []byte {
				r := posting()
				binary.LittleEndian.PutUint16(r[10:12], 64)
				return r
			}(),
			wantErr: "inconsistent inverted record",
		},
		{
			name:    "prefixes do not account for the whole record",
			record:  append(posting(), 0, 0, 0, 0),
			wantErr: "inconsistent inverted record",
		},
		{
			name:      "tombstone carrying an unaccounted trailer",
			record:    append(posting()[:invertedRecordTombstoneLen], 0),
			tombstone: true,
			wantErr:   "inconsistent inverted record",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var pair invertedPair
			err := pair.decodeCommitLog(tt.record, tt.tombstone)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
