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

package segmentindex

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
)

// buildInvertedHeaderBytes serializes a HeaderInverted of the given version into
// the on-disk byte layout LoadHeaderInverted/ParseHeaderInverted consume. It
// builds the bytes directly (not via WriteTo, which has no version cap) so the
// test can inject an arbitrary -- including a deliberately too-new -- version.
func buildInvertedHeaderBytes(version uint8) []byte {
	buf := make([]byte, HeaderInvertedSize)
	binary.LittleEndian.PutUint64(buf[0:8], 100)   // KeysOffset
	binary.LittleEndian.PutUint64(buf[8:16], 200)  // TombstoneOffset
	binary.LittleEndian.PutUint64(buf[16:24], 300) // PropertyLengthsOffset
	buf[24] = version
	buf[25] = byte(terms.BLOCK_SIZE) // BlockSize
	buf[26] = 2                      // DataFieldCount
	buf[27] = byte(varenc.DeltaVarIntUint64)
	buf[28] = byte(varenc.VarIntUint64)
	return buf
}

// TestForwardRejectUnknownInvertedVersion is the S17/G4 forward-reject guard.
// An inverted-segment Version above the highest this binary knows
// (SegmentInvertedVersionV2) must be rejected LOUDLY by BOTH LoadHeaderInverted
// and ParseHeaderInverted, with an operator-runbook error. The legacy V0 and the
// supported V2 sentinel must pass.
//
// This test catches a missing reject-unknown guard: with the guard removed, a
// Version=2 (or any future version) header parses successfully, which on a real
// node means the old binary mis-decodes a newer segment's property-length
// section and serves silently-wrong BM25 scores the replication hashtree cannot
// detect. The test exercises the exact input shape (a header byte slice / reader
// carrying a version byte above the sentinel) the guard must reject.
func TestForwardRejectUnknownInvertedVersion(t *testing.T) {
	tests := []struct {
		name      string
		version   uint8
		wantError bool
	}{
		{name: "VersionGuard_V0_legacy_passes", version: 0, wantError: false},
		{name: "VersionGuard_V2_sentinel_passes", version: SegmentInvertedVersionV2, wantError: false},
		{name: "VersionGuard_V2_plus_one_rejected", version: SegmentInvertedVersionV2 + 1, wantError: true},
		{name: "VersionGuard_far_future_rejected", version: 200, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := buildInvertedHeaderBytes(tt.version)

			// LoadHeaderInverted (the bytes-slice path used at segment open).
			loaded, loadErr := LoadHeaderInverted(raw)
			// ParseHeaderInverted (the io.Reader path).
			parsed, parseErr := ParseHeaderInverted(bytes.NewReader(raw))

			if tt.wantError {
				require.Error(t, loadErr, "LoadHeaderInverted must reject version %d", tt.version)
				require.Nil(t, loaded)
				require.Error(t, parseErr, "ParseHeaderInverted must reject version %d", tt.version)
				require.Nil(t, parsed)

				for _, err := range []error{loadErr, parseErr} {
					require.True(t,
						strings.Contains(err.Error(), "unsupported inverted segment version"),
						"error must be the actionable reject-unknown message, got: %v", err)
				}
				return
			}

			require.NoError(t, loadErr, "version %d must pass LoadHeaderInverted", tt.version)
			require.NotNil(t, loaded)
			require.Equal(t, tt.version, loaded.Version)
			require.NoError(t, parseErr, "version %d must pass ParseHeaderInverted", tt.version)
			require.NotNil(t, parsed)
			require.Equal(t, tt.version, parsed.Version)
		})
	}
}

// TestValidateInvertedVersionDirect pins the guard primitive itself, independent
// of the header serialization. It documents that V0 and the V2 sentinel pass and
// anything above the sentinel fails -- so a future sentinel bump is a one-line
// change with a test that fails loudly if the bump is forgotten.
func TestValidateInvertedVersionDirect(t *testing.T) {
	require.NoError(t, validateInvertedVersion(0))
	require.NoError(t, validateInvertedVersion(SegmentInvertedVersionV2))
	require.Error(t, validateInvertedVersion(SegmentInvertedVersionV2+1))
}
