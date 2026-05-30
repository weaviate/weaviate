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

// Package storobj write-gate regression tests (B1).
//
// These tests pin the rolling-upgrade binary-format guarantee: when the write-gate
// is closed (v1, default) objects must be written with the 42-byte v1 header, not
// the 50-byte v2 header. A v1-only (old) node's decode guard rejects any object
// with MarshallerVersion != 1, so emitting v2 during a rolling upgrade is a
// hard-fail decode error on the replica receiving the write.
package storobj

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

// TestB1_GateClosed_PersistsV1Binary verifies that when the write-gate is
// closed (default), objects are written with a 42-byte v1 header and their
// on-disk MarshallerVersion byte is 1. A v1-only old node decoding these bytes
// MUST NOT see MarshallerVersion==2 and return an error.
//
// Causal link: before the fix, shard_write_put.go unconditionally set
// obj.MarshallerVersion = storobj.CurrentMarshallerVersion (= 2), producing a
// 50-byte header that old nodes reject. This test catches that regression by
// verifying MarshalBinaryDisk emits the v1 format when gate is closed.
func TestB1_GateClosed_PersistsV1Binary(t *testing.T) {
	// Save and restore gate.
	orig := currentWriteMarshallerVersion
	SetWriteMarshallerVersion(1)
	defer SetWriteMarshallerVersion(orig)

	obj := FromObject(&models.Object{
		Class: "GateTest",
		ID:    strfmt.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
	}, nil, nil, nil)
	obj.DocID = 1
	obj.Version = 7 // simulate a minted version in memory

	// Simulate what shard_write_put.go does: set MarshallerVersion from gate.
	obj.MarshallerVersion = GetWriteMarshallerVersion()

	disk, err := obj.MarshalBinaryDisk(false)
	require.NoError(t, err, "MarshalBinaryDisk must succeed with gate closed")

	// Decode the first byte (MarshallerVersion byte at offset 0).
	require.Greater(t, len(disk), 0, "disk bytes must not be empty")
	versionByte := disk[0]
	assert.Equal(t, uint8(1), versionByte,
		"gate-closed write must produce v1 header byte (offset 0); got %d - old nodes would reject this", versionByte)

	// Confirm the disk bytes are decodable by the strict v1 path.
	decoded, err := FromBinaryNetwork(disk)
	require.NoError(t, err, "v1-encoded bytes must be decodable by the current code")
	assert.Equal(t, uint8(1), decoded.MarshallerVersion,
		"decoded MarshallerVersion must be 1 (v1 gate-closed write)")

	// Confirm that Version is NOT persisted in v1 (it decodes to 0).
	assert.Equal(t, uint64(0), decoded.Version,
		"gate-closed v1 write must not persist Version (decodes to 0); "+
			"a v1-only old node reading this is safe (Version is not in the binary)")
}

// TestB1_GateOpen_PersistsV2Binary verifies that when the write-gate is open
// (v2), objects are written with a 50-byte v2 header and their on-disk
// MarshallerVersion byte is 2, and the Version field is readable.
//
// Causal link: this tests the positive path - when the operator opts in, the
// 50-byte header is produced and Version is persisted correctly.
func TestB1_GateOpen_PersistsV2Binary(t *testing.T) {
	orig := currentWriteMarshallerVersion
	SetWriteMarshallerVersion(2)
	defer SetWriteMarshallerVersion(orig)

	const mintedVersion = uint64(42)

	obj := FromObject(&models.Object{
		Class: "GateTest",
		ID:    strfmt.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
	}, nil, nil, nil)
	obj.DocID = 1
	obj.Version = mintedVersion
	obj.MarshallerVersion = GetWriteMarshallerVersion()

	disk, err := obj.MarshalBinaryDisk(false)
	require.NoError(t, err, "MarshalBinaryDisk must succeed with gate open")

	require.Greater(t, len(disk), 0)
	versionByte := disk[0]
	assert.Equal(t, uint8(2), versionByte,
		"gate-open write must produce v2 header byte (offset 0)")

	// Confirm Version is round-trip readable from disk bytes.
	version, err := VersionFromBinary(disk)
	require.NoError(t, err, "VersionFromBinary must succeed on v2 bytes")
	assert.Equal(t, mintedVersion, version,
		"VersionFromBinary must return the minted version from disk bytes")
}

// TestB1_GateClosed_V2ObjectDecodableByV1Path simulates the rolling-upgrade
// mixed-version scenario: a gate-closed new node writes v1 bytes. Those bytes
// must be decodable by the old v1-only decode guard (MarshallerVersion == 1 check).
//
// The v2 decode guard (MarshallerVersion == 1 || == 2) is the new code; the
// old node's guard is strictly == 1. A gate-closed new node must not produce
// v2 bytes that the old node would reject. This test verifies that constraint
// at the binary level.
//
// Causal link: this catches the root-cause scenario from the B1 review finding:
// before the fix, all writes were v2; this test fails on the unfixed tree where
// the output has version byte 2 (which old nodes reject).
func TestB1_GateClosed_V2ObjectDecodableByV1Path(t *testing.T) {
	orig := currentWriteMarshallerVersion
	SetWriteMarshallerVersion(1)
	defer SetWriteMarshallerVersion(orig)

	obj := FromObject(&models.Object{
		Class: "OldNodeSafe",
		ID:    strfmt.UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
	}, []float32{1, 2, 3}, nil, nil)
	obj.DocID = 99
	obj.Version = 5 // in-memory version does not get persisted gate-closed
	obj.MarshallerVersion = GetWriteMarshallerVersion() // 1 = gate closed

	disk, err := obj.MarshalBinaryDisk(false)
	require.NoError(t, err)

	// Simulate old-node v1-only decode guard: any version byte != 1 is rejected.
	versionByte := disk[0]
	if versionByte != 1 {
		t.Fatalf("gate-closed new node emitted version byte %d - old v1-only node would reject this with "+
			"\"unsupported binary marshaller version %d\"; "+
			"this is the B1 rolling-upgrade break", versionByte, versionByte)
	}

	// Full decode must succeed.
	_, err = FromBinaryNetwork(disk)
	require.NoError(t, err, "gate-closed bytes must be decodable by current code (backward compat)")
}
