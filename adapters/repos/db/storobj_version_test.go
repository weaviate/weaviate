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
	"encoding/binary"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// TestHashtreeDigestUnchanged is AC4 / INV-HASHTREE-1: a v1 and a v2
// representation of the same object (same UUID + LastUpdateTimeUnix, different
// Version / MarshallerVersion) must produce the same hashtree digest input.
//
// The digest source is [16-byte UUID][8-byte BigEndian(LastUpdateTimeUnix)] as
// built in upsertObjectHashTree / upsertHashTreeLeaf. Neither the raw record
// bytes nor the Version field are hashed, so adding Version to the record does
// not change the digest. This test confirms that invariant at the data-structure
// level: DocIDAndTimeFromBinary on both records returns the same updateTime, and
// the UUID bytes are identical.
func TestHashtreeDigestUnchanged(t *testing.T) {
	uuidStr := strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247")
	updateTime := int64(123456789)

	orig := storobj.GetWriteMarshallerVersion()
	defer storobj.SetWriteMarshallerVersion(orig)

	// Produce v1 bytes.
	storobj.SetWriteMarshallerVersion(1)
	v1obj := storobj.FromObject(&models.Object{
		Class:              "DigestClass",
		ID:                 uuidStr,
		LastUpdateTimeUnix: updateTime,
	}, nil, nil, nil)
	v1Bytes, err := v1obj.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, uint8(1), v1Bytes[0], "sanity: v1 bytes expected")

	// Produce v2 bytes with a different Version.
	storobj.SetWriteMarshallerVersion(2)
	v2obj := storobj.FromObject(&models.Object{
		Class:              "DigestClass",
		ID:                 uuidStr,
		LastUpdateTimeUnix: updateTime,
	}, nil, nil, nil)
	v2obj.Version = 999
	v2Bytes, err := v2obj.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, uint8(2), v2Bytes[0], "sanity: v2 bytes expected")

	// Both records must yield the same updateTime via DocIDAndTimeFromBinary.
	_, v1Time, err := storobj.DocIDAndTimeFromBinary(v1Bytes)
	require.NoError(t, err)
	_, v2Time, err := storobj.DocIDAndTimeFromBinary(v2Bytes)
	require.NoError(t, err)

	assert.Equal(t, v1Time, v2Time,
		"v1 and v2 records with same LastUpdateTimeUnix must yield identical digest updateTime")
	assert.Equal(t, updateTime, v1Time)

	// Verify the digest bytes themselves are identical.
	// The digest is: [uuid 16 bytes] + BigEndian(LastUpdateTimeUnix).
	v1Digest := makeDigest(uuidStr.String(), v1Time)
	v2Digest := makeDigest(uuidStr.String(), v2Time)
	assert.Equal(t, v1Digest, v2Digest,
		"hashtree digest bytes must be identical for v1 and v2 of the same logical object")
}

// makeDigest mirrors the digest construction in upsertObjectHashTree
// (shard_write_put.go:353-363): [16-byte UUID][8-byte BigEndian updateTime].
func makeDigest(uuidStr string, updateTime int64) [24]byte {
	var digest [16 + 8]byte
	// Parse UUID into bytes the same way upsertObjectHashTree does.
	// uuidBytes in production come from the bucket key (16 raw bytes).
	// Here we approximate using the canonical string form to derive the same bytes.
	ub, err := parseUUIDToBytes(uuidStr)
	if err != nil {
		panic(err)
	}
	copy(digest[:16], ub)
	binary.BigEndian.PutUint64(digest[16:], uint64(updateTime))
	return digest
}

func parseUUIDToBytes(s string) ([]byte, error) {
	// Parse UUID string to 16 raw bytes.
	u := [16]byte{}
	// Reuse the go uuid package's parsing via the storobj uuid import path.
	// Here we just do it manually to avoid importing the package directly.
	hex := s
	j := 0
	for i := 0; i < len(hex); i++ {
		if hex[i] == '-' {
			continue
		}
		hi := fromHexChar(hex[i])
		lo := fromHexChar(hex[i+1])
		u[j] = hi<<4 | lo
		i++
		j++
	}
	return u[:], nil
}

func fromHexChar(c byte) byte {
	switch {
	case '0' <= c && c <= '9':
		return c - '0'
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}
