//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package segmentindex

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

// Note: checksums are also being tested in the compaction_integration_test.go file, that tests multiple segments and strategies
// to ensure that the ValidateChecksum function works correctly for all segment types.

// contentsWithChecksum is a precomputed segment file from the property__id bucket.
// The data object which was used to generate this segment file is the following:
//
//	{
//	  "id": "3722dfe8-b26c-4d05-95ee-41045673b43d",
//	  "class": "Paragraph",
//	  "properties": {
//	    "contents": "para1"
//	    }
//	}
//
// These contents include the CRC32 checksum which was calculated based on the:
//   - segment data
//   - segment indexes
//   - segment header
//
// The checksum is calculated using those components in that exact ordering.
// This is because during compactions, the header is not actually known until
// the compaction process is complete. So to accommodate this, all segment
// checksum calculations are made using the header last.
var contentsWithChecksum = []byte{
	0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x51, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x00, 0x33, 0x37, 0x32,
	0x32, 0x64, 0x66, 0x65, 0x38, 0x2d, 0x62, 0x32, 0x36, 0x63, 0x2d, 0x34,
	0x64, 0x30, 0x35, 0x2d, 0x39, 0x35, 0x65, 0x65, 0x2d, 0x34, 0x31, 0x30,
	0x34, 0x35, 0x36, 0x37, 0x33, 0x62, 0x34, 0x33, 0x64, 0x24, 0x00, 0x00,
	0x00, 0x33, 0x37, 0x32, 0x32, 0x64, 0x66, 0x65, 0x38, 0x2d, 0x62, 0x32,
	0x36, 0x63, 0x2d, 0x34, 0x64, 0x30, 0x35, 0x2d, 0x39, 0x35, 0x65, 0x65,
	0x2d, 0x34, 0x31, 0x30, 0x34, 0x35, 0x36, 0x37, 0x33, 0x62, 0x34, 0x33,
	0x64, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x51, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x53, 0x4c, 0x67,
	0x5a,
}

func prepareSegment(t *testing.T) (*os.File, int64) {
	dir := t.TempDir()
	fname := path.Join(dir, "tmp.db")

	{
		f, err := os.Create(fname)
		require.Nil(t, err)
		_, err = f.Write(contentsWithChecksum)
		require.Nil(t, err)
		f.Close()
	}

	f, err := os.Open(fname)
	require.Nil(t, err)

	fileInfo, err := f.Stat()
	if err != nil {
		fmt.Printf("Error getting file info: %v\n", err)
		return nil, 0
	}
	return f, fileInfo.Size()
}

func TestSegmentFile_ValidateChecksum(t *testing.T) {
	f, fileSize := prepareSegment(t)
	defer f.Close()
	segmentFile := NewSegmentFile(WithReader(f))
	err := segmentFile.ValidateChecksum(fileSize, HeaderSize)
	require.Nil(t, err)
}

// This test checks that the ValidateChecksum function works correctly when the reader buffer is close to the size of the final read of the checksum.
// In a previous implementation, this caused an error because the reader would not read the checksum bytes correctly by not using io.ReadFull.
// Setting a custom buffer size is simpler than creating a segment file with a size that is close to the checksum size.
// Interesting note, for this segment, the test would fail in the old implementation if the buffer size is set to 77, 78, 154, 155 or 156 bytes.
func TestSegmentFile_ValidateChecksumMultipleOfBufferReader(t *testing.T) {
	f, fileSize := prepareSegment(t)
	defer f.Close()
	segmentFile := NewSegmentFile(WithReaderCustomBufferSize(f, 77))
	err := segmentFile.ValidateChecksum(fileSize, HeaderSize)
	require.Nil(t, err)
}
