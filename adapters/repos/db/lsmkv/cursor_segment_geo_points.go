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
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/geopoints"
)

// newGeoPointsReader opens the segment's tree. The data section is the whole
// body; a segment held in memory or mmapped is sliced, one read with pread
// goes through its file.
func (s *segment) newGeoPointsReader() (*geopoints.SegmentReader, error) {
	if s.readFromMemory {
		return geopoints.NewSegmentReader(s.contents[s.dataStartPos:s.dataEndPos])
	}
	if s.contentFile == nil {
		return nil, fmt.Errorf("geopoints reader: nil contentFile for segment at %s", s.path)
	}
	return geopoints.NewSegmentReaderPread(s.contentFile, int64(s.dataStartPos),
		int64(s.dataEndPos-s.dataStartPos))
}
