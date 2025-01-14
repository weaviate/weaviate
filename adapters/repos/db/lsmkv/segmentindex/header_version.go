//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package segmentindex

const (
	// SegmentV1 is the current latest version, and introduced support
	// for integrity checks with checksums added to the segment files.
	SegmentV1 = uint16(1)

	// CurrentSegmentVersion is used to ensure that the parsed header
	// version does not exceed the highest valid version.
	CurrentSegmentVersion = SegmentV1
)

func ChooseHeaderVersion(checksumsEnabled bool) uint16 {
	if !checksumsEnabled {
		return 0
	}
	return CurrentSegmentVersion
}
