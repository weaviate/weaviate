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

package nested

import "strings"

const pathSep = "."

// SplitRelPath splits a dot-notation relative nested path into its property
// name segments. The inverse of JoinRelPath.
//
//	SplitRelPath("cars.tires.width") → ["cars", "tires", "width"]
func SplitRelPath(path string) []string {
	return strings.Split(path, pathSep)
}

// JoinRelPath joins property name segments into a dot-notation relative path.
// The inverse of SplitRelPath.
//
//	JoinRelPath(["cars", "tires", "width"]) → "cars.tires.width"
func JoinRelPath(segs []string) string {
	return strings.Join(segs, pathSep)
}
