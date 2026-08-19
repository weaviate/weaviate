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

package flat

import (
	"path/filepath"
	"strings"
)

const (
	metadataFilePrefix = "meta"
	metadataFileExt    = ".db"
)

// MetadataFileName returns the name of the bolt file a flat index keeps its
// metadata in, inside the index's own root directory. Clean and Base reduce
// targetVector to one path element; the degenerate results they can still return
// get the unnamed vector's name rather than a separator in a filename.
func MetadataFileName(targetVector string) string {
	name := filepath.Base(filepath.Clean(targetVector))
	if name == "." || name == ".." || name == string(filepath.Separator) {
		return metadataFilePrefix + metadataFileExt
	}
	return metadataFilePrefix + "_" + name + metadataFileExt
}

// IsMetadataFile reports whether the base name of name matches the flat metadata
// naming rule. name may be a base name or a path. entities/backup cannot import
// adapters/repos/db/vector/flat, so the predicate is defined here.
func IsMetadataFile(name string) bool {
	stem, ok := strings.CutSuffix(filepath.Base(name), metadataFileExt)
	if !ok {
		return false
	}
	return stem == metadataFilePrefix || strings.HasPrefix(stem, metadataFilePrefix+"_")
}
