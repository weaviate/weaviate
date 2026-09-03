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
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

// FlatMetadataFileName is the single derivation of the flat index's
// quantisation metadata file name, under the shard directory. Both the live
// index (via FlatMetadataFileNameForID) and the drop-artifact list
// (vectorIndexArtifactNames) call through here, so they cannot disagree.
// Previously they were two independent implementations — this one a plain
// Sprintf, entities/vectorindex/flat.MetadataFileName sanitizing via
// filepath.Clean/Base — which drifted for any name the sanitizer alters.
// Delegating keeps that sanitization intact for both callers.
func FlatMetadataFileName(targetVector string) string {
	return flatent.MetadataFileName(targetVector)
}

// FlatMetadataFileNameForID names the flat index's quantization metadata
// file for a physical index ID.
func FlatMetadataFileNameForID(physicalID string) string {
	return FlatMetadataFileName(helpers.PhysicalIDSuffix(physicalID))
}
