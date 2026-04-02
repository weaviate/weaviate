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

package schema

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/weaviate/weaviate/entities/models"
)

// HashBlob computes a SHA-256 hash of the given base64-encoded blob string
// and returns the hex-encoded digest. This is used by the BlobHash data type
// to store a compact hash instead of the full blob payload.
func HashBlob(base64Data string) string {
	h := sha256.Sum256([]byte(base64Data))
	return hex.EncodeToString(h[:])
}

// HashBlobHashProperties replaces the base64 data of all BlobHash properties
// on the object with their SHA-256 hashes. This must be called AFTER
// vectorization (so vectorizers see the raw media data) but BEFORE storage
// (so only the compact hash is persisted to disk).
func HashBlobHashProperties(class *models.Class, obj *models.Object) {
	if obj == nil || obj.Properties == nil || class == nil {
		return
	}

	props, ok := obj.Properties.(map[string]interface{})
	if !ok {
		return
	}

	for _, prop := range class.Properties {
		if !IsBlobHashDataType(prop.DataType) {
			continue
		}
		val, exists := props[prop.Name]
		if !exists {
			continue
		}
		if base64Str, ok := val.(string); ok {
			props[prop.Name] = HashBlob(base64Str)
		}
	}
}
