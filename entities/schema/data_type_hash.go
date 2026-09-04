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
	"regexp"

	"github.com/weaviate/weaviate/entities/models"
)

var onlyHexadecimalCharacters = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

// HashBlob computes a SHA-256 hash of the given base64-encoded blob string
// and returns the hex-encoded digest. This is used by the BlobHash data type
// to store a compact hash instead of the full blob payload.
func HashBlob(base64Data string) string {
	h := sha256.Sum256([]byte(base64Data))
	return hex.EncodeToString(h[:])
}

func IsLikelySHA256Hash(s string) bool {
	// Length of string must be 64
	if len(s) != 64 {
		return false
	}
	// Only hexadecimal characters
	if !onlyHexadecimalCharacters.MatchString(s) {
		return false
	}
	// Reject obvious dummy/all-zero hashes (very rare in practice)
	if s == "0000000000000000000000000000000000000000000000000000000000000000" ||
		s == "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" {
		return false
	}
	return true
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

	HashBlobHashPrimitiveProperties(class, props)
}

func HashBlobHashPrimitiveProperties(class *models.Class, props map[string]interface{}) {
	if class == nil || props == nil {
		return
	}

	for _, prop := range class.Properties {
		hashPropValue(prop.Name, prop.DataType, prop.NestedProperties, props)
	}
}

func hashNestedProperties(nestedProps []*models.NestedProperty, props map[string]interface{}) {
	if len(nestedProps) == 0 || props == nil {
		return
	}

	for _, nestedProp := range nestedProps {
		hashPropValue(nestedProp.Name, nestedProp.DataType, nestedProp.NestedProperties, props)
	}
}

func hashPropValue(name string, dataType []string, nestedProps []*models.NestedProperty, props map[string]interface{}) {
	if props == nil {
		return
	}
	val, exists := props[name]
	if !exists || val == nil {
		return
	}

	if IsBlobHashDataType(dataType) {
		if base64Str, ok := val.(string); ok {
			props[name] = HashBlob(base64Str)
		}
		return
	}

	if len(dataType) == 1 {
		hashNestedType(dataType[0], nestedProps, val)
	}
}

func hashNestedType(dataType string, nestedProps []*models.NestedProperty, val interface{}) {
	switch dataType {
	case string(DataTypeObject):
		if objMap, ok := val.(map[string]interface{}); ok {
			hashNestedProperties(nestedProps, objMap)
		}
	case string(DataTypeObjectArray):
		if objList, ok := val.([]interface{}); ok {
			hashObjectList(nestedProps, objList)
		}
	}
}

func hashObjectList(nestedProps []*models.NestedProperty, objList []interface{}) {
	for _, item := range objList {
		if objMap, ok := item.(map[string]interface{}); ok {
			hashNestedProperties(nestedProps, objMap)
		}
	}
}
