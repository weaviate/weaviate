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

package dbuser

import "time"

// Export status values classify each user in an export. Only ExportStatusExported
// carries a usable credential; the others carry a nil SecureHash and name why the
// user's key cannot be migrated.
const (
	// ExportStatusExported: a strong-key user whose argon2id hash is carried.
	ExportStatusExported = "exported"
	// ExportStatusImportedKey: a user created from a static (weak sha256) key,
	// which cannot be reconstructed through CreateUser.
	ExportStatusImportedKey = "imported_key"
	// ExportStatusRevoked: a strong-key user whose key was revoked; carrying it
	// would resurrect a revoked credential.
	ExportStatusRevoked = "revoked"
	// ExportStatusNoKey: a user record with no secure hash on file (defensive).
	ExportStatusNoKey = "no_key"
)

// ExportRecord is the per-user result of an export. It is a shared wire type at
// both ends of the RAFT query hop, kept separate from [View] so hash material
// never reaches any pre-existing response type. Only records with
// Status == ExportStatusExported carry a non-nil SecureHash; every other status
// is a sentinel that reports why the user was not carried.
type ExportRecord struct {
	Id                 string
	UserIdentifier     string
	SecureHash         *string
	ApiKeyFirstLetters string
	Active             bool
	CreatedAt          time.Time
	Namespace          string
	Status             string
}
