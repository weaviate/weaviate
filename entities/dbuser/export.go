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
	// ExportStatusExported marks a strong-key user whose argon2id hash is carried.
	ExportStatusExported = "exported"
	// ExportStatusImportedKey marks a user created from a static key. Its weak
	// sha256 hash cannot be stored through CreateUser.
	ExportStatusImportedKey = "imported_key"
	// ExportStatusRevoked marks a strong-key user whose key was revoked. Carrying
	// the hash would bring the revoked credential back to life.
	ExportStatusRevoked = "revoked"
)

// ExportRecord is the per-user result of an export. The RAFT query sends and
// receives this same type. It is kept separate from [View] so hash material never
// reaches an existing response type. Only records with
// Status == ExportStatusExported carry a non-nil SecureHash; every other status
// reports why the user was not carried.
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
