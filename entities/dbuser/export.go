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

import (
	"fmt"
	"time"
)

// ExportStatus classifies each user in an export. Only ExportStatusExported
// carries a usable credential; the others carry a nil SecureHash and name why the
// user's key cannot be migrated.
type ExportStatus int

const (
	// ExportStatusUnspecified is the zero value. It is never a valid result: a
	// record that reaches a response with it is a bug, not an exported user.
	ExportStatusUnspecified ExportStatus = iota
	// ExportStatusExported marks a strong-key user whose argon2id hash is carried.
	ExportStatusExported
	// ExportStatusImportedKey marks a user created from a static key. Its weak
	// sha256 hash is never carried.
	ExportStatusImportedKey
	// ExportStatusRevoked marks a strong-key user whose key was revoked. Carrying
	// the hash would bring the revoked credential back to life.
	ExportStatusRevoked
)

// exportStatusNames are the wire and API spellings. They match the enum in
// openapi-specs/schema.json for DBUserCredential.status.
var exportStatusNames = map[ExportStatus]string{
	ExportStatusExported:    "exported",
	ExportStatusImportedKey: "imported_key",
	ExportStatusRevoked:     "revoked",
}

// String returns the API spelling, or "" for a value that has none.
func (s ExportStatus) String() string {
	return exportStatusNames[s]
}

// Valid reports whether s is one of the named statuses.
func (s ExportStatus) Valid() bool {
	_, ok := exportStatusNames[s]
	return ok
}

// MarshalText writes the API spelling so the RAFT wire form stays readable and
// independent of the iota order.
func (s ExportStatus) MarshalText() ([]byte, error) {
	name, ok := exportStatusNames[s]
	if !ok {
		return nil, fmt.Errorf("invalid export status %d", int(s))
	}
	return []byte(name), nil
}

// UnmarshalText rejects any spelling not in exportStatusNames.
func (s *ExportStatus) UnmarshalText(text []byte) error {
	for status, name := range exportStatusNames {
		if name == string(text) {
			*s = status
			return nil
		}
	}
	return fmt.Errorf("invalid export status %q", string(text))
}

// ExportRecord is the per-user export result and the RAFT wire type. It is kept
// separate from [View] so hash material never reaches an existing response type.
// Only ExportStatusExported records carry a non-nil SecureHash.
type ExportRecord struct {
	Id                 string
	UserIdentifier     string
	SecureHash         *string
	ApiKeyFirstLetters string
	Active             bool
	CreatedAt          time.Time
	Namespace          string
	Status             ExportStatus
}
