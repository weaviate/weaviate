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

package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"time"

	"github.com/weaviate/weaviate/entities/dbuser"
)

const (
	// NOTE: in case changes happens to the dynamic user message, add new version
	DynUserLatestCommandPolicyVersion = iota
)

type CreateUsersRequest struct {
	UserId             string
	SecureHash         string
	UserIdentifier     string
	ApiKeyFirstLetters string
	Namespace          string
	CreatedAt          time.Time
	Version            int
}

type CreateUserWithKeyRequest struct {
	UserId             string
	ApiKeyFirstLetters string
	WeakHash           [sha256.Size]byte
	CreatedAt          time.Time
	Version            int
}

type RotateUserApiKeyRequest struct {
	UserId             string
	ApiKeyFirstLetters string
	SecureHash         string
	OldIdentifier      string
	NewIdentifier      string
	Version            int
}

type DeleteUsersRequest struct {
	UserId  string
	Version int
}

// DeleteUsersInNamespaceRequest deletes every DB user bound to Namespace.
type DeleteUsersInNamespaceRequest struct {
	Namespace string
	Version   int
}

type ActivateUsersRequest struct {
	UserId  string
	Version int
}

type SuspendUserRequest struct {
	UserId    string
	RevokeKey bool
	Version   int
}

// UpdateUserRequest sets each non-nil pointer field on an existing user and
// leaves the rest as stored. Those fields carry omitempty, so an update that
// leaves a field nil still decodes on a node built before that field was added.
type UpdateUserRequest struct {
	UserId string
	// ExpiresAt clears the expiry when it points to the zero time.
	ExpiresAt *time.Time `json:",omitempty"`
	Version   int
}

// UnmarshalJSON refuses a key this binary does not know, so an update carrying
// a field added later fails instead of applying without it.
func (r *UpdateUserRequest) UnmarshalJSON(b []byte) error {
	type plain UpdateUserRequest
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	return dec.Decode((*plain)(r))
}

type QueryGetUsersRequest struct {
	UserIds []string
}

type QueryGetUsersResponse struct {
	Users map[string]*dbuser.View
}

type QueryUserIdentifierExistsRequest struct {
	UserIdentifier string
}

type QueryUserIdentifierExistsResponse struct {
	Exists bool
}

type QueryExportUsersRequest struct {
	UserIds []string
}

type QueryExportUsersResponse struct {
	Users map[string]dbuser.ExportRecord
}
