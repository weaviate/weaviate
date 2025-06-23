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

package api

import (
	"crypto/sha256"
	"time"

	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
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

type ActivateUsersRequest struct {
	UserId  string
	Version int
}

type SuspendUserRequest struct {
	UserId    string
	RevokeKey bool
	Version   int
}

type QueryGetUsersRequest struct {
	UserIds []string
}

type QueryGetUsersResponse struct {
	Users map[string]*apikey.User
}

type QueryUserIdentifierExistsRequest struct {
	UserIdentifier string
}

type QueryUserIdentifierExistsResponse struct {
	Exists bool
}
