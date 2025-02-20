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

package apikey

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"

	"github.com/alexedwards/argon2id"

	"github.com/weaviate/weaviate/entities/models"
)

type DynamicUser interface {
	CreateUser(userId, secureHash, userIdentifier string) error
}

type DynamicApiKey struct {
	weakKeyStorage       map[string][sha256.Size]byte
	secureKeyStorageById map[string]string
	IdentifierToId       map[string]string
}

func NewDynamicApiKey() *DynamicApiKey {
	return &DynamicApiKey{
		weakKeyStorage:       make(map[string][sha256.Size]byte),
		secureKeyStorageById: make(map[string]string),
		IdentifierToId:       make(map[string]string),
	}
}

func (c *DynamicApiKey) CreateUser(userId, secureHash, userIdentifier string) error {
	c.secureKeyStorageById[userId] = secureHash
	c.IdentifierToId[userIdentifier] = userId

	return nil
}

func (c *DynamicApiKey) ValidateAndExtract(key, userIdentifier string) (*models.Principal, error) {
	userId, ok := c.IdentifierToId[userIdentifier]
	if !ok {
		return nil, fmt.Errorf("invalid token")
	}

	secureHash, ok := c.secureKeyStorageById[userId]
	if !ok {
		return nil, fmt.Errorf("invalid token")
	}
	weakHash, ok := c.weakKeyStorage[userId]
	if ok {
		return c.validateWeakHash([]byte(key+secureHash), weakHash, userId)
	} else {
		return c.validateStrongHash(key, secureHash, userId)
	}
}

func (c *DynamicApiKey) validateWeakHash(key []byte, weakHash [32]byte, userId string) (*models.Principal, error) {
	keyHash := sha256.Sum256(key)
	if subtle.ConstantTimeCompare(keyHash[:], weakHash[:]) == 1 {
		return &models.Principal{Username: userId}, nil
	}

	return nil, fmt.Errorf("invalid token")
}

func (c *DynamicApiKey) validateStrongHash(key, secureHash, userId string) (*models.Principal, error) {
	match, err := argon2id.ComparePasswordAndHash(key, secureHash)
	if err != nil {
		return nil, err
	}
	if !match {
		return nil, fmt.Errorf("invalid token")
	}
	token := []byte(key + secureHash)
	c.weakKeyStorage[userId] = sha256.Sum256(token)

	return &models.Principal{Username: userId}, nil
}
