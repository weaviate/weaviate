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
	GetUsers(userIds ...string) (map[string]*User, error)
	CheckUserIdentifierExists(userIdentifier string) (bool, error)
}

type User struct {
	Id     string
	Active bool
}

type DynamicApiKey struct {
	weakKeyStorage       map[string][sha256.Size]byte
	secureKeyStorageById map[string]string
	identifierToId       map[string]string
	users                map[string]*User
}

func NewDynamicApiKey() *DynamicApiKey {
	return &DynamicApiKey{
		weakKeyStorage:       make(map[string][sha256.Size]byte),
		secureKeyStorageById: make(map[string]string),
		identifierToId:       make(map[string]string),
		users:                make(map[string]*User),
	}
}

func (c *DynamicApiKey) CreateUser(userId, secureHash, userIdentifier string) error {
	c.secureKeyStorageById[userId] = secureHash
	c.identifierToId[userIdentifier] = userId
	c.users[userId] = &User{Id: userId, Active: true}
	return nil
}

func (c *DynamicApiKey) GetUsers(userIds ...string) (map[string]*User, error) {
	users := make(map[string]*User, len(userIds))
	for _, id := range userIds {
		user, ok := c.users[id]
		if ok {
			users[id] = user
		}
	}
	return users, nil
}

func (c *DynamicApiKey) CheckUserIdentifierExists(userIdentifier string) (bool, error) {
	_, ok := c.users[userIdentifier]
	return ok, nil
}

func (c *DynamicApiKey) ValidateAndExtract(key, userIdentifier string) (*models.Principal, error) {
	userId, ok := c.identifierToId[userIdentifier]
	if !ok {
		return nil, fmt.Errorf("invalid token")
	}

	secureHash, ok := c.secureKeyStorageById[userId]
	if !ok {
		return nil, fmt.Errorf("invalid token")
	}
	weakHash, ok := c.weakKeyStorage[userId]
	if ok {
		// use the secureHash as salt for the computation of the weaker in-memory
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
