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
	"sync"

	"github.com/alexedwards/argon2id"

	"github.com/weaviate/weaviate/entities/models"
)

const SNAPSHOT_VERSION = 0

type DBUsers interface {
	CreateUser(userId, secureHash, userIdentifier string) error
	DeleteUser(userId string) error
	ActivateUser(userId string) error
	DeactivateUser(userId string, revokeKey bool) error
	GetUsers(userIds ...string) (map[string]*User, error)
	RotateKey(userId, secureHash string) error
	CheckUserIdentifierExists(userIdentifier string) (bool, error)
}

type User struct {
	Id                 string
	Active             bool
	InternalIdentifier string
}

type DBUser struct {
	sync.RWMutex
	data dbUserdata
}

type DBUserSnapshot struct {
	data    dbUserdata
	version int
}

type dbUserdata struct {
	weakKeyStorageById   map[string][sha256.Size]byte
	secureKeyStorageById map[string]string
	identifierToId       map[string]string
	idToIdentifier       map[string]string
	users                map[string]*User
	userKeyRevoked       map[string]struct{}
}

func NewDBUser() *DBUser {
	return &DBUser{
		data: dbUserdata{
			weakKeyStorageById:   make(map[string][sha256.Size]byte),
			secureKeyStorageById: make(map[string]string),
			identifierToId:       make(map[string]string),
			idToIdentifier:       make(map[string]string),
			users:                make(map[string]*User),
			userKeyRevoked:       make(map[string]struct{}),
		},
	}
}

func (c *DBUser) CreateUser(userId, secureHash, userIdentifier string) error {
	c.Lock()
	defer c.Unlock()
	_, secureKeyExists := c.data.secureKeyStorageById[userId]
	_, identifierExists := c.data.identifierToId[userId]
	_, usersExists := c.data.users[userId]

	// Todo-Dynuser: Does this make sense? Error would end up with RAFT layer
	if secureKeyExists || identifierExists || usersExists {
		return fmt.Errorf("user %s already exists", userId)
	}
	c.data.secureKeyStorageById[userId] = secureHash
	c.data.identifierToId[userIdentifier] = userId
	c.data.idToIdentifier[userId] = userIdentifier
	c.data.users[userId] = &User{Id: userId, Active: true, InternalIdentifier: userIdentifier}
	return nil
}

func (c *DBUser) RotateKey(userId, secureHash string) error {
	c.Lock()
	defer c.Unlock()

	c.data.secureKeyStorageById[userId] = secureHash
	delete(c.data.weakKeyStorageById, userId)
	delete(c.data.userKeyRevoked, userId)
	return nil
}

func (c *DBUser) DeleteUser(userId string) error {
	c.Lock()
	defer c.Unlock()

	delete(c.data.secureKeyStorageById, userId)
	delete(c.data.idToIdentifier, userId)
	delete(c.data.identifierToId, c.data.idToIdentifier[userId])
	delete(c.data.users, userId)
	delete(c.data.weakKeyStorageById, userId)
	delete(c.data.userKeyRevoked, userId)
	return nil
}

func (c *DBUser) ActivateUser(userId string) error {
	c.Lock()
	defer c.Unlock()

	c.data.users[userId].Active = true
	return nil
}

func (c *DBUser) DeactivateUser(userId string, revokeKey bool) error {
	c.Lock()
	defer c.Unlock()
	if revokeKey {
		c.data.userKeyRevoked[userId] = struct{}{}
	}
	c.data.users[userId].Active = false
	return nil
}

func (c *DBUser) GetUsers(userIds ...string) (map[string]*User, error) {
	c.RLock()
	defer c.RUnlock()

	if len(userIds) == 0 {
		return c.data.users, nil
	}

	users := make(map[string]*User, len(userIds))
	for _, id := range userIds {
		user, ok := c.data.users[id]
		if ok {
			users[id] = user
		}
	}
	return users, nil
}

func (c *DBUser) CheckUserIdentifierExists(userIdentifier string) (bool, error) {
	c.RLock()
	defer c.RUnlock()

	_, ok := c.data.users[userIdentifier]
	return ok, nil
}

func (c *DBUser) ValidateAndExtract(key, userIdentifier string) (*models.Principal, error) {
	c.RLock()
	defer c.RUnlock()

	userId, ok := c.data.identifierToId[userIdentifier]
	if !ok {
		return nil, fmt.Errorf("invalid token")
	}

	secureHash, ok := c.data.secureKeyStorageById[userId]
	if !ok {
		return nil, fmt.Errorf("invalid token")
	}
	weakHash, ok := c.data.weakKeyStorageById[userId]
	if ok {
		// use the secureHash as salt for the computation of the weaker in-memory
		if err := c.validateWeakHash([]byte(key+secureHash), weakHash); err != nil {
			return nil, err
		}
	} else {
		if err := c.validateStrongHash(key, secureHash, userId); err != nil {
			return nil, err
		}
	}

	if c.data.users[userId] != nil && !c.data.users[userId].Active {
		return nil, fmt.Errorf("user deactivated")
	}
	if _, ok := c.data.userKeyRevoked[userId]; ok {
		return nil, fmt.Errorf("key is revoked")
	}

	return &models.Principal{Username: userId, UserType: models.UserTypeInputDb}, nil
}

func (c *DBUser) validateWeakHash(key []byte, weakHash [32]byte) error {
	keyHash := sha256.Sum256(key)
	if subtle.ConstantTimeCompare(keyHash[:], weakHash[:]) != 1 {
		return fmt.Errorf("invalid token")
	}

	return nil
}

func (c *DBUser) validateStrongHash(key, secureHash, userId string) error {
	match, err := argon2id.ComparePasswordAndHash(key, secureHash)
	if err != nil {
		return err
	}
	if !match {
		return fmt.Errorf("invalid token")
	}
	token := []byte(key + secureHash)
	c.data.weakKeyStorageById[userId] = sha256.Sum256(token)

	return nil
}

func (c *DBUser) Snapshot() DBUserSnapshot {
	c.Lock()
	defer c.Unlock()

	return DBUserSnapshot{data: c.data, version: SNAPSHOT_VERSION}
}

func (c *DBUser) Restore(snapshot DBUserSnapshot) error {
	c.Lock()
	defer c.Unlock()

	if snapshot.version != SNAPSHOT_VERSION {
		return fmt.Errorf("invalid snapshot version")
	}
	c.data = snapshot.data

	return nil
}
