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
	lock *sync.RWMutex
	data dbUserdata
}

type DBUserSnapshot struct {
	Data    dbUserdata
	Version int
}

type dbUserdata struct {
	WeakKeyStorageById   map[string][sha256.Size]byte
	SecureKeyStorageById map[string]string
	IdentifierToId       map[string]string
	IdToIdentifier       map[string]string
	Users                map[string]*User
	UserKeyRevoked       map[string]struct{}
}

func NewDBUser() *DBUser {
	return &DBUser{
		lock: &sync.RWMutex{},
		data: dbUserdata{
			WeakKeyStorageById:   make(map[string][sha256.Size]byte),
			SecureKeyStorageById: make(map[string]string),
			IdentifierToId:       make(map[string]string),
			IdToIdentifier:       make(map[string]string),
			Users:                make(map[string]*User),
			UserKeyRevoked:       make(map[string]struct{}),
		},
	}
}

func (c *DBUser) CreateUser(userId, secureHash, userIdentifier string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, secureKeyExists := c.data.SecureKeyStorageById[userId]
	_, identifierExists := c.data.IdentifierToId[userId]
	_, usersExists := c.data.Users[userId]

	// Todo-Dynuser: Does this make sense? Error would end up with RAFT layer
	if secureKeyExists || identifierExists || usersExists {
		return fmt.Errorf("user %s already exists", userId)
	}
	c.data.SecureKeyStorageById[userId] = secureHash
	c.data.IdentifierToId[userIdentifier] = userId
	c.data.IdToIdentifier[userId] = userIdentifier
	c.data.Users[userId] = &User{Id: userId, Active: true, InternalIdentifier: userIdentifier}
	return nil
}

func (c *DBUser) RotateKey(userId, secureHash string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.data.SecureKeyStorageById[userId] = secureHash
	delete(c.data.WeakKeyStorageById, userId)
	delete(c.data.UserKeyRevoked, userId)
	return nil
}

func (c *DBUser) DeleteUser(userId string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.data.SecureKeyStorageById, userId)
	delete(c.data.IdToIdentifier, userId)
	delete(c.data.IdentifierToId, c.data.IdToIdentifier[userId])
	delete(c.data.Users, userId)
	delete(c.data.WeakKeyStorageById, userId)
	delete(c.data.UserKeyRevoked, userId)
	return nil
}

func (c *DBUser) ActivateUser(userId string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.data.Users[userId].Active = true
	return nil
}

func (c *DBUser) DeactivateUser(userId string, revokeKey bool) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if revokeKey {
		c.data.UserKeyRevoked[userId] = struct{}{}
	}
	c.data.Users[userId].Active = false
	return nil
}

func (c *DBUser) GetUsers(userIds ...string) (map[string]*User, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if len(userIds) == 0 {
		return c.data.Users, nil
	}

	users := make(map[string]*User, len(userIds))
	for _, id := range userIds {
		user, ok := c.data.Users[id]
		if ok {
			users[id] = user
		}
	}
	return users, nil
}

func (c *DBUser) CheckUserIdentifierExists(userIdentifier string) (bool, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	_, ok := c.data.Users[userIdentifier]
	return ok, nil
}

func (c *DBUser) ValidateAndExtract(key, userIdentifier string) (*models.Principal, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	userId, ok := c.data.IdentifierToId[userIdentifier]
	if !ok {
		return nil, fmt.Errorf("invalid token")
	}

	secureHash, ok := c.data.SecureKeyStorageById[userId]
	if !ok {
		return nil, fmt.Errorf("invalid token")
	}
	weakHash, ok := c.data.WeakKeyStorageById[userId]
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

	if c.data.Users[userId] != nil && !c.data.Users[userId].Active {
		return nil, fmt.Errorf("user deactivated")
	}
	if _, ok := c.data.UserKeyRevoked[userId]; ok {
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
	c.data.WeakKeyStorageById[userId] = sha256.Sum256(token)

	return nil
}

func (c *DBUser) Snapshot() DBUserSnapshot {
	c.lock.Lock()
	defer c.lock.Unlock()

	return DBUserSnapshot{Data: c.data, Version: SNAPSHOT_VERSION}
}

func (c *DBUser) Restore(snapshot DBUserSnapshot) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if snapshot.Version != SNAPSHOT_VERSION {
		return fmt.Errorf("invalid snapshot version")
	}
	c.data = snapshot.Data

	return nil
}
