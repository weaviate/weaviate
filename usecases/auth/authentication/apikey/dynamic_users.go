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
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/weaviate/weaviate/entities/backup"

	"github.com/alexedwards/argon2id"

	"github.com/weaviate/weaviate/entities/models"
)

type DynamicUser interface {
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

type DynamicApiKey struct {
	sync.RWMutex
	weakKeyStorageById map[string][sha256.Size]byte
	longTermData       dynamicApiKeyBackup // this is data that should survive restarts and is part of the backup
}

// have everything Exported for backup
type dynamicApiKeyBackup struct {
	SecureKeyStorageById map[string]string
	IdentifierToId       map[string]string
	IdToIdentifier       map[string]string
	Users                map[string]*User
	UserKeyRevoked       map[string]struct{}
}

func NewDynamicApiKey() *DynamicApiKey {
	return &DynamicApiKey{
		weakKeyStorageById: make(map[string][sha256.Size]byte),
		longTermData: dynamicApiKeyBackup{
			SecureKeyStorageById: make(map[string]string),
			IdentifierToId:       make(map[string]string),
			IdToIdentifier:       make(map[string]string),
			Users:                make(map[string]*User),
			UserKeyRevoked:       make(map[string]struct{}),
		},
	}
}

func (c *DynamicApiKey) CreateUser(userId, secureHash, userIdentifier string) error {
	c.Lock()
	defer c.Unlock()
	_, secureKeyExists := c.longTermData.SecureKeyStorageById[userId]
	_, identifierExists := c.longTermData.IdentifierToId[userId]
	_, usersExists := c.longTermData.Users[userId]

	// Todo-Dynuser: Does this make sense? Error would end up with RAFT layer
	if secureKeyExists || identifierExists || usersExists {
		return fmt.Errorf("user %s already exists", userId)
	}
	c.longTermData.SecureKeyStorageById[userId] = secureHash
	c.longTermData.IdentifierToId[userIdentifier] = userId
	c.longTermData.IdToIdentifier[userId] = userIdentifier
	c.longTermData.Users[userId] = &User{Id: userId, Active: true, InternalIdentifier: userIdentifier}
	return nil
}

func (c *DynamicApiKey) RotateKey(userId, secureHash string) error {
	c.Lock()
	defer c.Unlock()

	c.longTermData.SecureKeyStorageById[userId] = secureHash
	delete(c.weakKeyStorageById, userId)
	delete(c.longTermData.UserKeyRevoked, userId)
	return nil
}

func (c *DynamicApiKey) DeleteUser(userId string) error {
	c.Lock()
	defer c.Unlock()

	delete(c.longTermData.SecureKeyStorageById, userId)
	delete(c.longTermData.IdToIdentifier, userId)
	delete(c.longTermData.IdentifierToId, c.longTermData.IdToIdentifier[userId])
	delete(c.longTermData.Users, userId)
	delete(c.weakKeyStorageById, userId)
	delete(c.longTermData.UserKeyRevoked, userId)
	return nil
}

func (c *DynamicApiKey) ActivateUser(userId string) error {
	c.Lock()
	defer c.Unlock()

	c.longTermData.Users[userId].Active = true
	return nil
}

func (c *DynamicApiKey) DeactivateUser(userId string, revokeKey bool) error {
	c.Lock()
	defer c.Unlock()
	if revokeKey {
		c.longTermData.UserKeyRevoked[userId] = struct{}{}
	}
	c.longTermData.Users[userId].Active = false
	return nil
}

func (c *DynamicApiKey) GetUsers(userIds ...string) (map[string]*User, error) {
	c.RLock()
	defer c.RUnlock()

	if len(userIds) == 0 {
		return c.longTermData.Users, nil
	}

	users := make(map[string]*User, len(userIds))
	for _, id := range userIds {
		user, ok := c.longTermData.Users[id]
		if ok {
			users[id] = user
		}
	}
	return users, nil
}

func (c *DynamicApiKey) CheckUserIdentifierExists(userIdentifier string) (bool, error) {
	c.RLock()
	defer c.RUnlock()

	_, ok := c.longTermData.Users[userIdentifier]
	return ok, nil
}

func (c *DynamicApiKey) ValidateAndExtract(key, userIdentifier string) (*models.Principal, error) {
	c.RLock()
	defer c.RUnlock()

	userId, ok := c.longTermData.IdentifierToId[userIdentifier]
	if !ok {
		return nil, fmt.Errorf("invalid token")
	}

	secureHash, ok := c.longTermData.SecureKeyStorageById[userId]
	if !ok {
		return nil, fmt.Errorf("invalid token")
	}
	weakHash, ok := c.weakKeyStorageById[userId]
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

	if c.longTermData.Users[userId] != nil && !c.longTermData.Users[userId].Active {
		return nil, fmt.Errorf("user deactivated")
	}
	if _, ok := c.longTermData.UserKeyRevoked[userId]; ok {
		return nil, fmt.Errorf("key is revoked")
	}

	return &models.Principal{Username: userId, UserType: models.UserTypeDb}, nil
}

func (c *DynamicApiKey) validateWeakHash(key []byte, weakHash [32]byte) error {
	keyHash := sha256.Sum256(key)
	if subtle.ConstantTimeCompare(keyHash[:], weakHash[:]) != 1 {
		return fmt.Errorf("invalid token")
	}

	return nil
}

func (c *DynamicApiKey) validateStrongHash(key, secureHash, userId string) error {
	match, err := argon2id.ComparePasswordAndHash(key, secureHash)
	if err != nil {
		return err
	}
	if !match {
		return fmt.Errorf("invalid token")
	}
	token := []byte(key + secureHash)
	c.weakKeyStorageById[userId] = sha256.Sum256(token)

	return nil
}

func (c *DynamicApiKey) getBytes() ([]byte, error) {
	c.Lock()
	defer c.Unlock()

	marshal, err := json.Marshal(c.longTermData)
	if err != nil {
		return nil, err
	}

	return marshal, nil
}

func (c *DynamicApiKey) restoreFromBytes(data []byte) error {
	c.Lock()
	defer c.Unlock()

	var dynamicUsers dynamicApiKeyBackup

	err := json.Unmarshal(data, &dynamicUsers)
	if err != nil {
		return err
	}

	c.longTermData = dynamicUsers
	clear(c.weakKeyStorageById)

	return nil
}

func (c *DynamicApiKey) BackupLocations() BackupWrapper {
	return NewBackupWrapper(c.getBytes, c.restoreFromBytes)
}

type BackupWrapper struct {
	getBytes             func() ([]byte, error)
	restoreFromBytesFunc func([]byte) error
}

func NewBackupWrapper(getbytesFunc func() ([]byte, error), restoreFromBytesFunc func([]byte) error) BackupWrapper {
	return BackupWrapper{getBytes: getbytesFunc, restoreFromBytesFunc: restoreFromBytesFunc}
}

func (b BackupWrapper) GetDescriptors(_ context.Context) (map[string]backup.OtherDescriptors, error) {
	bts, err := b.getBytes()
	if err != nil {
		return nil, err
	}
	return map[string]backup.OtherDescriptors{"dynamicUsers": {Content: bts}}, nil
}

func (b BackupWrapper) WriteDescriptors(_ context.Context, descriptors map[string]backup.OtherDescriptors) error {
	descr, ok := descriptors["dynamicUsers"]
	if !ok {
		return errors.New("no policies found")
	}

	return b.restoreFromBytesFunc(descr.Content)
}
