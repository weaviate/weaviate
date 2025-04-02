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
	"os"
	"path/filepath"
	"sync"

	"github.com/weaviate/weaviate/entities/backup"
	ucbackup "github.com/weaviate/weaviate/usecases/backup"

	"github.com/alexedwards/argon2id"

	"github.com/weaviate/weaviate/entities/models"
)

const (
	SnapshotVersion = 0
	FileName        = "users.json"
)

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
	lock          *sync.RWMutex
	data          dbUserdata
	memoryOnyData memoryOnlyData
	path          string
}

type DBUserSnapshot struct {
	Data    dbUserdata
	Version int
}

type dbUserdata struct {
	SecureKeyStorageById map[string]string
	IdentifierToId       map[string]string
	IdToIdentifier       map[string]string
	Users                map[string]*User
	UserKeyRevoked       map[string]struct{}
}

type memoryOnlyData struct {
	WeakKeyStorageById map[string][sha256.Size]byte
}

func NewDBUser(path string) (*DBUser, error) {
	fullpath := fmt.Sprintf("%s/raft/db_users/", path)
	err := createStorage(fullpath + FileName)
	if err != nil {
		return nil, err
	}
	existingData, err := ReadFile(fullpath + FileName)
	if err != nil {
		return nil, err
	}
	snapshot := DBUserSnapshot{}
	if len(existingData) > 0 {
		if err := json.Unmarshal(existingData, &snapshot); err != nil {
			return nil, err
		}
	}

	if snapshot.Data.SecureKeyStorageById == nil {
		snapshot.Data.SecureKeyStorageById = make(map[string]string)
	}
	if snapshot.Data.IdentifierToId == nil {
		snapshot.Data.IdentifierToId = make(map[string]string)
	}
	if snapshot.Data.IdToIdentifier == nil {
		snapshot.Data.IdToIdentifier = make(map[string]string)
	}
	if snapshot.Data.Users == nil {
		snapshot.Data.Users = make(map[string]*User)
	}
	if snapshot.Data.UserKeyRevoked == nil {
		snapshot.Data.UserKeyRevoked = make(map[string]struct{})
	}

	return &DBUser{
		path:          fullpath,
		lock:          &sync.RWMutex{},
		data:          snapshot.Data,
		memoryOnyData: memoryOnlyData{WeakKeyStorageById: make(map[string][sha256.Size]byte)},
	}, nil
}

func (c *DBUser) CreateUser(userId, secureHash, userIdentifier string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.data.SecureKeyStorageById[userId] = secureHash
	c.data.IdentifierToId[userIdentifier] = userId
	c.data.IdToIdentifier[userId] = userIdentifier
	c.data.Users[userId] = &User{Id: userId, Active: true, InternalIdentifier: userIdentifier}
	return c.storeToFile()
}

func (c *DBUser) RotateKey(userId, secureHash string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.data.SecureKeyStorageById[userId] = secureHash
	delete(c.memoryOnyData.WeakKeyStorageById, userId)
	delete(c.data.UserKeyRevoked, userId)
	return c.storeToFile()
}

func (c *DBUser) DeleteUser(userId string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.data.SecureKeyStorageById, userId)
	delete(c.data.IdToIdentifier, userId)
	delete(c.data.IdentifierToId, c.data.IdToIdentifier[userId])
	delete(c.data.Users, userId)
	delete(c.memoryOnyData.WeakKeyStorageById, userId)
	delete(c.data.UserKeyRevoked, userId)
	return c.storeToFile()
}

func (c *DBUser) ActivateUser(userId string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.data.Users[userId].Active = true
	return c.storeToFile()
}

func (c *DBUser) DeactivateUser(userId string, revokeKey bool) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if revokeKey {
		c.data.UserKeyRevoked[userId] = struct{}{}
	}
	c.data.Users[userId].Active = false

	return c.storeToFile()
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
	weakHash, ok := c.memoryOnyData.WeakKeyStorageById[userId]
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
	c.memoryOnyData.WeakKeyStorageById[userId] = sha256.Sum256(token)

	return nil
}

func (c *DBUser) Snapshot() DBUserSnapshot {
	c.lock.Lock()
	defer c.lock.Unlock()

	return DBUserSnapshot{Data: c.data, Version: SnapshotVersion}
}

func (c *DBUser) Restore(snapshot DBUserSnapshot) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if snapshot.Version != SnapshotVersion {
		return fmt.Errorf("invalid snapshot version")
	}
	c.data = snapshot.Data

	return nil
}

func (c *DBUser) storeToFile() error {
	data, err := json.Marshal(DBUserSnapshot{Data: c.data, Version: SnapshotVersion})
	if err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(c.path, "temp-*.tmp")
	if err != nil {
		return err
	}
	tempFilename := tmpFile.Name()

	defer func() {
		tmpFile.Close()
		os.Remove(tempFilename) // Remove temp file if it still exists
	}()

	// Write data to temp file, flush and close
	if _, err := tmpFile.Write(data); err != nil {
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	// Atomically rename the temp file to the target filename to not leave garbage when it crashes
	return os.Rename(tempFilename, c.path+"/"+FileName)
}

func createStorage(filePath string) error {
	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories: %w", err)
	}

	_, err := os.Stat(filePath)
	if err == nil { // file exists
		return nil
	}

	if os.IsNotExist(err) {
		file, err := os.Create(filePath)
		if err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}
		defer file.Close()
		return nil
	}

	return err
}

func ReadFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	data := make([]byte, fileInfo.Size())

	_, err = file.Read(data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (c *DBUser) getBytes() ([]byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	marshal, err := json.Marshal(c.data)
	if err != nil {
		return nil, err
	}

	return marshal, nil
}

func (c *DBUser) restoreFromBytes(data []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	var dynamicUsers dbUserdata

	err := json.Unmarshal(data, &dynamicUsers)
	if err != nil {
		return err
	}

	c.data = dynamicUsers
	clear(c.memoryOnyData.WeakKeyStorageById)

	return nil
}

func (c *DBUser) BackupLocations() ucbackup.SourcerNonClass {
	if c == nil {
		// Dynamic User Management is not enabled, there's nothing to backup
		return nil
	}
	return NewBackupWrapper(c.getBytes, c.restoreFromBytes)
}

type BackupWrapper struct {
	getBytes             func() ([]byte, error)
	restoreFromBytesFunc func([]byte) error
}

func NewBackupWrapper(getbytesFunc func() ([]byte, error), restoreFromBytesFunc func([]byte) error) *BackupWrapper {
	return &BackupWrapper{getBytes: getbytesFunc, restoreFromBytesFunc: restoreFromBytesFunc}
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
