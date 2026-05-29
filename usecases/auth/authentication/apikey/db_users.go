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
	"time"

	"github.com/alexedwards/argon2id"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

const (
	SnapshotVersion   = 0
	FileName          = "users.json"
	UserNameMaxLength = 128
	UserNameRegexCore = `[A-Za-z][-_0-9A-Za-z@.]{0,128}`
)

// MakeUserKey returns the internal storage key for a user. Namespaced users
// are stored under "namespace<sep>userId" so two namespaces can host the same
// short id without collision; unnamespaced users keep the bare id for
// backward compatibility with pre-namespace data.
//
// The separator is the cluster-wide schema.NamespaceSeparator (also used to
// qualify class names): ":" is excluded from UserNameRegexCore, so a
// user-supplied id can never contain it and the split-back is unambiguous.
func MakeUserKey(userId, namespace string) string {
	return namespacing.QualifiedName(namespace, userId)
}

// DBUsers is the cluster-side interface implemented by *cluster.Raft.
// Write methods accept a context so the implementation can propagate
// request cancellation through RAFT.
type DBUsers interface {
	CreateUser(ctx context.Context, userId, secureHash, userIdentifier, apiKeyFirstLetters, namespace string, createdAt time.Time) error
	CreateUserWithKey(ctx context.Context, userId, apiKeyFirstLetters string, weakHash [sha256.Size]byte, createdAt time.Time) error
	DeleteUser(ctx context.Context, userId string) error
	ActivateUser(ctx context.Context, userId string) error
	DeactivateUser(ctx context.Context, userId string, revokeKey bool) error
	GetUsers(userIds ...string) (map[string]*User, error)
	RotateKey(ctx context.Context, userId, apiKeyFirstLetters, secureHash, oldIdentifier, newIdentifier string) error
	CheckUserIdentifierExists(userIdentifier string) (bool, error)
}

type User struct {
	sync.RWMutex
	Id                 string
	Active             bool
	InternalIdentifier string
	ApiKeyFirstLetters string
	CreatedAt          time.Time
	LastUsedAt         time.Time
	ImportedWithKey    bool
	Namespace          string
}

type DBUser struct {
	lock           *sync.RWMutex
	singleFlight   singleflight.Group
	data           dbUserdata
	memoryOnlyData memoryOnlyData
	path           string
	enabled        bool
}

type DBUserSnapshot struct {
	Data    dbUserdata
	Version int
}

type dbUserdata struct {
	SecureKeyStorageById    map[string]string
	IdentifierToId          map[string]string
	IdToIdentifier          map[string]string
	Users                   map[string]*User
	UserKeyRevoked          map[string]struct{}
	ImportedApiKeysWeakHash map[string][sha256.Size]byte
}

type memoryOnlyData struct {
	weakKeyStorageById *sync.Map // maps userId -> [sha256.Size]byte
	// imported keys from static users should not work after key rotation, eg the following scenario
	// - import user with "key"
	// - login works when using "key" through dynamic users
	// - key rotation "key" => "new-key"
	// - login works when using "new-key" through dynamic users
	// - login using "key" is blocked and does not reach static user config where the old key is still present
	//
	// Note that this will NOT be persisted and we expect that the static user configuration does not contain "key" anymore
	// on the next restart
	importedApiKeysBlocked [][sha256.Size]byte
}

func NewDBUser(path string, enabled bool, logger logrus.FieldLogger) (*DBUser, error) {
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

	data := restoreAllFields(snapshot.Data)

	dbUsers := &DBUser{
		path: fullpath,
		lock: &sync.RWMutex{},
		data: data,
		memoryOnlyData: memoryOnlyData{
			weakKeyStorageById:     &sync.Map{},
			importedApiKeysBlocked: make([][sha256.Size]byte, 0),
		},
		enabled: enabled,
	}

	// we save every change to file after a request is done, EXCEPT the lastUsedAt time as we do not want to write to a
	// file with every request.
	// This information is not terribly important (besides WCD UX), so it does not matter much if we very rarely loose
	// some information here. This info will also be written on shutdown so the only loss of information occurs with
	// OOM or similar.
	if enabled {
		enterrors.GoWrapper(func() {
			ticker := time.NewTicker(1 * time.Minute)
			for range ticker.C {
				func() {
					dbUsers.lock.RLock()
					defer dbUsers.lock.RUnlock()
					err := dbUsers.storeToFile()
					if err != nil {
						logger.WithField("action", "db_users_write_to_file").
							WithField("error", err).
							Warn("db users file not written")
					}
				}()
			}
		}, logger)
	}

	return dbUsers, nil
}

// ensure all fields are non-nil after a restore to avoid nil map panics
func restoreAllFields(data dbUserdata) dbUserdata {
	if data.SecureKeyStorageById == nil {
		data.SecureKeyStorageById = make(map[string]string)
	}
	if data.IdentifierToId == nil {
		data.IdentifierToId = make(map[string]string)
	}
	if data.IdToIdentifier == nil {
		data.IdToIdentifier = make(map[string]string)
	}
	if data.Users == nil {
		data.Users = make(map[string]*User)
	}
	if data.UserKeyRevoked == nil {
		data.UserKeyRevoked = make(map[string]struct{})
	}

	if data.ImportedApiKeysWeakHash == nil {
		data.ImportedApiKeysWeakHash = make(map[string][sha256.Size]byte)
	}
	return data
}

func (c *DBUser) CreateUser(userId, secureHash, userIdentifier, apiKeyFirstLetters, namespace string, createdAt time.Time) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(apiKeyFirstLetters) > 3 {
		return errors.New("api key first letters too long")
	}

	c.data.SecureKeyStorageById[userId] = secureHash
	c.data.IdentifierToId[userIdentifier] = userId
	c.data.IdToIdentifier[userId] = userIdentifier
	c.data.Users[userId] = &User{
		Id:                 userId,
		Active:             true,
		InternalIdentifier: userIdentifier,
		CreatedAt:          createdAt,
		ApiKeyFirstLetters: apiKeyFirstLetters,
		Namespace:          namespace,
	}
	return c.storeToFile()
}

func (c *DBUser) CreateUserWithKey(userId, apiKeyFirstLetters string, weakHash [sha256.Size]byte, createdAt time.Time) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(apiKeyFirstLetters) > 3 {
		return errors.New("api key first letters too long")
	}

	c.data.ImportedApiKeysWeakHash[userId] = weakHash
	c.memoryOnlyData.importedApiKeysBlocked = append(c.memoryOnlyData.importedApiKeysBlocked, weakHash)
	c.data.Users[userId] = &User{
		Id:                 userId,
		Active:             true,
		InternalIdentifier: "imported_" + userId,
		CreatedAt:          createdAt,
		ApiKeyFirstLetters: apiKeyFirstLetters,
		ImportedWithKey:    true,
	}
	return c.storeToFile()
}

func (c *DBUser) RotateKey(userId, apiKeyFirstLetters, secureHash, oldIdentifier, newIdentifier string) error {
	if len(apiKeyFirstLetters) > 3 {
		return errors.New("api key first letters too long")
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.data.Users[userId]; !ok {
		return fmt.Errorf("user %s does not exist", userId)
	}

	// replay of old raft commands can have these be ""
	if oldIdentifier != "" && newIdentifier != "" {
		c.data.IdToIdentifier[userId] = newIdentifier
		delete(c.data.IdentifierToId, oldIdentifier)
		c.data.IdentifierToId[newIdentifier] = userId
		c.data.Users[userId].InternalIdentifier = newIdentifier
	}
	if c.data.Users[userId].ImportedWithKey {
		c.data.Users[userId].ImportedWithKey = false
		delete(c.data.ImportedApiKeysWeakHash, userId)

	}

	c.data.Users[userId].ApiKeyFirstLetters = apiKeyFirstLetters
	c.data.SecureKeyStorageById[userId] = secureHash
	c.memoryOnlyData.weakKeyStorageById.Delete(userId)
	delete(c.data.UserKeyRevoked, userId)
	return c.storeToFile()
}

func (c *DBUser) DeleteUser(userId string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.deleteUserLocked(userId)
	return c.storeToFile()
}

// deleteUserLocked removes userId from every in-memory map. Caller must
// hold the write lock and call storeToFile.
func (c *DBUser) deleteUserLocked(userId string) {
	delete(c.data.SecureKeyStorageById, userId)
	delete(c.data.IdentifierToId, c.data.IdToIdentifier[userId])
	delete(c.data.IdToIdentifier, userId)
	delete(c.data.Users, userId)
	c.memoryOnlyData.weakKeyStorageById.Delete(userId)
	delete(c.data.UserKeyRevoked, userId)
	delete(c.data.ImportedApiKeysWeakHash, userId)
}

// UsersInNamespace returns the IDs of users bound to namespace in
// unspecified order. An empty namespace returns nil.
//
// The returned IDs are whatever userId was passed to CreateUser. In the
// namespaced-handler path that is the [MakeUserKey] qualified form (e.g.
// "alpha:bob"); DBUser itself does not enforce that shape. Treat them as
// opaque handles for existence/count checks; do not surface them in
// user-facing responses.
func (c *DBUser) UsersInNamespace(namespace string) []string {
	if namespace == "" {
		return nil
	}
	c.lock.RLock()
	defer c.lock.RUnlock()

	out := make([]string, 0)
	for id, u := range c.data.Users {
		if u != nil && u.Namespace == namespace {
			out = append(out, id)
		}
	}
	return out
}

// DeleteUsersInNamespace removes every user bound to namespace. An empty
// namespace argument is rejected so the helper cannot wipe unscoped users.
func (c *DBUser) DeleteUsersInNamespace(namespace string) error {
	if namespace == "" {
		return errors.New("namespace is required")
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	deleted := false
	for id, u := range c.data.Users {
		if u != nil && u.Namespace == namespace {
			c.deleteUserLocked(id)
			deleted = true
		}
	}
	if !deleted {
		return nil
	}
	return c.storeToFile()
}

func (c *DBUser) ActivateUser(userId string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.data.Users[userId]; !ok {
		return fmt.Errorf("user %s does not exist", userId)
	}

	c.data.Users[userId].Active = true
	return c.storeToFile()
}

func (c *DBUser) DeactivateUser(userId string, revokeKey bool) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, ok := c.data.Users[userId]; !ok {
		return fmt.Errorf("user %s does not exist", userId)
	}
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

// ListAllUsers returns storage keys (qualified "namespace:userId" form, see
// [MakeUserKey]) — directly matchable against backup includeUsers selectors.
func (c *DBUser) ListAllUsers() []string {
	c.lock.RLock()
	defer c.lock.RUnlock()

	out := make([]string, 0, len(c.data.Users))
	for id := range c.data.Users {
		out = append(out, id)
	}
	return out
}

func (c *DBUser) CheckUserIdentifierExists(userIdentifier string) (bool, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	_, ok := c.data.IdentifierToId[userIdentifier]
	return ok, nil
}

func (c *DBUser) UpdateLastUsedTimestamp(users map[string]time.Time) {
	// RLock is fine here, we only want to avoid that c.data.Users is being changed. LastUsed has its own
	// locking mechanism
	c.lock.RLock()
	defer c.lock.RUnlock()

	for userID, lastUsed := range users {
		user, ok := c.data.Users[userID]
		if !ok {
			continue
		}
		if user.LastUsedAt.Before(lastUsed) {
			func() {
				user.Lock()
				defer user.Unlock()
				user.LastUsedAt = lastUsed
			}()
		}
	}
}

func (c *DBUser) ValidateImportedKey(token string) (*models.Principal, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	keyHashGiven := sha256.Sum256([]byte(token))
	for userId, keyHashStored := range c.data.ImportedApiKeysWeakHash {
		if subtle.ConstantTimeCompare(keyHashGiven[:], keyHashStored[:]) != 1 {
			continue
		}
		if c.data.Users[userId] != nil && !c.data.Users[userId].Active {
			return nil, fmt.Errorf("user deactivated")
		}

		if _, ok := c.data.UserKeyRevoked[userId]; ok {
			return nil, fmt.Errorf("key is revoked")
		}

		// imported keys are always without a namespace, something is seriously wrong here
		if c.data.Users[userId].Namespace != "" {
			return nil, fmt.Errorf("imported key with namespace %v", c.data.Users[userId].Namespace)
		}

		// Last used time does not have to be exact. If we have multiple concurrent requests for the same
		// user, only recording one of them is good enough
		if c.data.Users[userId].TryLock() {
			c.data.Users[userId].LastUsedAt = time.Now()
			c.data.Users[userId].Unlock()
		}

		return &models.Principal{
			Username:         userId,
			UserType:         models.UserTypeInputDb,
			Namespace:        "",
			IsGlobalOperator: false,
		}, nil
	}

	return nil, nil
}

func (c *DBUser) IsBlockedKey(token string) bool {
	keyHashGiven := sha256.Sum256([]byte(token))
	for _, keyHashStored := range c.memoryOnlyData.importedApiKeysBlocked {
		if subtle.ConstantTimeCompare(keyHashGiven[:], keyHashStored[:]) == 1 {
			return true
		}
	}
	return false
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
	weakHashValue, ok := c.memoryOnlyData.weakKeyStorageById.Load(userId)
	if !ok {
		// Ensure only one Argon2 verification runs for this user
		if _, err, _ := c.singleFlight.Do("auth:"+userId, func() (any, error) {
			return nil, c.validateStrongHash(key, secureHash, userId)
		}); err != nil {
			return nil, err
		}
		weakHashValue, _ = c.memoryOnlyData.weakKeyStorageById.Load(userId)
	}

	weakHash := weakHashValue.([sha256.Size]byte)
	if err := c.validateWeakHash([]byte(key+secureHash), weakHash); err != nil {
		return nil, err
	}

	if c.data.Users[userId] != nil && !c.data.Users[userId].Active {
		return nil, fmt.Errorf("user deactivated")
	}
	if _, ok := c.data.UserKeyRevoked[userId]; ok {
		return nil, fmt.Errorf("key is revoked")
	}

	// Last used time does not have to be exact. If we have multiple concurrent requests for the same
	// user, only recording one of them is good enough
	if c.data.Users[userId].TryLock() {
		c.data.Users[userId].LastUsedAt = time.Now()
		c.data.Users[userId].Unlock()
	}

	return &models.Principal{
		Username:         userId,
		UserType:         models.UserTypeInputDb,
		Namespace:        c.data.Users[userId].Namespace,
		IsGlobalOperator: false,
	}, nil
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
	weakHash := sha256.Sum256(token)
	c.memoryOnlyData.weakKeyStorageById.Store(userId, weakHash)

	return nil
}

// Snapshot serialises the dynamic-user state to JSON. Zero userIDs captures
// the whole cluster (RAFT FSM, ordinary backups); a non-empty set is the
// graduation path, where a missing id errors rather than ship incomplete.
func (c *DBUser) Snapshot(userIDs ...string) ([]byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	data := c.data
	if len(userIDs) > 0 {
		filtered, err := filterDBUserData(c.data, userIDs)
		if err != nil {
			return nil, err
		}
		data = filtered
	}

	marshal, err := json.Marshal(DBUserSnapshot{Data: data, Version: SnapshotVersion})
	if err != nil {
		return nil, err
	}
	return marshal, nil
}

// filterDBUserData returns src restricted to ids. A missing id errors rather
// than silently dropping — the resolved set must materialise exactly.
func filterDBUserData(src dbUserdata, ids []string) (dbUserdata, error) {
	keep := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := src.Users[id]; !ok {
			return dbUserdata{}, fmt.Errorf("dynamic user %q not found in snapshot source", id)
		}
		keep[id] = struct{}{}
	}

	out := dbUserdata{
		SecureKeyStorageById:    make(map[string]string, len(keep)),
		IdentifierToId:          make(map[string]string, len(keep)),
		IdToIdentifier:          make(map[string]string, len(keep)),
		Users:                   make(map[string]*User, len(keep)),
		UserKeyRevoked:          make(map[string]struct{}, len(keep)),
		ImportedApiKeysWeakHash: make(map[string][sha256.Size]byte, len(keep)),
	}

	// Users is the existence-of-truth map (verified above); absence from any
	// other id-keyed map is fine.
	for id := range keep {
		out.Users[id] = src.Users[id] // pointer copy; same lock that produced src
		if v, ok := src.SecureKeyStorageById[id]; ok {
			out.SecureKeyStorageById[id] = v
		}
		if v, ok := src.IdToIdentifier[id]; ok {
			out.IdToIdentifier[id] = v
		}
		if _, ok := src.UserKeyRevoked[id]; ok {
			out.UserKeyRevoked[id] = struct{}{}
		}
		if v, ok := src.ImportedApiKeysWeakHash[id]; ok {
			out.ImportedApiKeysWeakHash[id] = v
		}
	}

	// IdentifierToId is value-keyed by user id; filter by value.
	for identifier, id := range src.IdentifierToId {
		if _, ok := keep[id]; ok {
			out.IdentifierToId[identifier] = id
		}
	}

	return out, nil
}

// Restore replaces in-memory state with snapshot. stripNamespaces=true is the
// graduation path: ids drop the "<namespace>:" prefix and User.Namespace is
// cleared, with a collision erroring rather than overwriting credentials.
func (c *DBUser) Restore(snapshot []byte, stripNamespaces bool) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// don't overwrite with empty snapshot to avoid overwriting recovery from file
	// with a non-existent db user snapshot when coming from old versions
	if len(snapshot) == 0 {
		return nil
	}

	snapshotRestore := DBUserSnapshot{}
	err := json.Unmarshal(snapshot, &snapshotRestore)
	if err != nil {
		return err
	}

	if snapshotRestore.Version != SnapshotVersion {
		return fmt.Errorf("invalid snapshot version")
	}

	if stripNamespaces {
		stripped, err := stripDBUserNamespace(snapshotRestore.Data)
		if err != nil {
			return err
		}
		snapshotRestore.Data = stripped
	}

	c.data = restoreAllFields(snapshotRestore.Data)

	return nil
}

// stripDBUserNamespace drops the "<namespace>:" prefix from every id-bearing
// field; foreign-prefix or bare ids pass through. A post-strip collision
// errors with an empty result — silent overwrite would corrupt credentials.
func stripDBUserNamespace(src dbUserdata) (dbUserdata, error) {
	out := dbUserdata{
		SecureKeyStorageById: make(map[string]string, len(src.SecureKeyStorageById)),
		IdentifierToId:       make(map[string]string, len(src.IdentifierToId)),
		IdToIdentifier:       make(map[string]string, len(src.IdToIdentifier)),
		Users:                make(map[string]*User, len(src.Users)),
		UserKeyRevoked:       make(map[string]struct{}, len(src.UserKeyRevoked)),
		// Imported keys are never namespaced so don't need stripping
		ImportedApiKeysWeakHash: src.ImportedApiKeysWeakHash,
	}

	for id, user := range src.Users {
		newID := namespacing.StripNamespacePrefix(id)
		if _, exists := out.Users[newID]; exists {
			return dbUserdata{}, fmt.Errorf("namespace strip would alias dynamic users under %q", newID)
		}
		// Fresh User — [User] embeds sync.RWMutex; value-copy would copylock.
		cp := &User{
			Id:                 user.Id,
			Active:             user.Active,
			InternalIdentifier: user.InternalIdentifier,
			ApiKeyFirstLetters: user.ApiKeyFirstLetters,
			CreatedAt:          user.CreatedAt,
			LastUsedAt:         user.LastUsedAt,
			ImportedWithKey:    user.ImportedWithKey,
			Namespace:          user.Namespace,
		}
		if newID != id {
			cp.Id = newID
			cp.Namespace = ""
		}
		out.Users[newID] = cp
	}

	for id, v := range src.SecureKeyStorageById {
		newID := namespacing.StripNamespacePrefix(id)
		if _, exists := out.SecureKeyStorageById[newID]; exists {
			return dbUserdata{}, fmt.Errorf("namespace strip would alias SecureKeyStorageById under %q", newID)
		}
		out.SecureKeyStorageById[newID] = v
	}
	for id, v := range src.IdToIdentifier {
		newID := namespacing.StripNamespacePrefix(id)
		if _, exists := out.IdToIdentifier[newID]; exists {
			return dbUserdata{}, fmt.Errorf("namespace strip would alias IdToIdentifier under %q", newID)
		}
		out.IdToIdentifier[newID] = v
	}
	for id := range src.UserKeyRevoked {
		newID := namespacing.StripNamespacePrefix(id)
		if _, exists := out.UserKeyRevoked[newID]; exists {
			return dbUserdata{}, fmt.Errorf("namespace strip would alias UserKeyRevoked under %q", newID)
		}
		out.UserKeyRevoked[newID] = struct{}{}
	}
	// IdentifierToId is value-keyed by user id; rewrite the value.
	for identifier, id := range src.IdentifierToId {
		out.IdentifierToId[identifier] = namespacing.StripNamespacePrefix(id)
	}

	return out, nil
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

func (c *DBUser) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.storeToFile()
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
