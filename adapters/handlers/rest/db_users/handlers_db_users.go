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

package db_users

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/weaviate/weaviate/usecases/auth/authentication"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/weaviate/weaviate/adapters/clients"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/usecases/auth/authorization/adminlist"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/filter"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

type dynUserHandler struct {
	authorizer           authorization.Authorizer
	dbUsers              DbUserAndRolesGetter
	staticApiKeysConfigs config.StaticAPIKey
	rbacConfig           rbacconf.Config
	adminListConfig      adminlist.Config
	logger               logrus.FieldLogger
	dbUserEnabled        bool
	remoteUser           *clients.RemoteUser
	nodesGetter          schema.SchemaGetter
	namespacesEnabled    bool
	namespaces           namespaces.Exister
}

type DbUserAndRolesGetter interface {
	apikey.DBUsers
	GetRolesForUserOrGroup(user string, authTyoes authentication.AuthType, isGroup bool) (map[string][]authorization.Policy, error)
	RevokeRolesForUser(userName string, roles ...string) error
}

var validateUserNameRegex = regexp.MustCompile(`^` + apikey.UserNameRegexCore + `$`)

func SetupHandlers(
	api *operations.WeaviateAPI, dbUsers DbUserAndRolesGetter, authorizer authorization.Authorizer, authNConfig config.Authentication,
	authZConfig config.Authorization, remoteUser *clients.RemoteUser, nodesGetter schema.SchemaGetter,
	namespacesEnabled bool, ns namespaces.Exister, logger logrus.FieldLogger,
) {
	h := &dynUserHandler{
		authorizer:           authorizer,
		dbUsers:              dbUsers,
		staticApiKeysConfigs: authNConfig.APIKey,
		dbUserEnabled:        authNConfig.DBUsers.Enabled,
		rbacConfig:           authZConfig.Rbac,
		remoteUser:           remoteUser,
		nodesGetter:          nodesGetter,
		namespacesEnabled:    namespacesEnabled,
		namespaces:           ns,
		logger:               logger,
	}

	api.UsersCreateUserHandler = users.CreateUserHandlerFunc(h.createUser)
	api.UsersDeleteUserHandler = users.DeleteUserHandlerFunc(h.deleteUser)
	api.UsersGetUserInfoHandler = users.GetUserInfoHandlerFunc(h.getUser)
	api.UsersRotateUserAPIKeyHandler = users.RotateUserAPIKeyHandlerFunc(h.rotateKey)
	api.UsersDeactivateUserHandler = users.DeactivateUserHandlerFunc(h.deactivateUser)
	api.UsersActivateUserHandler = users.ActivateUserHandlerFunc(h.activateUser)
	api.UsersListAllUsersHandler = users.ListAllUsersHandlerFunc(h.listUsers)
}

func (h *dynUserHandler) listUsers(params users.ListAllUsersParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	isRootUser := h.isRequestFromRootUser(principal)

	if !h.dbUserEnabled {
		return users.NewListAllUsersOK().WithPayload([]*models.DBUserInfo{})
	}

	allDbUsers, err := h.dbUsers.GetUsers()
	if err != nil {
		return users.NewListAllUsersInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	allUsers := make([]apikey.UserView, 0, len(allDbUsers))
	for _, dbUser := range allDbUsers {
		allUsers = append(allUsers, dbUser)
	}

	resourceFilter := filter.New[apikey.UserView](h.authorizer, h.rbacConfig)
	filteredUsers := resourceFilter.Filter(
		ctx,
		principal,
		allUsers,
		authorization.READ,
		func(user apikey.UserView) string {
			return authorization.Users(user.Id)[0]
		},
	)

	var usersWithTime map[string]time.Time
	if params.IncludeLastUsedTime != nil && *params.IncludeLastUsedTime {
		usersWithTime = h.getLastUsed(filteredUsers)
	}

	exposeNamespace := principal != nil && principal.IsGlobalOperator

	allDynamicUsers := map[string]struct{}{}
	response := make([]*models.DBUserInfo, 0, len(filteredUsers))
	for _, dbUser := range filteredUsers {
		apiKeyFirstLetter := ""
		if isRootUser {
			apiKeyFirstLetter = dbUser.ApiKeyFirstLetters
		}
		var lastUsedTime time.Time
		if val, ok := usersWithTime[dbUser.Id]; ok {
			lastUsedTime = val
		}
		namespace := ""
		if exposeNamespace {
			namespace = dbUser.Namespace
		}
		// dbUser.Id is the qualified storage key; show the short form to namespaced callers.
		displayID := namespacing.StripOwnNamespace(principal, dbUser.Id)
		response, err = h.addToListAllResponse(response, dbUser.Id, displayID, string(models.UserTypeOutputDbUser), dbUser.Active, apiKeyFirstLetter, namespace, &dbUser.CreatedAt, &lastUsedTime)
		if err != nil {
			return users.NewListAllUsersInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
		if isRootUser {
			allDynamicUsers[dbUser.Id] = struct{}{}
		}
	}

	if isRootUser {
		for _, staticUser := range h.staticApiKeysConfigs.Users {
			if _, ok := allDynamicUsers[staticUser]; ok {
				// don't overwrite dynamic users with the same name. Can happen after import
				continue
			}
			response, err = h.addToListAllResponse(response, staticUser, staticUser, string(models.UserTypeOutputDbEnvUser), true, "", "", nil, nil)
			if err != nil {
				return users.NewListAllUsersInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
			}
		}
	}

	return users.NewListAllUsersOK().WithPayload(response)
}

func (h *dynUserHandler) addToListAllResponse(response []*models.DBUserInfo, internalID, displayID, userType string, active bool, apiKeyFirstLetter, namespace string, createdAt *time.Time, lastusedAt *time.Time) ([]*models.DBUserInfo, error) {
	roles, err := h.dbUsers.GetRolesForUserOrGroup(internalID, authentication.AuthTypeDb, false)
	if err != nil {
		return response, err
	}

	roleNames := make([]string, 0, len(roles))
	for role := range roles {
		roleNames = append(roleNames, role)
	}

	resp := &models.DBUserInfo{
		Active:             &active,
		UserID:             &displayID,
		DbUserType:         &userType,
		Roles:              roleNames,
		APIKeyFirstLetters: apiKeyFirstLetter,
		Namespace:          namespace,
	}
	if createdAt != nil {
		resp.CreatedAt = strfmt.DateTime(*createdAt)
	}
	if lastusedAt != nil {
		resp.LastUsedAt = strfmt.DateTime(*lastusedAt)
	}

	response = append(response, resp)
	return response, nil
}

func (h *dynUserHandler) getUser(params users.GetUserInfoParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	internalKey := namespacing.QualifyUserIDForLookup(principal, h.namespacesEnabled, params.UserID)

	if err := h.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Users(internalKey)...); err != nil {
		return users.NewGetUserInfoForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if !h.dbUserEnabled {
		return users.NewGetUserInfoUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("db user management is not enabled")))
	}

	// also check for existing static users if request comes from root
	isRootUser := h.isRequestFromRootUser(principal)

	active := true
	displayID := namespacing.StripOwnNamespace(principal, internalKey)
	response := &models.DBUserInfo{UserID: &displayID, Active: &active}

	existingDbUsers, err := h.dbUsers.GetUsers(internalKey)
	if err != nil {
		return users.NewGetUserInfoInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("checking user existence: %w", err)))
	}
	var userType string
	if len(existingDbUsers) > 0 {
		user := existingDbUsers[internalKey]
		response.Active = &user.Active
		response.CreatedAt = strfmt.DateTime(user.CreatedAt)
		if isRootUser {
			response.APIKeyFirstLetters = user.ApiKeyFirstLetters
		}
		if principal != nil && principal.IsGlobalOperator {
			response.Namespace = user.Namespace
		}

		if params.IncludeLastUsedTime != nil && *params.IncludeLastUsedTime {
			usersWithTime := h.getLastUsed([]apikey.UserView{user})
			response.LastUsedAt = strfmt.DateTime(usersWithTime[internalKey])
		}
		userType = string(models.UserTypeOutputDbUser)
	} else if isRootUser && h.staticUserExists(internalKey) {
		userType = string(models.UserTypeOutputDbEnvUser)
	} else {
		return users.NewGetUserInfoNotFound()
	}
	response.DbUserType = &userType

	existingRoles, err := h.dbUsers.GetRolesForUserOrGroup(internalKey, authentication.AuthTypeDb, false)
	if err != nil {
		return users.NewGetUserInfoInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("get roles: %w", err)))
	}

	roles := make([]string, 0, len(existingRoles))
	for roleName := range existingRoles {
		roles = append(roles, roleName)
	}
	response.Roles = roles

	return users.NewGetUserInfoOK().WithPayload(response)
}

func (h *dynUserHandler) getLastUsed(users []apikey.UserView) map[string]time.Time {
	usersWithTime := make(map[string]time.Time, len(users))
	for _, user := range users {
		usersWithTime[user.Id] = user.LastUsedAt
	}

	nodes := h.nodesGetter.Nodes()
	if len(nodes) == 1 {
		return usersWithTime
	}

	// we tolerate errors in requests to other nodes and don't want to wait too long. Last used time is a best-effort
	// operation
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()
	userStatuses := make([]*apikey.UserStatusResponse, len(nodes))
	wg := &sync.WaitGroup{}
	wg.Add(len(nodes))
	for i, nodeName := range nodes {
		i, nodeName := i, nodeName
		enterrors.GoWrapper(func() {
			defer wg.Done()
			status, err := h.remoteUser.GetAndUpdateLastUsedTime(ctx, nodeName, usersWithTime, true)
			if err == nil {
				userStatuses[i] = status
			}
		}, h.logger)
	}
	wg.Wait()

	for _, status := range userStatuses {
		if status == nil {
			continue
		}
		for userId, lastUsedTime := range status.Users {
			if lastUsedTime.After(usersWithTime[userId]) {
				usersWithTime[userId] = lastUsedTime
			}
		}
	}

	// update all other nodes with maximum time so usage does not "jump back" when the node that has the latest time
	// recorded is down.
	// This is opportunistic (we dont care about errors) and there is no need to keep the request waiting for this
	enterrors.GoWrapper(func() {
		ctx2, cancelFunc2 := context.WithTimeout(context.Background(), time.Second)
		defer cancelFunc2()
		wg := &sync.WaitGroup{}
		wg.Add(len(nodes))
		for _, nodeName := range nodes {
			nodeName := nodeName
			enterrors.GoWrapper(func() {
				defer wg.Done()
				// dont care about returns or errors
				_, _ = h.remoteUser.GetAndUpdateLastUsedTime(ctx2, nodeName, usersWithTime, false)
			}, h.logger)
		}
		wg.Wait() // wait so cancelFunc2 is not executed too early
	}, h.logger)

	return usersWithTime
}

// resolveUserKeyForCreate validates the user id and returns the storage key
// plus the derived namespace ("" when namespaces are disabled).
func (h *dynUserHandler) resolveUserKeyForCreate(principal *models.Principal, raw string) (key, ns string, err error) {
	if !h.namespacesEnabled {
		if err := validateUserName(raw); err != nil {
			return "", "", err
		}
		return raw, "", nil
	}

	if err := namespacing.ValidateNamespacePrefix(principal, h.namespacesEnabled, raw, "user"); err != nil {
		return "", "", err
	}

	if principal == nil || principal.IsGlobalOperator {
		ns = namespacing.NamespaceFromQualified(raw)
		if ns == "" {
			return "", "", errors.New(`a namespace-qualified user name "<namespace>:<user>" is required`)
		}
		// Validate the portion after the "<ns>:" prefix.
		if err := validateUserName(raw[len(ns)+1:]); err != nil {
			return "", "", err
		}
		return raw, ns, nil
	}

	// Namespaced principal: short name only (a ':' was rejected above).
	if err := validateUserName(raw); err != nil {
		return "", "", err
	}
	return apikey.MakeUserKey(raw, principal.Namespace), principal.Namespace, nil
}

func (h *dynUserHandler) createUser(params users.CreateUserParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	internalKey, ns, err := h.resolveUserKeyForCreate(principal, params.UserID)
	if err != nil {
		return users.NewCreateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.authorizer.Authorize(ctx, principal, authorization.CREATE, authorization.Users(internalKey)...); err != nil {
		return users.NewCreateUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if !h.dbUserEnabled {
		return users.NewCreateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("db user management is not enabled")))
	}

	if params.Body.Import != nil && *params.Body.Import && h.namespacesEnabled {
		return users.NewCreateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("import is not supported on namespace-enabled clusters")))
	}

	if params.Body.Import != nil && *params.Body.Import {
		if !h.principalIsRootUser(principal.Username) {
			return users.NewCreateUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("only root users can import static api keys")))
		}

		if !h.staticUserExists(params.UserID) {
			return users.NewCreateUserNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("static user %v does not exist", params.UserID)))
		}

		var apiKey string
		for i, user := range h.staticApiKeysConfigs.Users {
			if user == params.UserID {
				apiKey = h.staticApiKeysConfigs.AllowedKeys[i]
			}
		}

		createdAt := time.Now()
		if !time.Time(params.Body.CreateTime).IsZero() {
			createdAt = time.Time(params.Body.CreateTime).UTC()
		}

		if err := h.dbUsers.CreateUserWithKey(ctx, params.UserID, apiKey[:3], sha256.Sum256([]byte(apiKey)), createdAt); err != nil {
			return users.NewCreateUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("creating user: %w", err)))
		}

		return users.NewCreateUserCreated().WithPayload(&models.UserAPIKey{Apikey: &apiKey})
	}

	// Skip the RAFT round-trip when the namespace is locally known missing or
	// deleting; the apply path re-validates authoritatively.
	if ns != "" {
		if !h.namespaces.Exists(ns) {
			return users.NewCreateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("namespace %q does not exist", ns)))
		}
		if !h.namespaces.IsActive(ns) {
			return users.NewCreateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("namespace %q is being deleted", ns)))
		}
	}

	if h.staticUserExists(internalKey) {
		return users.NewCreateUserConflict().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("user '%v' already exists", params.UserID)))
	}
	if h.isRootUser(internalKey) {
		return users.NewCreateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("cannot create db user with root user name")))
	}
	if h.isAdminlistUser(internalKey) {
		return users.NewCreateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("cannot create db user with admin list name")))
	}

	existingUser, err := h.dbUsers.GetUsers(internalKey)
	if err != nil {
		return users.NewCreateUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("checking user existence: %w", err)))
	}

	if len(existingUser) > 0 {
		return users.NewCreateUserConflict().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("user '%v' already exists", params.UserID)))
	}

	apiKey, hash, userIdentifier, err := h.getApiKey()
	if err != nil {
		return users.NewCreateUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.dbUsers.CreateUser(ctx, internalKey, hash, userIdentifier, apiKey[:3], ns, time.Now()); err != nil {
		// Apply-time race: surface a deleted/deleting namespace as 422 so
		// clients can retry against current state.
		if errors.Is(err, namespaces.ErrNamespaceGone) || errors.Is(err, namespaces.ErrNamespaceDeleting) {
			return users.NewCreateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("creating user: %w", err)))
		}
		return users.NewCreateUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("creating user: %w", err)))
	}

	return users.NewCreateUserCreated().WithPayload(&models.UserAPIKey{Apikey: &apiKey})
}

func (h *dynUserHandler) rotateKey(params users.RotateUserAPIKeyParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	internalKey := namespacing.QualifyUserIDForLookup(principal, h.namespacesEnabled, params.UserID)

	if err := h.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.Users(internalKey)...); err != nil {
		return users.NewRotateUserAPIKeyForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if !h.dbUserEnabled {
		return users.NewRotateUserAPIKeyUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("db user management is not enabled")))
	}

	existingUser, err := h.dbUsers.GetUsers(internalKey)
	if err != nil {
		return users.NewRotateUserAPIKeyInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("checking user existence: %w", err)))
	}

	if len(existingUser) == 0 {
		if h.staticUserExists(internalKey) {
			return users.NewRotateUserAPIKeyUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("user '%v' is static user", params.UserID)))
		}
		return users.NewRotateUserAPIKeyNotFound()
	}

	oldUserIdentifier := existingUser[internalKey].InternalIdentifier

	apiKey, hash, newUserIdentifier, err := h.getApiKey()
	if err != nil {
		return users.NewRotateUserAPIKeyInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.dbUsers.RotateKey(ctx, internalKey, apiKey[:3], hash, oldUserIdentifier, newUserIdentifier); err != nil {
		return users.NewRotateUserAPIKeyInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("rotate key: %w", err)))
	}

	return users.NewRotateUserAPIKeyOK().WithPayload(&models.UserAPIKey{Apikey: &apiKey})
}

func (h *dynUserHandler) getApiKey() (string, string, string, error) {
	// the user identifier is random, and we need to be sure that there is no reuse. Otherwise, an existing apikey would
	// become invalid. The chances are minimal, but with a lot of users it can happen (birthday paradox!).
	// If we happen to have a collision by chance, simply generate a new key
	count := 0
	for {
		apiKey, hash, userIdentifier, err := keys.CreateApiKeyAndHash()
		if err != nil {
			return "", "", "", err
		}

		exists, err := h.dbUsers.CheckUserIdentifierExists(userIdentifier)
		if err != nil {
			return "", "", "", err
		}
		if !exists {
			return apiKey, hash, userIdentifier, nil
		}

		// make sure we don't deadlock. The chance for one collision is very small, so this should never happen. But better be safe than sorry.
		if count >= 10 {
			return "", "", "", errors.New("could not create a new user identifier")
		}
		count++
	}
}

func (h *dynUserHandler) deleteUser(params users.DeleteUserParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	internalKey := namespacing.QualifyUserIDForLookup(principal, h.namespacesEnabled, params.UserID)

	if err := h.authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.Users(internalKey)...); err != nil {
		return users.NewDeleteUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if !h.dbUserEnabled {
		return users.NewDeleteUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("db user management is not enabled")))
	}

	if internalKey == principal.Username {
		return users.NewDeleteUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("cannot delete its own user %q", params.UserID)))
	}

	if h.isRootUser(internalKey) {
		return users.NewDeleteUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("cannot delete root user")))
	}
	existingUsers, err := h.dbUsers.GetUsers(internalKey)
	if err != nil {
		return users.NewDeleteUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	if len(existingUsers) == 0 {
		if h.staticUserExists(internalKey) {
			return users.NewDeleteUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("user '%v' is static user", params.UserID)))
		}
		return users.NewDeleteUserNotFound()
	}
	roles, err := h.dbUsers.GetRolesForUserOrGroup(internalKey, authentication.AuthTypeDb, false)
	if err != nil {
		return users.NewDeleteUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	if len(roles) > 0 {
		roleNames := make([]string, 0, len(roles))
		for name := range roles {
			roleNames = append(roleNames, name)
		}
		if err := h.dbUsers.RevokeRolesForUser(conv.UserNameWithTypeFromId(internalKey, authentication.AuthTypeDb), roleNames...); err != nil {
			return users.NewDeleteUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
	}

	if err := h.dbUsers.DeleteUser(ctx, internalKey); err != nil {
		return users.NewDeleteUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	return users.NewDeleteUserNoContent()
}

func (h *dynUserHandler) deactivateUser(params users.DeactivateUserParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	internalKey := namespacing.QualifyUserIDForLookup(principal, h.namespacesEnabled, params.UserID)

	if err := h.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.Users(internalKey)...); err != nil {
		return users.NewDeactivateUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if !h.dbUserEnabled {
		return users.NewDeactivateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("db user management is not enabled")))
	}

	if internalKey == principal.Username {
		return users.NewDeactivateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("cannot deactivate its own user %q", params.UserID)))
	}

	if h.isRootUser(internalKey) {
		return users.NewDeactivateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("cannot deactivate root user")))
	}

	existingUser, err := h.dbUsers.GetUsers(internalKey)
	if err != nil {
		return users.NewDeactivateUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("checking user existence: %w", err)))
	}

	if len(existingUser) == 0 {
		if h.staticUserExists(internalKey) {
			return users.NewDeactivateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("user '%v' is static user", params.UserID)))
		}
		return users.NewDeactivateUserNotFound()
	}

	if !existingUser[internalKey].Active {
		return users.NewDeactivateUserConflict()
	}

	revokeKey := false
	if params.Body.RevokeKey != nil {
		revokeKey = *params.Body.RevokeKey
	}

	if err := h.dbUsers.DeactivateUser(ctx, internalKey, revokeKey); err != nil {
		return users.NewDeactivateUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("deactivate user: %w", err)))
	}

	return users.NewDeactivateUserOK()
}

func (h *dynUserHandler) activateUser(params users.ActivateUserParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	internalKey := namespacing.QualifyUserIDForLookup(principal, h.namespacesEnabled, params.UserID)

	if err := h.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.Users(internalKey)...); err != nil {
		return users.NewActivateUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if !h.dbUserEnabled {
		return users.NewActivateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("db user management is not enabled")))
	}

	if h.isRootUser(internalKey) {
		return users.NewActivateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("cannot activate root user")))
	}

	existingUser, err := h.dbUsers.GetUsers(internalKey)
	if err != nil {
		return users.NewActivateUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("checking user existence: %w", err)))
	}

	if len(existingUser) == 0 {
		if h.staticUserExists(internalKey) {
			return users.NewActivateUserUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("user '%v' is static user", params.UserID)))
		}
		return users.NewActivateUserNotFound()
	}

	if existingUser[internalKey].Active {
		return users.NewActivateUserConflict()
	}

	if err := h.dbUsers.ActivateUser(ctx, internalKey); err != nil {
		return users.NewActivateUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("activate user: %w", err)))
	}

	return users.NewActivateUserOK()
}

func (h *dynUserHandler) staticUserExists(newUser string) bool {
	if h.staticApiKeysConfigs.Enabled {
		for _, staticUser := range h.staticApiKeysConfigs.Users {
			if staticUser == newUser {
				return true
			}
		}
	}
	return false
}

func (h *dynUserHandler) principalIsRootUser(name string) bool {
	if !h.rbacConfig.Enabled && !h.adminListConfig.Enabled {
		return true
	}
	for i := range h.rbacConfig.RootUsers {
		if h.rbacConfig.RootUsers[i] == name {
			return true
		}
	}
	return false
}

func (h *dynUserHandler) isRootUser(name string) bool {
	for i := range h.rbacConfig.RootUsers {
		if h.rbacConfig.RootUsers[i] == name {
			return true
		}
	}
	return false
}

func (h *dynUserHandler) isAdminlistUser(name string) bool {
	for i := range h.adminListConfig.Users {
		if h.adminListConfig.Users[i] == name {
			return true
		}
	}
	for i := range h.adminListConfig.ReadOnlyUsers {
		if h.adminListConfig.ReadOnlyUsers[i] == name {
			return true
		}
	}
	return false
}

func (h *dynUserHandler) isRequestFromRootUser(principal *models.Principal) bool {
	if principal == nil {
		return false
	}
	return h.rbacConfig.IsRootUser(principal.Username, principal.Groups)
}

// validateRoleName validates that this string is a valid role name (format wise)
func validateUserName(name string) error {
	if len(name) > apikey.UserNameMaxLength {
		return fmt.Errorf("'%s' is not a valid user name. Name should not be longer than %d characters", name, apikey.UserNameMaxLength)
	}
	if !validateUserNameRegex.MatchString(name) {
		return fmt.Errorf("'%s' is not a valid user name", name)
	}
	return nil
}
