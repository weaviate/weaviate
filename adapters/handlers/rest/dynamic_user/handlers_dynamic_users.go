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

package dynamic_user

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"

	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

type dynUserHandler struct {
	authorizer  authorization.Authorizer
	dynamicUser DynamicUserAndRolesGetter
}

type DynamicUserAndRolesGetter interface {
	apikey.DynamicUser
	GetRolesForUser(user string) (map[string][]authorization.Policy, error)
}

const (
	userNameMaxLength = 64
	userNameRegexCore = `[A-Za-z][-_0-9A-Za-z]{0,254}`
)

var validateUserNameRegex = regexp.MustCompile(`^` + userNameRegexCore + `$`)

func SetupHandlers(api *operations.WeaviateAPI, dynamicUser DynamicUserAndRolesGetter, authorizer authorization.Authorizer, logger logrus.FieldLogger,
) {
	h := &dynUserHandler{
		authorizer:  authorizer,
		dynamicUser: dynamicUser,
	}

	api.UsersCreateUserHandler = users.CreateUserHandlerFunc(h.createUser)
	api.UsersDeleteUserHandler = users.DeleteUserHandlerFunc(h.deleteUser)
	api.UsersGetUserInfoHandler = users.GetUserInfoHandlerFunc(h.getUser)
	api.UsersRotateUserAPIKeyHandler = users.RotateUserAPIKeyHandlerFunc(h.rotateKey)
}

func (h *dynUserHandler) getUser(params users.GetUserInfoParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Users(params.UserID)...); err != nil {
		return users.NewGetUserInfoForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	existingUser, err := h.dynamicUser.GetUsers(params.UserID)
	if err != nil {
		return users.NewGetUserInfoInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("checking user existence: %w", err)))
	}

	if len(existingUser) == 0 {
		return users.NewGetUserInfoNotFound()
	}

	existedRoles, err := h.dynamicUser.GetRolesForUser(params.UserID)
	if err != nil {
		return users.NewGetUserInfoInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("get roles: %w", err)))
	}

	roles := make([]string, 0, len(existedRoles))
	for roleName := range existedRoles {
		roles = append(roles, roleName)
	}

	return users.NewGetUserInfoOK().WithPayload(&models.UserInfo{UserID: &params.UserID, Roles: roles})
}

func (h *dynUserHandler) createUser(params users.CreateUserParams, principal *models.Principal) middleware.Responder {
	if err := validateUserName(params.UserID); err != nil {
		return users.NewCreateUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := h.authorizer.Authorize(principal, authorization.CREATE, authorization.Users(params.UserID)...); err != nil {
		return users.NewCreateUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	existingUser, err := h.dynamicUser.GetUsers(params.UserID)
	if err != nil {
		return users.NewCreateUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("checking user existence: %w", err)))
	}

	if len(existingUser) > 0 {
		return users.NewCreateUserConflict().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("user %v already exists", params.UserID)))
	}

	var apiKey, hash, userIdentifier string

	// the user identifier is random, and we need to be sure that there is no reuse. Otherwise, an existing apikey would
	// become invalid. The chances are minimal, but with a lot of users it can happen (birthday paradox!).
	// If we happen to have a collision by chance, simply generate a new key
	count := 0
	for {
		apiKey, hash, userIdentifier, err = keys.CreateApiKeyAndHash("")
		if err != nil {
			return users.NewCreateUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		}

		exists, err := h.dynamicUser.CheckUserIdentifierExists(userIdentifier)
		if err != nil {
			return users.NewCreateUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		}
		if !exists {
			break
		}

		// make sure we don't deadlock. The chance for one collision is very small, so this should never happen. But better be safe than sorry.
		if count >= 10 {
			return users.NewCreateUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(errors.New("could not create a new user identifier")))
		}
		count++
	}

	if err := h.dynamicUser.CreateUser(params.UserID, hash, userIdentifier); err != nil {
		return users.NewCreateUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("creating user: %w", err)))
	}

	return users.NewCreateUserCreated().WithPayload(&models.UserAPIKey{Apikey: &apiKey})
}

func (h *dynUserHandler) rotateKey(params users.RotateUserAPIKeyParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.Users(params.UserID)...); err != nil {
		return users.NewRotateUserAPIKeyForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	existingUser, err := h.dynamicUser.GetUsers(params.UserID)
	if err != nil {
		return users.NewRotateUserAPIKeyInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("checking user existence: %w", err)))
	}

	if len(existingUser) == 0 {
		return users.NewRotateUserAPIKeyNotFound()
	}

	apiKey, hash, _, err := keys.CreateApiKeyAndHash(existingUser[params.UserID].InternalIdentifier)
	if err != nil {
		return users.NewRotateUserAPIKeyInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("generating key: %w", err)))
	}

	if err := h.dynamicUser.RotateKey(params.UserID, hash); err != nil {
		return users.NewRotateUserAPIKeyInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("rotate key: %w", err)))
	}

	return users.NewRotateUserAPIKeyOK().WithPayload(&models.UserAPIKey{Apikey: &apiKey})
}

func (h *dynUserHandler) deleteUser(params users.DeleteUserParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.DELETE, authorization.Users(params.UserID)...); err != nil {
		return users.NewDeleteUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	err := h.dynamicUser.DeleteUser(params.UserID)
	if err != nil {
		return users.NewDeleteUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}
	return users.NewDeleteUserNoContent()
}

// validateRoleName validates that this string is a valid role name (format wise)
func validateUserName(name string) error {
	if len(name) > userNameMaxLength {
		return fmt.Errorf("'%s' is not a valid user name. Name should not be longer than %d characters", name, userNameMaxLength)
	}
	if !validateUserNameRegex.MatchString(name) {
		return fmt.Errorf("'%s' is not a valid user name", name)
	}
	return nil
}
