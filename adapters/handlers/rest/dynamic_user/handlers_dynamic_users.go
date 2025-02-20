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

	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/dynamic"

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
	dynamicUser apikey.DynamicUser
}

const (
	userNameMaxLength = 64
	userNameRegexCore = `[A-Za-z][-_0-9A-Za-z]{0,254}`
)

var validateUserNameRegex = regexp.MustCompile(`^` + userNameRegexCore + `$`)

func SetupHandlers(api *operations.WeaviateAPI, dynamicUser apikey.DynamicUser, authorizer authorization.Authorizer, logger logrus.FieldLogger,
) {
	h := &dynUserHandler{
		authorizer:  authorizer,
		dynamicUser: dynamicUser,
	}

	api.UsersCreateUserHandler = users.CreateUserHandlerFunc(h.createUser)
}

func (h *dynUserHandler) createUser(params users.CreateUserParams, principal *models.Principal) middleware.Responder {
	if err := validateUserName(params.UserID); err != nil {
		return users.NewCreateUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(errors.New("role name is invalid")))
	}

	apiKey, hash, userIdentifier, err := dynamic.CreateApiKeyAndHash()
	if err != nil {
		return users.NewCreateUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := h.dynamicUser.CreateUser(params.UserID, hash, userIdentifier); err != nil {
		return nil
	}

	return users.NewCreateUserCreated().WithPayload(&models.UserAPIKey{Apikey: &apiKey})
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
