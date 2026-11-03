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

package handlers

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/go-openapi/runtime/middleware"

	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
	"github.com/weaviate/weaviate/wl/dbusers"
)

// ExpirationRaft is the part of cluster.Raft that setUserExpiration calls, so
// SetUserExpiration is the only RAFT command setUserExpiration can submit.
type ExpirationRaft interface {
	GetUsers(userIds ...string) (map[string]apikey.UserView, error)
	SetUserExpiration(ctx context.Context, userId string, expiresAt time.Time) error
}

type expirationHandler struct {
	raft              ExpirationRaft
	authorizer        authorization.Authorizer
	expiry            *dbusers.ValidatingExpiry
	rbac              rbacconf.Config
	static            config.StaticAPIKey
	namespacesEnabled bool
}

// SetupHandlers registers PUT /users/db/{user_id}/expiration on api.
// setUserExpiration checks neither AUTHENTICATION_DB_USERS_ENABLED nor the
// license key, so call SetupHandlers only in license.FeatureLicensed.
func SetupHandlers(
	api *operations.WeaviateAPI,
	raft ExpirationRaft,
	authorizer authorization.Authorizer,
	expiry *dbusers.ValidatingExpiry,
	rbac rbacconf.Config,
	static config.StaticAPIKey,
	namespacesEnabled bool,
) {
	h := &expirationHandler{
		raft:              raft,
		authorizer:        authorizer,
		expiry:            expiry,
		rbac:              rbac,
		static:            static,
		namespacesEnabled: namespacesEnabled,
	}
	api.UsersSetUserExpirationHandler = users.SetUserExpirationHandlerFunc(h.setUserExpiration)
}

func (h *expirationHandler) setUserExpiration(params users.SetUserExpirationParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	internalKey := namespacing.QualifyUserIDForLookup(principal, h.namespacesEnabled, params.UserID)

	if err := h.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.Users(internalKey)...); err != nil {
		return users.NewSetUserExpirationForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if apikey.IsOwnUser(principal, internalKey) {
		return unprocessable(principal, fmt.Errorf("cannot set the expiration of its own user %q", params.UserID))
	}

	if h.rbac.IsRootUser(internalKey, nil) {
		return unprocessable(principal, errors.New("cannot set the expiration of a root user"))
	}

	existingUser, err := h.raft.GetUsers(internalKey)
	if err != nil {
		return users.NewSetUserExpirationInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("checking user existence: %w", err)))
	}

	if len(existingUser) == 0 {
		if h.static.Enabled && slices.Contains(h.static.Users, internalKey) {
			return unprocessable(principal, fmt.Errorf("user '%v' is static user", params.UserID))
		}
		return users.NewSetUserExpirationNotFound()
	}

	requested, err := requestedExpiresAt(params.Body)
	if err != nil {
		return unprocessable(principal, err)
	}

	expiresAt, err := h.expiry.Resolve(requested)
	if err != nil {
		return unprocessable(principal, err)
	}

	if err := h.raft.SetUserExpiration(ctx, internalKey, expiresAt); err != nil {
		return users.NewSetUserExpirationInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("set user expiration: %w", err)))
	}

	return users.NewSetUserExpirationOK()
}

// requestedExpiresAt returns the expiresAt body sets, or nil where it sets
// neverExpires to true, which clears the expiry.
func requestedExpiresAt(body users.SetUserExpirationBody) (*time.Time, error) {
	if body.ExpiresAt != nil && body.NeverExpires != nil {
		return nil, errors.New("body must set only one of expiresAt and neverExpires")
	}
	if body.ExpiresAt != nil {
		return (*time.Time)(body.ExpiresAt), nil
	}
	if body.NeverExpires == nil || !*body.NeverExpires {
		return nil, errors.New("body must set expiresAt, or neverExpires to true to clear the expiration")
	}
	return nil, nil
}

func unprocessable(principal *models.Principal, err error) middleware.Responder {
	return users.NewSetUserExpirationUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
}
