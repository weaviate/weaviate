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

package auth

import (
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

type Auth struct {
	allowAnonymousAccess bool
	authComposer         composer.TokenFunc
	authorizer           authorization.Authorizer
}

func NewAuth(state *state.State) *Auth {
	return &Auth{
		allowAnonymousAccess: state.ServerConfig.Config.Authentication.AnonymousAccess.Enabled,
		authComposer: composer.New(
			state.ServerConfig.Config.Authentication,
			state.APIKey,
			state.OIDC,
		),
		authorizer: state.Authorizer,
	}
}

func (a *Auth) Authorize(req mcp.CallToolRequest, verb string) (*models.Principal, error) {
	principal, err := a.principalFromRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get principal: %w", err)
	}
	if err := a.authorizer.Authorize(principal, verb, authorization.Mcp()); err != nil {
		return nil, err
	}
	return principal, nil
}

func (a *Auth) principalFromRequest(req mcp.CallToolRequest) (*models.Principal, error) {
	authValue, ok := req.Header["Authorization"]
	if !ok {
		return a.tryAnonymous()
	}

	if len(authValue) == 0 {
		return a.tryAnonymous()
	}

	if !strings.HasPrefix(authValue[0], "Bearer ") {
		return a.tryAnonymous()
	}

	token := strings.TrimPrefix(authValue[0], "Bearer ")
	return a.authComposer(token, nil)
}

func (a *Auth) tryAnonymous() (*models.Principal, error) {
	if a.allowAnonymousAccess {
		return nil, nil
	}

	return a.authComposer("", nil)
}
