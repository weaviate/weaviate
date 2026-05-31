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

package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/metrics"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

type Auth struct {
	allowAnonymousAccess bool
	authComposer         composer.TokenFunc
	authorizer           authorization.Authorizer
	metrics              *metrics.MCPMetrics
}

func NewAuth(allowAnonymousAccess bool, authComposer composer.TokenFunc, authorizer authorization.Authorizer, m *metrics.MCPMetrics) *Auth {
	return &Auth{
		allowAnonymousAccess: allowAnonymousAccess,
		authComposer:         authComposer,
		authorizer:           authorizer,
		metrics:              m,
	}
}

func (a *Auth) Authorize(ctx context.Context, req mcp.CallToolRequest, verb string) (*models.Principal, error) {
	principal, err := a.principalFromRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get principal: %w", err)
	}
	if err := a.authorizer.Authorize(ctx, principal, verb, authorization.Mcp()); err != nil {
		a.observeAuthzFailure(err)
		return nil, err
	}
	return principal, nil
}

// AuthorizeCollectionData checks that the principal has the given verb permission
// on the specified collection and tenant data, matching the authorization pattern
// used by the gRPC endpoint (service.go classGetterWithAuthzFunc): use
// CollectionsData when no tenant is specified, ShardsData when one is.
func (a *Auth) AuthorizeCollectionData(ctx context.Context, principal *models.Principal, verb, collection, tenant string) error {
	resources := authorization.CollectionsData(collection)
	if tenant != "" {
		resources = authorization.ShardsData(collection, tenant)
	}
	if err := a.authorizer.Authorize(ctx, principal, verb, resources...); err != nil {
		a.observeAuthzFailure(err)
		return err
	}
	return nil
}

func (a *Auth) observeAuthzFailure(err error) {
	var forbidden authzerrors.Forbidden
	if errors.As(err, &forbidden) {
		a.metrics.ObserveAuthFailure(metrics.AuthReasonForbidden)
		return
	}
	var unauth authzerrors.Unauthenticated
	if errors.As(err, &unauth) {
		a.metrics.ObserveAuthFailure(metrics.AuthReasonUnauthenticated)
		return
	}
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
		a.metrics.ObserveAuthFailure(metrics.AuthReasonInvalidToken)
		return nil, fmt.Errorf("invalid authorization header: expected 'Bearer <token>' format")
	}

	token := strings.TrimPrefix(authValue[0], "Bearer ")
	principal, err := a.authComposer(token, nil)
	if err != nil {
		a.metrics.ObserveAuthFailure(metrics.AuthReasonInvalidToken)
		return nil, err
	}
	return principal, nil
}

func (a *Auth) tryAnonymous() (*models.Principal, error) {
	if a.allowAnonymousAccess {
		return nil, nil
	}

	principal, err := a.authComposer("", nil)
	if err != nil {
		a.metrics.ObserveAuthFailure(metrics.AuthReasonMissingToken)
		return nil, err
	}
	return principal, nil
}
