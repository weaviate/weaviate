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

package composer

import (
	"errors"

	openapi "github.com/go-openapi/errors"
	"github.com/golang-jwt/jwt/v4"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

type TokenFunc func(token string, scopes []string) (*models.Principal, error)

// New provides an OpenAPI compatible token validation
// function that validates the token either as OIDC or as an StaticAPIKey token
// depending on which is configured. If both are configured, the scheme is
// figured out at runtime.
//
// On namespace-enabled clusters every authenticated principal must be either
// namespace-confined or a global operator; the returned function rejects any
// principal that is neither (see assertClassified).
func New(config config.Authentication, namespacesEnabled bool, logger logrus.FieldLogger,
	apikey authValidator, oidc authValidator,
) TokenFunc {
	inner := selectScheme(config, apikey, oidc)
	return func(token string, scopes []string) (*models.Principal, error) {
		principal, err := inner(token, scopes)
		if err != nil {
			return nil, err
		}
		if err := assertClassified(principal, namespacesEnabled, logger); err != nil {
			return nil, err
		}
		return principal, nil
	}
}

func selectScheme(config config.Authentication,
	apikey authValidator, oidc authValidator,
) TokenFunc {
	if config.AnyApiKeyAvailable() && config.OIDC.Enabled {
		return pickAuthSchemeDynamically(apikey, oidc)
	}

	if config.AnyApiKeyAvailable() {
		return apikey.ValidateAndExtract
	}

	if config.OIDC.Enabled {
		return oidc.ValidateAndExtract
	}

	return func(token string, scopes []string) (*models.Principal, error) {
		return nil, openapi.New(401, "no authentication scheme is configured (API Key or OIDC), "+
			"but an 'Authorization' header was provided. Please configure an auth scheme "+
			"or remove the header for anonymous access")
	}
}

// assertClassified rejects an authenticated principal that carries no namespace
// and no global-operator flag on a namespace-enabled cluster. Such a principal
// would otherwise be treated as a least-privilege caller with cluster-wide
// reach; fail immediately instead.
func assertClassified(principal *models.Principal, namespacesEnabled bool, logger logrus.FieldLogger) error {
	if !namespacesEnabled || principal == nil {
		return nil
	}
	if principal.Namespace == "" && !principal.IsGlobalOperator {
		logger.WithField("user", principal.Username).
			Warn("rejecting authenticated principal with neither a namespace nor global-operator classification on namespace-enabled cluster")
		return openapi.New(401, "unauthorized: principal is neither namespace-confined nor a global operator")
	}
	return nil
}

func pickAuthSchemeDynamically(
	apiKey authValidator, oidc authValidator,
) TokenFunc {
	return func(token string, scopes []string) (*models.Principal, error) {
		_, err := jwt.Parse(token, func(t *jwt.Token) (interface{}, error) {
			return nil, nil
		})

		if err != nil && errors.Is(err, jwt.ErrTokenMalformed) {
			return apiKey.ValidateAndExtract(token, scopes)
		}

		return oidc.ValidateAndExtract(token, scopes)
	}
}

type authValidator interface {
	ValidateAndExtract(token string, scopes []string) (*models.Principal, error)
}
