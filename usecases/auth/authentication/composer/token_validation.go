//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package composer

import (
	"github.com/golang-jwt/jwt/v4"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

type TokenFunc func(token string, scopes []string) (*models.Principal, error)

// New provides an OpenAPI compatible token validation
// function that validates the token either as OIDC or as an StaticAPIKey token
// depending on which is configured. If both are configured, the scheme is
// figured out at runtime.
func New(config config.Authentication,
	apikey authValidator, oidc authValidator,
) TokenFunc {
	if config.AnyApiKeyAvailable() && config.OIDC.Enabled {
		return pickAuthSchemeDynamically(apikey, oidc)
	}

	if config.AnyApiKeyAvailable() {
		return apikey.ValidateAndExtract
	}

	// default to OIDC, even if no scheme is enabled, then it can deal with this
	// scenario itself. This is the backward-compatible scenario.
	return oidc.ValidateAndExtract
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
