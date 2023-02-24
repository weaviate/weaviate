package rest

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

type openAPITokenFunc func(token string, scopes []string) (*models.Principal, error)

func NewOpenAPITokenValidator(config config.Authentication,
	apikey apiKeyValidator, oidc oidcValidator,
) openAPITokenFunc {
	if config.APIKey.Enabled && config.OIDC.Enabled {
		return func(token string, scopes []string) (*models.Principal, error) {
			return nil, fmt.Errorf("not supported yet")
		}
	}

	if config.APIKey.Enabled {
		return apikey.ValidateAndExtract
	}

	// default to OIDC, even if no scheme is enabled, then it can deal with this
	// scenario itself.
	return oidc.ValidateAndExtract
}

type oidcValidator interface {
	ValidateAndExtract(token string, scopes []string) (*models.Principal, error)
}

type apiKeyValidator interface {
	ValidateAndExtract(token string, scopes []string) (*models.Principal, error)
}
