package auth

import (
	"context"
	"strings"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"google.golang.org/grpc/metadata"
)

type Auth struct {
	allowAnonymousAccess bool
	authComposer         composer.TokenFunc
}

func NewAuth(state *state.State) *Auth {
	return &Auth{
		allowAnonymousAccess: state.ServerConfig.Config.Authentication.AnonymousAccess.Enabled,
		authComposer: composer.New(
			state.ServerConfig.Config.Authentication,
			state.APIKey,
			state.OIDC,
		),
	}
}

// This should probably be run as part of a middleware. In the initial gRPC
// implementation there is only a single endpoint, so it's fine to run this
// straight from the endpoint. But the moment we add a second endpoint, this
// should be called from a central place. This way we can make sure it's
// impossible to forget to add it to a new endpoint.
func (a *Auth) PrincipalFromContext(ctx context.Context) (*models.Principal, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return a.tryAnonymous()
	}

	// the grpc library will lowercase all md keys, so we need to make sure to
	// check a lowercase key
	authValue, ok := md["authorization"]
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
