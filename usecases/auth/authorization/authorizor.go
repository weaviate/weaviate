package authorization

import (
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/adminlist"
	"github.com/semi-technologies/weaviate/usecases/config"
)

// Authorizer always makes a yes/no decision on a specific resource. Which
// authorization technique is used in the background (e.g. RBAC, adminlist,
// ...) is hidden through this interface
type Authorizer interface {
}

// New Authorizer based on the application-wide config
func New(cfg config.Config) Authorizer {

	if cfg.Authorization.AdminList.Enabled {
		return adminlist.New(cfg.Authorization.AdminList)
	}

	return &DummyAuthorizer{}
}

// DummyAuthorizer is a pluggable Authorizor which can be used if no specific
// authorizor is configured. It will allow every authz decision, i.e. it is
// effectively the same as "no authorization at all"
type DummyAuthorizer struct{}
