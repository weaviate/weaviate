package adminlist

import (
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
	"github.com/semi-technologies/weaviate/usecases/config"
)

// Authorizer provides either full (admin) or no access
type Authorizer struct {
	allowedUsers map[string]int
}

// New Authorizer using the AdminList method
func New(cfg config.AdminList) *Authorizer {
	a := &Authorizer{}
	a.addUserList(cfg.Users)
	return a
}

// Authorize will give full access (to any resource!) if the user is part of
// the admin list or no access at all if they are not
func (a *Authorizer) Authorize(principal *models.Principal, verb, resource string) error {
	if _, ok := a.allowedUsers[principal.Username]; ok {
		return nil
	}

	return errors.NewForbidden(principal, verb, resource)
}

func (a *Authorizer) addUserList(users []string) {
	// build a map for more effecient lookup on long lists
	if a.allowedUsers == nil {
		a.allowedUsers = map[string]int{}
	}

	for _, user := range users {
		a.allowedUsers[user] = 1
	}
}
