/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package adminlist

import (
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
)

// Authorizer provides either full (admin) or no access
type Authorizer struct {
	allowedUsers map[string]int
}

// New Authorizer using the AdminList method
func New(cfg Config) *Authorizer {
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
