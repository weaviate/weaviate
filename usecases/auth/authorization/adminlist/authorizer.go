//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package adminlist

import (
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
)

const AnonymousPrinicpalUsername = "anonymous"

// Authorizer provides either full (admin) or no access
type Authorizer struct {
	adminUsers    map[string]int
	readOnlyUsers map[string]int
}

// New Authorizer using the AdminList method
func New(cfg Config) *Authorizer {
	a := &Authorizer{}
	a.addAdminUserList(cfg.Users)
	a.addReadOnlyUserList(cfg.ReadOnlyUsers)
	return a
}

// Authorize will give full access (to any resource!) if the user is part of
// the admin list or no access at all if they are not
func (a *Authorizer) Authorize(principal *models.Principal, verb, resource string) error {
	if principal == nil {
		principal = newAnonymousPrincipal()
	}

	if _, ok := a.adminUsers[principal.Username]; ok {
		return nil
	}

	if verb == "get" || verb == "list" {
		if _, ok := a.readOnlyUsers[principal.Username]; ok {
			return nil
		}
	}

	return errors.NewForbidden(principal, verb, resource)
}

func (a *Authorizer) addAdminUserList(users []string) {
	// build a map for more effecient lookup on long lists
	if a.adminUsers == nil {
		a.adminUsers = map[string]int{}
	}

	for _, user := range users {
		a.adminUsers[user] = 1
	}
}

func (a *Authorizer) addReadOnlyUserList(users []string) {
	// build a map for more effecient lookup on long lists
	if a.readOnlyUsers == nil {
		a.readOnlyUsers = map[string]int{}
	}

	for _, user := range users {
		a.readOnlyUsers[user] = 1
	}
}

func newAnonymousPrincipal() *models.Principal {
	return &models.Principal{
		Username: AnonymousPrinicpalUsername,
	}
}
