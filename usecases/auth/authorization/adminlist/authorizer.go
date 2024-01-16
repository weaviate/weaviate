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

package adminlist

import (
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

const AnonymousPrincipalUsername = "anonymous"

// Authorizer provides either full (admin) or no access
type Authorizer struct {
	adminUsers     map[string]int
	readOnlyUsers  map[string]int
	adminGroups    map[string]int
	readOnlyGroups map[string]int
}

// New Authorizer using the AdminList method
func New(cfg Config) *Authorizer {
	a := &Authorizer{}
	a.addAdminUserList(cfg.Users)
	a.addReadOnlyUserList(cfg.ReadOnlyUsers)
	a.addAdminGroupList(cfg.Groups)
	a.addReadOnlyGroupList(cfg.ReadOnlyGroups)
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

	for _, group := range principal.Groups {
		if _, ok := a.adminGroups[group]; ok {
			return nil
		}
	}

	if verb == "get" || verb == "list" {
		if _, ok := a.readOnlyUsers[principal.Username]; ok {
			return nil
		}
		for _, group := range principal.Groups {
			if _, ok := a.readOnlyGroups[group]; ok {
				return nil
			}
		}
	}

	return errors.NewForbidden(principal, verb, resource)
}

func (a *Authorizer) addAdminUserList(users []string) {
	// build a map for more efficient lookup on long lists
	if a.adminUsers == nil {
		a.adminUsers = map[string]int{}
	}

	for _, user := range users {
		a.adminUsers[user] = 1
	}
}

func (a *Authorizer) addReadOnlyUserList(users []string) {
	// build a map for more efficient lookup on long lists
	if a.readOnlyUsers == nil {
		a.readOnlyUsers = map[string]int{}
	}

	for _, user := range users {
		a.readOnlyUsers[user] = 1
	}
}

func (a *Authorizer) addAdminGroupList(groups []string) {
	// build a map for more efficient lookup on long lists
	if a.adminGroups == nil {
		a.adminGroups = map[string]int{}
	}

	for _, group := range groups {
		a.adminGroups[group] = 1
	}
}

func (a *Authorizer) addReadOnlyGroupList(groups []string) {
	// build a map for more efficient lookup on long lists
	if a.readOnlyGroups == nil {
		a.readOnlyGroups = map[string]int{}
	}

	for _, group := range groups {
		a.readOnlyGroups[group] = 1
	}
}

func newAnonymousPrincipal() *models.Principal {
	return &models.Principal{
		Username: AnonymousPrincipalUsername,
	}
}
