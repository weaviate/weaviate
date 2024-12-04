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

package cluster

import (
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (s *Raft) GetRoles(names ...string) (map[string][]authorization.Policy, error) {
	return s.store.authZManager.GetRoles(names...)
}

func (s *Raft) GetRolesForUser(user string) (map[string][]authorization.Policy, error) {
	return s.store.authZManager.GetRolesForUser(user)
}

func (s *Raft) GetUsersForRole(role string) ([]string, error) {
	return s.store.authZManager.GetUsersForRole(role)
}
