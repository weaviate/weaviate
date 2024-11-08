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

package rbac

import (
	"github.com/casbin/casbin/v2"
)

type Enforcer struct {
	casbin *casbin.SyncedCachedEnforcer
}

func NewEnforcer(casbin *casbin.SyncedCachedEnforcer) *Enforcer {
	return &Enforcer{casbin: casbin}
}

func (e *Enforcer) enforce(username, resource, verb string) (bool, error) {
	return e.casbin.Enforce(username, resource, verb)
}
