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

package config

import (
	"fmt"

	"github.com/weaviate/weaviate/usecases/auth/authorization/adminlist"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

// Authorization configuration
type Authorization struct {
	AdminList adminlist.Config `json:"admin_list" yaml:"admin_list"`
	Rbac      rbacconf.Config  `json:"rbac" yaml:"rbac"`
}

// Validate the Authorization configuration. This only validates at a general
// level. Validation specific to the individual auth methods should happen
// inside their respective packages
func (a Authorization) Validate() error {
	if a.AdminList.Enabled && a.Rbac.Enabled {
		return fmt.Errorf("cannot enable adminlist and rbac at the same time")
	}

	if a.AdminList.Enabled {
		if err := a.AdminList.Validate(); err != nil {
			return fmt.Errorf("authorization adminlist: %w", err)
		}
	}

	if a.Rbac.Enabled {
		if err := a.Rbac.Validate(); err != nil {
			return fmt.Errorf("authorization rbac: %w", err)
		}
	}

	return nil
}
