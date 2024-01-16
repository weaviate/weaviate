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
)

// Authorization configuration
type Authorization struct {
	AdminList adminlist.Config `json:"admin_list" yaml:"admin_list"`
}

// Validate the Authorization configuration. This only validates at a general
// level. Validation specific to the individual auth methods should happen
// inside their respective packages
func (a Authorization) Validate() error {
	if a.AdminList.Enabled {
		if err := a.AdminList.Validate(); err != nil {
			return fmt.Errorf("authorization: %s", err)
		}
	}

	return nil
}
