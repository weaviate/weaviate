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

package config

import (
	"fmt"

	"github.com/semi-technologies/weaviate/usecases/auth/authorization/adminlist"
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
