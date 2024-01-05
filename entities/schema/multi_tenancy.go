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

package schema

import "github.com/weaviate/weaviate/entities/models"

func MultiTenancyEnabled(class *models.Class) bool {
	if class.MultiTenancyConfig != nil {
		return class.MultiTenancyConfig.Enabled
	}
	return false
}

func ActivityStatus(status string) string {
	if status == "" {
		return models.TenantActivityStatusHOT
	}
	return status
}
