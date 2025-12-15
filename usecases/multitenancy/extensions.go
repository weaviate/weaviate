//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package multitenancy

import "github.com/weaviate/weaviate/entities/models"

// IsMultiTenant returns true if a collection is multi-tenant, false otherwise
func IsMultiTenant(config *models.MultiTenancyConfig) bool {
	return config != nil && config.Enabled
}
