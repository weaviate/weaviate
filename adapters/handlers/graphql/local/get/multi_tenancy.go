//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package get

import (
	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/weaviate/weaviate/entities/models"
)

func multiTenancyEnabled(class *models.Class) bool {
	if class.MultiTenancyConfig != nil &&
		class.MultiTenancyConfig.Enabled &&
		class.MultiTenancyConfig.TenantKey != "" {
		return true
	}
	return false
}

func tenantKeyArgument() *graphql.ArgumentConfig {
	return &graphql.ArgumentConfig{
		Description: descriptions.TenantKey,
		Type:        graphql.String,
	}
}
