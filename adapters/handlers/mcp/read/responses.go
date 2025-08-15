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

package read

import "github.com/weaviate/weaviate/entities/models"

type GetTenantsResp struct {
	Tenants []*models.Tenant `json:"tenants" jsonschema_description:"The returned tenants"`
}

type GetSchemaResp struct {
	Schema *models.Schema `json:"schema" jsonschema_description:"The returned schema"`
}
