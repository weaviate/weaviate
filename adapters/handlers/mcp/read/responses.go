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

package read

import "github.com/weaviate/weaviate/entities/models"

type GetCollectionConfigResp struct {
	Collections []*models.Class `json:"collections" jsonschema_description:"The returned collection configurations"`
}

type GetTenantsResp struct {
	Tenants []*models.Tenant `json:"tenants" jsonschema_description:"The returned tenants"`
}

type FetchLogsResp struct {
	Logs string `json:"logs" jsonschema_description:"The fetched log content"`
}

type GetObjectsResp struct {
	Objects []*models.Object `json:"objects" jsonschema_description:"Array of retrieved objects"`
}
