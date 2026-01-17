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

type GetCollectionConfigArgs struct {
	CollectionName string `json:"collection_name,omitempty" jsonschema_description:"Name of specific collection to get config for. If not provided, returns all collections"`
}

type GetTenantsArgs struct {
	CollectionName string `json:"collection_name" jsonschema:"required" jsonschema_description:"Name of collection to get tenants from"`
}
