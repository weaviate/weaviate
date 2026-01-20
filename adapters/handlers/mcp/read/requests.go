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

type FetchLogsArgs struct {
	Limit  int `json:"limit,omitempty" jsonschema_description:"Maximum number of characters to return (default: 2000, max: 50000)"`
	Offset int `json:"offset,omitempty" jsonschema_description:"Number of characters to skip from the end before returning logs (default: 0 for most recent logs)"`
}

type GetObjectsArgs struct {
	CollectionName   string   `json:"collection_name" jsonschema:"required" jsonschema_description:"Name of the collection to retrieve objects from"`
	TenantName       string   `json:"tenant_name,omitempty" jsonschema_description:"Name of the tenant (for multi-tenant collections)"`
	UUIDs            []string `json:"uuids,omitempty" jsonschema_description:"Array of object UUIDs to retrieve. If provided, fetches only these specific objects. If not provided, fetches a paginated list of objects from the collection."`
	Offset           *int     `json:"offset,omitempty" jsonschema_description:"Number of objects to skip (for pagination). Only used when UUIDs are not specified."`
	Limit            *int     `json:"limit,omitempty" jsonschema_description:"Maximum number of objects to return (for pagination, default: 25). Only used when UUIDs are not specified."`
	IncludeVector    bool     `json:"include_vector,omitempty" jsonschema_description:"Whether to include the default vector in the response (default: false)"`
	ReturnProperties []string `json:"return_properties,omitempty" jsonschema_description:"Specific properties to return. If not specified, all properties are returned."`
	ReturnMetadata   []string `json:"return_metadata,omitempty" jsonschema_description:"Metadata fields to include in the response. Supported values: 'id', 'vector', 'creationTimeUnix', 'lastUpdateTimeUnix'. By default, only 'id' is included."`
}
