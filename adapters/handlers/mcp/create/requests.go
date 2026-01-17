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

package create

type UpsertObjectArgs struct {
	CollectionName string         `json:"collection_name" jsonschema:"required" jsonschema_description:"Name of collection to upsert object into"`
	Tenant         string         `json:"tenant,omitempty" jsonschema_description:"Name of the tenant the object belongs to"`
	Properties     map[string]any `json:"properties,omitempty" jsonschema_description:"Properties of the object to upsert"`
}
