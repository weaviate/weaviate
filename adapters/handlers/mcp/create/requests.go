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
	CollectionName string                    `json:"collection_name" jsonschema:"required" jsonschema_description:"Name of collection to upsert object into"`
	TenantName     string                    `json:"tenant_name,omitempty" jsonschema_description:"Name of the tenant the object belongs to"`
	UUID           string                    `json:"uuid,omitempty" jsonschema_description:"UUID of the object. If not provided, a new UUID will be generated. If provided and exists, the object will be updated; otherwise, a new object with this UUID will be created"`
	Properties     map[string]any            `json:"properties" jsonschema:"required" jsonschema_description:"Properties of the object to upsert"`
	Vectors        map[string][]float32      `json:"vectors,omitempty" jsonschema_description:"Named vectors for the object (e.g., {'default': [0.1, 0.2, ...], 'image': [0.3, 0.4, ...]})"`
}
