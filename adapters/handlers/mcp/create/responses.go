//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package create

type UpsertObjectResult struct {
	ID    string `json:"id,omitempty" jsonschema_description:"UUID of the upserted object (only present if successful)"`
	Error string `json:"error,omitempty" jsonschema_description:"Error message if the upsert failed for this object"`
}

type UpsertObjectResp struct {
	Results []UpsertObjectResult `json:"results" jsonschema_description:"Results for each object in the batch, in the same order as the input"`
}
