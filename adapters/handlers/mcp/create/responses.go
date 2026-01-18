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

type UpsertObjectResp struct {
	ID string `json:"id" jsonschema_description:"ID of the upserted object"`
}

type CreateCollectionResp struct {
	CollectionName string `json:"collection_name" jsonschema_description:"Name of the created collection"`
}

type DeleteObjectsResp struct {
	Deleted int  `json:"deleted" jsonschema_description:"Number of objects deleted (0 if dry_run=true)"`
	Matches int  `json:"matches" jsonschema_description:"Number of objects that matched the deletion criteria"`
	DryRun  bool `json:"dry_run" jsonschema_description:"Whether this was a dry run (true) or actual deletion (false)"`
}
