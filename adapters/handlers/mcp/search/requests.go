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

package search

type SearchWithHybridArgs struct {
	Collection       string   `json:"collection" jsonschema_description:"Name of collection to get tenants from"`
	Query            string   `json:"query,omitempty" jsonschema_description:"The plain-text query to search the collection on"`
	TargetProperties []string `json:"targetProperties,omitempty" jsonschema_description:"Names of properties to perform BM25 querying on"`
}
