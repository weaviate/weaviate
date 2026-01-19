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

package search

type QueryHybridArgs struct {
	Query            string         `json:"query" jsonschema:"required" jsonschema_description:"The plain-text query to search the collection on"`
	CollectionName   string         `json:"collection_name" jsonschema:"required" jsonschema_description:"Name of collection to search from"`
	TenantName       string         `json:"tenant_name,omitempty" jsonschema_description:"Name of the tenant to search within"`
	Alpha            *float64       `json:"alpha,omitempty" jsonschema_description:"Semantic weight (0.0 = pure keyword, 1.0 = pure vector). Default is 0.5 if not specified"`
	Limit            *int           `json:"limit,omitempty" jsonschema_description:"Maximum number of results to return"`
	TargetVectors    []string       `json:"target_vectors,omitempty" jsonschema_description:"Target vectors to use in vector search"`
	TargetProperties []string       `json:"target_properties,omitempty" jsonschema_description:"Properties to perform BM25 keyword search on. If not specified, searches all text properties"`
	ReturnProperties []string       `json:"return_properties,omitempty" jsonschema_description:"Properties to return in the result"`
	ReturnMetadata   []string       `json:"return_metadata,omitempty" jsonschema_description:"Metadata to return (e.g., 'id', 'vector', 'distance', 'score', 'creationTimeUnix', 'lastUpdateTimeUnix')"`
	Filters          map[string]any `json:"filters,omitempty" jsonschema_description:"Filter to narrow down search results using Weaviate's where filter syntax. Same format as 'where' in weaviate-objects-delete. Valid operators: Equal, NotEqual, GreaterThan, GreaterThanEqual (NOT GreaterThanOrEqual), LessThan, LessThanEqual (NOT LessThanOrEqual), Like, And, Or, Not, ContainsAny, ContainsAll, ContainsNone, IsNull, WithinGeoRange. Required fields: 'path' (array of property names), 'operator', and typed value field. Value fields: 'valueText' for strings, 'valueNumber' for numbers, 'valueBoolean' for booleans, 'valueDate' for dates in RFC3339 format (MUST include time: '2001-01-01T00:00:00Z', NOT just '2001-01-01'). Examples: {\\\"path\\\": [\\\"status\\\"], \\\"operator\\\": \\\"Equal\\\", \\\"valueText\\\": \\\"published\\\"}. Date: {\\\"path\\\": [\\\"releaseDate\\\"], \\\"operator\\\": \\\"GreaterThan\\\", \\\"valueDate\\\": \\\"2001-01-01T00:00:00Z\\\"}. CRITICAL: Use 'GreaterThanEqual' NOT 'GreaterThanOrEqual'. Dates MUST be full RFC3339 with time. Filter is applied after hybrid search to refine results."`
}
