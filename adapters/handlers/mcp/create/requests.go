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

type ObjectToUpsert struct {
	UUID       string               `json:"uuid,omitempty" jsonschema_description:"UUID of the object. If not provided, a new UUID will be generated"`
	Properties map[string]any       `json:"properties" jsonschema:"required" jsonschema_description:"Properties of the object"`
	Vectors    map[string][]float32 `json:"vectors,omitempty" jsonschema_description:"Named vectors for the object (e.g., {'default': [0.1, 0.2, ...], 'image': [0.3, 0.4, ...]})"`
}

type UpsertObjectArgs struct {
	CollectionName string           `json:"collection_name" jsonschema:"required" jsonschema_description:"Name of collection to upsert objects into"`
	TenantName     string           `json:"tenant_name,omitempty" jsonschema_description:"Name of the tenant the objects belong to (for multi-tenant collections)"`
	Objects        []ObjectToUpsert `json:"objects" jsonschema:"required,minItems=1" jsonschema_description:"Array of objects to upsert. Each object should have 'properties' (required) and optionally 'uuid' and 'vectors'. Minimum 1 object required."`
}

type CreateCollectionArgs struct {
	CollectionName      string         `json:"collection_name" jsonschema:"required" jsonschema_description:"Name of the collection to create. Multiple words should be concatenated in CamelCase, e.g. 'ArticleAuthor'"`
	Description         string         `json:"description,omitempty" jsonschema_description:"Description of the collection for metadata purposes"`
	Properties          []any          `json:"properties,omitempty" jsonschema_description:"Array of property definitions. Each property must be an object with 'name' (string) and 'dataType' (array of strings, e.g., [\"text\"], [\"number\"]), plus optional 'description', 'indexFilterable', 'indexSearchable', 'tokenization' fields. Example: [{\"name\": \"title\", \"dataType\": [\"text\"], \"indexSearchable\": true}, {\"name\": \"score\", \"dataType\": [\"number\"], \"indexFilterable\": true}]"`
	InvertedIndexConfig map[string]any `json:"invertedIndexConfig,omitempty" jsonschema_description:"Configuration for the inverted index (e.g., {'bm25': {'b': 0.75, 'k1': 1.2}, 'stopwords': {'preset': 'en'}})"`
	VectorConfig        map[string]any `json:"vectorConfig,omitempty" jsonschema_description:"Configuration for named vectors. Maps vector name to config object with 'vectorizer' (nested object where key is vectorizer name), 'vectorIndexType', and optional 'vectorIndexConfig'. Example: {\"text_vector\": {\"vectorizer\": {\"text2vec-transformers\": {\"properties\": [\"title\", \"description\"]}}, \"vectorIndexType\": \"hnsw\", \"vectorIndexConfig\": {\"ef\": 100, \"efConstruction\": 128}}}"`
	MultiTenancyConfig  map[string]any `json:"multiTenancyConfig,omitempty" jsonschema_description:"Multi-tenancy configuration (e.g., {'enabled': true, 'autoTenantCreation': false})"`
}

type DeleteObjectsArgs struct {
	CollectionName string         `json:"collection_name" jsonschema:"required" jsonschema_description:"Name of the collection to delete objects from"`
	TenantName     string         `json:"tenant_name,omitempty" jsonschema_description:"Name of the tenant (for multi-tenant collections)"`
	Where          map[string]any `json:"where,omitempty" jsonschema_description:"Filter to specify which objects to delete. Uses Weaviate's where filter syntax with required fields: 'path' (array of property names), 'operator', and a typed value field. Valid operators: Equal, NotEqual, GreaterThan, GreaterThanEqual (NOT GreaterThanOrEqual), LessThan, LessThanEqual (NOT LessThanOrEqual), Like, And, Or, Not, ContainsAny, ContainsAll, ContainsNone, IsNull, WithinGeoRange. Value fields: 'valueText' for strings, 'valueNumber' for numbers, 'valueBoolean' for booleans, 'valueDate' for dates in RFC3339 format (MUST include time: '2001-01-01T00:00:00Z', NOT just '2001-01-01'). Examples: {\"path\": [\"status\"], \"operator\": \"Equal\", \"valueText\": \"draft\"}. Numeric: {\"path\": [\"price\"], \"operator\": \"LessThan\", \"valueNumber\": 10}. Date: {\"path\": [\"releaseDate\"], \"operator\": \"GreaterThan\", \"valueDate\": \"2001-01-01T00:00:00Z\"}. AND: {\"operator\": \"And\", \"operands\": [{\"path\": [\"status\"], \"operator\": \"Equal\", \"valueText\": \"archived\"}, {\"path\": [\"year\"], \"operator\": \"LessThan\", \"valueNumber\": 2020}]}. CRITICAL: Use 'GreaterThanEqual' not 'GreaterThanOrEqual'. Dates MUST be full RFC3339 with time. 'path' must be array. Omit 'path' for And/Or. If not provided, matches ALL objects."`
	DryRun         *bool          `json:"dry_run,omitempty" jsonschema_description:"If true, returns count of objects that would be deleted without actually deleting them (default: true)"`
}
