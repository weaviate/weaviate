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
	CollectionName string               `json:"collection_name" jsonschema:"required" jsonschema_description:"Name of collection to upsert object into"`
	TenantName     string               `json:"tenant_name,omitempty" jsonschema_description:"Name of the tenant the object belongs to"`
	UUID           string               `json:"uuid,omitempty" jsonschema_description:"UUID of the object. If not provided, a new UUID will be generated. If provided and exists, the object will be updated; otherwise, a new object with this UUID will be created"`
	Properties     map[string]any       `json:"properties" jsonschema:"required" jsonschema_description:"Properties of the object to upsert"`
	Vectors        map[string][]float32 `json:"vectors,omitempty" jsonschema_description:"Named vectors for the object (e.g., {'default': [0.1, 0.2, ...], 'image': [0.3, 0.4, ...]})"`
}

type CreateCollectionArgs struct {
	CollectionName      string         `json:"collection_name" jsonschema:"required" jsonschema_description:"Name of the collection to create. Multiple words should be concatenated in CamelCase, e.g. 'ArticleAuthor'"`
	Description         string         `json:"description,omitempty" jsonschema_description:"Description of the collection for metadata purposes"`
	Properties          []any          `json:"properties,omitempty" jsonschema_description:"Array of property definitions. Each property must be an object with 'name' (string) and 'dataType' (array of strings, e.g., [\"text\"], [\"number\"]), plus optional 'description', 'indexFilterable', 'indexSearchable', 'tokenization' fields. Example: [{\"name\": \"title\", \"dataType\": [\"text\"], \"indexSearchable\": true}, {\"name\": \"score\", \"dataType\": [\"number\"], \"indexFilterable\": true}]"`
	InvertedIndexConfig map[string]any `json:"invertedIndexConfig,omitempty" jsonschema_description:"Configuration for the inverted index (e.g., {'bm25': {'b': 0.75, 'k1': 1.2}, 'stopwords': {'preset': 'en'}})"`
	VectorConfig        map[string]any `json:"vectorConfig,omitempty" jsonschema_description:"Configuration for named vectors. Maps vector name to config object with 'vectorizer' (nested object where key is vectorizer name), 'vectorIndexType', and optional 'vectorIndexConfig'. Example: {\"text_vector\": {\"vectorizer\": {\"text2vec-transformers\": {\"properties\": [\"title\", \"description\"]}}, \"vectorIndexType\": \"hnsw\", \"vectorIndexConfig\": {\"ef\": 100, \"efConstruction\": 128}}}"`
	MultiTenancyConfig  map[string]any `json:"multiTenancyConfig,omitempty" jsonschema_description:"Multi-tenancy configuration (e.g., {'enabled': true, 'autoTenantCreation': false})"`
}
