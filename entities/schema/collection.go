package schema

/// TODO-RAfT START
// Currently, our schema definition relies on auto-generated Swagger definitions for “entities/models/*”.
// While Swagger definitions work ok for transport, they introduce inefficiencies when used in core logic for several reasons.
// Types are not explicitly defined, leading to unnecessary parsing, even for primitive types.
// This places an undue burden on garbage collection, which has to manage numerous unnecessary pointers.

// In this task, our goal is to develop an efficient data structure, a native core data structure that addresses these deficiencies.
// To achieve this, we need to identify the various access patterns for entities/models to optimize the data structure for these scenarios.

/// TODO-RAFT END

type Collection struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`

	// inverted index config
	InvertedIndexConfig InvertedIndexConfig `json:"invertedIndex,omitempty"`

	// Configuration specific to modules this Weaviate instance has installed
	ModuleConfig map[string]interface{} `json:"moduleConfig,omitempty"`

	// multi tenancy config
	MultiTenancyConfig MultiTenancyConfig `json:"multiTenancyConfig,omitempty"`

	// The properties of the class.
	Properties []Property `json:"properties"`

	// replication config
	ReplicationConfig ReplicationConfig `json:"replicationConfig,omitempty"`

	// Manage how the index should be sharded and distributed in the cluster
	ShardingConfig ShardingConfig `json:"shardingConfig,omitempty"`

	// VectorIndexType which vector index to use
	VectorIndexType VectorIndexType `json:"vectorIndexType,omitempty"`
	// TODO-RAFT START
	// TODO-RAFT: can we do better?
	// We can unmarshal vector index config into a concrete struct
	// depending on VectorIndexType and store into VectorIndexConfig

	// Vector-index config, that is specific to the type of index selected in vectorIndexType
	VectorIndexConfig interface{} `json:"vectorIndexConfig,omitempty"`
	// TODO-RAFT END

	// Specify how the vectors for this class should be determined. The options are either 'none' - this means you have to import a vector with each object yourself - or the name of a module that provides vectorization capabilities, such as 'text2vec-contextionary'. If left empty, it will use the globally configured default which can itself either be 'none' or a specific module.
	Vectorizer string `json:"vectorizer,omitempty"`
}

type VectorIndexType int

const (
	VectorIndexTypeHNSW VectorIndexType = iota
)

type MultiTenancyConfig struct {
	Enabled bool `json:"enabled"`
}

type ReplicationConfig struct {
	// Factor represent replication factor
	Factor int64 `json:"factor,omitempty"`
}

type ShardingConfig struct {
	VirtualPerPhysical  int    `json:"virtualPerPhysical"`
	DesiredCount        int    `json:"desiredCount"`
	ActualCount         int    `json:"actualCount"`
	DesiredVirtualCount int    `json:"desiredVirtualCount"`
	ActualVirtualCount  int    `json:"actualVirtualCount"`
	Key                 string `json:"key"`
	Strategy            string `json:"strategy"`
	Function            string `json:"function"`
}

type Property struct {
	// Name of the property as URI relative to the schema URL.
	Name string `json:"name,omitempty"`

	// Description of the property.
	Description string `json:"description,omitempty"`

	// Can be a reference to another type when it starts with a capital (for example Person), otherwise "string" or "int".
	DataType string `json:"data_type"`

	// Optional. Should this property be indexed in the inverted index. Defaults to true. If you choose false, you will not be able to use this property in where filters. This property has no affect on vectorization decisions done by modules
	IndexFilterable bool `json:"indexFilterable,omitempty"`

	// Optional. Should this property be indexed in the inverted index. Defaults to true. If you choose false, you will not be able to use this property in where filters, bm25 or hybrid search. This property has no affect on vectorization decisions done by modules (deprecated as of v1.19; use indexFilterable or/and indexSearchable instead)
	IndexInverted bool `json:"indexInverted,omitempty"`

	// Optional. Should this property be indexed in the inverted index. Defaults to true. Applicable only to properties of data type text and text[]. If you choose false, you will not be able to use this property in bm25 or hybrid search. This property has no affect on vectorization decisions done by modules
	IndexSearchable bool `json:"indexSearchable,omitempty"`

	// Configuration specific to modules this Weaviate instance has installed
	ModuleConfig interface{} `json:"moduleConfig,omitempty"`

	// The properties of the nested object(s). Applies to object and object[] data types.
	NestedProperties []*NestedProperty `json:"nestedProperties,omitempty"`

	// Determines tokenization of the property as separate words or whole field. Optional. Applies to text and text[] data types. Allowed values are `word` (default; splits on any non-alphanumerical, lowercases), `lowercase` (splits on white spaces, lowercases), `whitespace` (splits on white spaces), `field` (trims). Not supported for remaining data types
	// Enum: [word lowercase whitespace field]
	Tokenization string `json:"tokenization,omitempty"`
}

type NestedProperty struct {
	// name
	Name string `json:"name,omitempty"`
	// description
	Description string `json:"description,omitempty"`
	// data type
	DataType string `json:"data_type"` // Why the data type is not

	// index filterable
	IndexFilterable bool `json:"index_filterable,omitempty"`

	// index searchable
	IndexSearchable bool `json:"index_searchable,omitempty"`

	// nested properties
	NestedProperties []*NestedProperty `json:"nested_properties,omitempty"`

	// tokenization
	// Enum: [word lowercase whitespace field]
	Tokenization string `json:"tokenization,omitempty"`
}
