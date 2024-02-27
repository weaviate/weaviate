package models

import (
	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type TypeSafeSchema struct {

		// Semantic classes that are available.
		Classes []*TypedClass `json:"classes"`
	
		// Email of the maintainer.
		// Format: email
		Maintainer strfmt.Email `json:"maintainer,omitempty"`

		// Name of the schema.
		Name string `json:"name,omitempty"`
	}

func TypedSchema(schema *models.Schema) *TypeSafeSchema {
	classes := make([]*TypedClass, len(schema.Classes))
	for i, class := range schema.Classes {
		classes[i] = TypedClass(class)
	}
	return &TypeSafeSchema{
		Classes:    classes,
		Maintainer: schema.Maintainer,
		Name:       schema.Name,
	}
}

func UntypedSchema(schema *TypeSafeSchema) *models.Schema {
	classes := make([]*models.Class, len(schema.Classes))
	for i, class := range schema.Classes {
		classes[i] = UntypedClass(class)
	}
	return &models.Schema{
		Classes:    classes,
		Maintainer: schema.Maintainer,
		Name:       schema.Name,
	}
}

type TypeSafeClass struct {

	// Name of the class as URI relative to the schema URL.
	Class string `json:"class,omitempty"`

	// Description of the class.
	Description string `json:"description,omitempty"`

	// inverted index config
	InvertedIndexConfig *models.InvertedIndexConfig `json:"invertedIndexConfig,omitempty"`

	// Configuration specific to modules this Weaviate instance has installed
	ModuleConfig map[string]interface{} `json:"moduleConfig,omitempty"`

	// multi tenancy config
	MultiTenancyConfig *models.MultiTenancyConfig `json:"multiTenancyConfig,omitempty"`

	// The properties of the class.
	Properties []*models.Property `json:"properties"`

	// replication config
	ReplicationConfig *models.ReplicationConfig `json:"replicationConfig,omitempty"`

	// Manage how the index should be sharded and distributed in the cluster
	ShardingConfig sharding.Config `json:"shardingConfig,omitempty"`

	// vector config
	VectorConfig map[string]models.VectorConfig `json:"vectorConfig,omitempty"`

	// Vector-index config, that is specific to the type of index selected in vectorIndexType
	VectorIndexConfig schema.VectorIndexConfig `json:"vectorIndexConfig,omitempty"`

	// Name of the vector index to use, eg. (HNSW)
	VectorIndexType string `json:"vectorIndexType,omitempty"`

	// Specify how the vectors for this class should be determined. The options are either 'none' - this means you have to import a vector with each object yourself - or the name of a module that provides vectorization capabilities, such as 'text2vec-contextionary'. If left empty, it will use the globally configured default which can itself either be 'none' or a specific module.
	Vectorizer string `json:"vectorizer,omitempty"`
}


func TypedClass(class *models.Class) *TypedClass {
	return &TypedClass{
		Class:              TypedSchema,
		Description:        class.Description,
		InvertedIndexConfig: class.InvertedIndexConfig,
		ModuleConfig:       class.ModuleConfig,
		MultiTenancyConfig: class.MultiTenancyConfig,
		Properties:         class.Properties,
		ReplicationConfig:  class.Replication
		ShardingConfig:     class.ShardingConfig,
		VectorConfig:       class.VectorConfig,
		VectorIndexConfig:  class.VectorIndex
		VectorIndexType:    class.VectorIndex
		Vectorizer:         class.Vectorizer
	}
}

func UntypedClass(class *TypedClass) *models.Class {
	return &models.Class{
		Class:              class.Class,
		Description:        class.Description,
		InvertedIndexConfig: class.InvertedIndexConfig,
		ModuleConfig:       class.ModuleConfig,
		MultiTenancyConfig: class.MultiTenancyConfig,
		Properties:         class.Properties,
		ReplicationConfig:  class.Replication
		ShardingConfig:     class.ShardingConfig,
		VectorConfig:       class.VectorConfig,
		VectorIndexConfig:  class.VectorIndex
		VectorIndexType:    class.VectorIndex
		Vectorizer:         class.Vectorizer
	}
}
