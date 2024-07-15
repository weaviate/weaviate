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

package schema

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/config"
	vIndex "github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	sharding "github.com/weaviate/weaviate/usecases/sharding/config"
)

type Collection struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`

	// inverted index config
	InvertedIndexConfig InvertedIndexConfig `json:"invertedIndex,omitempty"`

	// TODO-RAFT START
	// Can we also get rid of the interface{} in the value side ?
	// Configuration specific to modules this Weaviate instance has installed
	ModuleConfig map[string]interface{} `json:"moduleConfig,omitempty"`
	// TODO-RAFT END

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

	// VectorIndexConfig underlying implementation depends on VectorIndexType
	VectorIndexConfig config.VectorIndexConfig `json:"vectorIndexConfig,omitempty"`

	// Specify how the vectors for this class should be determined. The options are either 'none' - this means you have to import a vector
	// with each object yourself - or the name of a module that provides vectorization capabilities, such as 'text2vec-contextionary'. If
	// left empty, it will use the globally configured default which can itself either be 'none' or a specific module.
	Vectorizer string `json:"vectorizer,omitempty"`
}

type VectorIndexType int

const (
	// VectorIndexTypeEmpty is used when we parse an unexpected index type
	VectorIndexTypeEmpty VectorIndexType = iota
	VectorIndexTypeHNSW
	VectorIndexTypeFlat
)

var (
	vectorIndexTypeToString = map[VectorIndexType]string{
		VectorIndexTypeHNSW:  vIndex.VectorIndexTypeHNSW,
		VectorIndexTypeFlat:  vIndex.VectorIndexTypeFLAT,
		VectorIndexTypeEmpty: "",
	}
	stringToVectorIndexType = map[string]VectorIndexType{
		vIndex.VectorIndexTypeHNSW: VectorIndexTypeHNSW,
		vIndex.VectorIndexTypeFLAT: VectorIndexTypeFlat,
		"":                         VectorIndexTypeEmpty,
	}
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
	// TODO-RAFT: Can we make DataType a slice of interface where other type and native type implements it ?
	DataType []string `json:"data_type"`

	// Optional. Should this property be indexed in the inverted index. Defaults to true. If you choose false, you will not be able to use this property in where filters. This property has no affect on vectorization decisions done by modules
	IndexFilterable bool `json:"indexFilterable,omitempty"`

	// Optional. Should this property be indexed in the inverted index. Defaults to true. If you choose false, you will not be able to use this property in where filters, bm25 or hybrid search. This property has no affect on vectorization decisions done by modules (deprecated as of v1.19; use indexFilterable or/and indexSearchable instead)
	IndexInverted bool `json:"indexInverted,omitempty"`

	// Optional. Should this property be indexed in the inverted index. Defaults to true. Applicable only to properties of data type text and text[]. If you choose false, you will not be able to use this property in bm25 or hybrid search. This property has no affect on vectorization decisions done by modules
	IndexSearchable bool `json:"indexSearchable,omitempty"`

	// Optional. Should this property be indexed in the inverted index. Defaults to false. Provides better performance for range queries compared to filterable index in large datasets. Applicable only to properties of data type int, number, date."
	IndexRangeFilters bool `json:"indexRangeFilters,omitempty"`

	// Configuration specific to modules this Weaviate instance has installed
	ModuleConfig map[string]interface{} `json:"moduleConfig,omitempty"`

	// The properties of the nested object(s). Applies to object and object[] data types.
	NestedProperties []NestedProperty `json:"nestedProperties,omitempty"`

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
	DataType []string `json:"data_type"`

	// index filterable
	IndexFilterable bool `json:"index_filterable,omitempty"`

	// index searchable
	IndexSearchable bool `json:"index_searchable,omitempty"`

	// index range filters
	IndexRangeFilters bool `json:"index_range_filters,omitempty"`

	// nested properties
	NestedProperties []NestedProperty `json:"nested_properties,omitempty"`

	// tokenization
	// Enum: [word lowercase whitespace field]
	Tokenization string `json:"tokenization,omitempty"`
}

// NestedPropertyFromModel returns a NestedProperty copied from m.
func NestedPropertyFromModel(m models.NestedProperty) NestedProperty {
	n := NestedProperty{}

	n.DataType = m.DataType
	n.Description = m.Description
	if m.IndexFilterable != nil {
		n.IndexFilterable = *m.IndexFilterable
	} else {
		n.IndexFilterable = true
	}
	if m.IndexSearchable != nil {
		n.IndexSearchable = *m.IndexSearchable
	} else {
		n.IndexSearchable = true
	}
	if m.IndexRangeFilters != nil {
		n.IndexRangeFilters = *m.IndexRangeFilters
	} else {
		n.IndexRangeFilters = false
	}
	n.Name = m.Name
	n.Tokenization = m.Tokenization
	if len(m.NestedProperties) > 0 {
		n.NestedProperties = make([]NestedProperty, 0, len(m.NestedProperties))
		for _, npm := range m.NestedProperties {
			np := NestedPropertyFromModel(*npm)

			n.NestedProperties = append(n.NestedProperties, np)
		}
	}

	return n
}

// NestedPropertyToModel returns a models.NestedProperty from n. If the original models.NestedProperty from which n was created had nil pointers they will
// be replaced with default initialized structs in NestedProperty.
func NestedPropertyToModel(n NestedProperty) models.NestedProperty {
	var m models.NestedProperty

	m.DataType = n.DataType
	m.Description = n.Description
	indexFilterable := n.IndexFilterable
	m.IndexFilterable = &indexFilterable
	indexSearchable := n.IndexSearchable
	m.IndexSearchable = &indexSearchable
	indexRangeFilters := n.IndexRangeFilters
	m.IndexRangeFilters = &indexRangeFilters
	m.Name = n.Name
	m.Tokenization = n.Tokenization
	if len(n.NestedProperties) > 0 {
		m.NestedProperties = make([]*models.NestedProperty, 0, len(n.NestedProperties))
		for _, np := range n.NestedProperties {
			npm := NestedPropertyToModel(np)
			m.NestedProperties = append(m.NestedProperties, &npm)
		}
	}

	return m
}

// PropertyFromModel returns a Property copied from m.
func PropertyFromModel(m models.Property) Property {
	p := Property{}

	p.Name = m.Name
	p.DataType = m.DataType
	p.Description = m.Description
	if m.IndexFilterable != nil {
		p.IndexFilterable = *m.IndexFilterable
	} else {
		p.IndexFilterable = true
	}
	if m.IndexInverted != nil {
		p.IndexInverted = *m.IndexInverted
	} else {
		p.IndexInverted = true
	}
	if m.IndexSearchable != nil {
		p.IndexSearchable = *m.IndexSearchable
	} else {
		p.IndexSearchable = true
	}
	if m.IndexRangeFilters != nil {
		p.IndexRangeFilters = *m.IndexRangeFilters
	} else {
		p.IndexRangeFilters = false
	}
	if v, ok := m.ModuleConfig.(map[string]interface{}); ok {
		p.ModuleConfig = v
	}
	p.Tokenization = m.Tokenization
	if len(m.NestedProperties) > 0 {
		p.NestedProperties = make([]NestedProperty, 0, len(m.NestedProperties))
		for _, npm := range m.NestedProperties {
			np := NestedPropertyFromModel(*npm)

			p.NestedProperties = append(p.NestedProperties, np)
		}
	}

	return p
}

// PropertyToModel returns a models.Property from p. If the original models.Property from which n was created had nil pointers they will be replaced
// with default initialized structs in Property.
func PropertyToModel(p Property) models.Property {
	var m models.Property

	m.DataType = p.DataType
	m.Description = p.Description
	indexFilterable := p.IndexFilterable
	m.IndexFilterable = &indexFilterable
	indexInverted := p.IndexInverted
	m.IndexInverted = &indexInverted
	indexSearchable := p.IndexSearchable
	m.IndexSearchable = &indexSearchable
	indexRangeFilters := p.IndexRangeFilters
	m.IndexRangeFilters = &indexRangeFilters
	m.ModuleConfig = p.ModuleConfig
	m.Name = p.Name
	m.Tokenization = p.Tokenization
	if len(p.NestedProperties) > 0 {
		m.NestedProperties = make([]*models.NestedProperty, 0, len(p.NestedProperties))
		for _, np := range p.NestedProperties {
			npm := NestedPropertyToModel(np)
			m.NestedProperties = append(m.NestedProperties, &npm)
		}
	}

	return m
}

// ShardingConfigFromModel returns a ShardingConfig copied from m.
// If m isn't a sharding.Config underneath the interface{}, a default initialized ShardingConfig will be returned.
func ShardingConfigFromModel(m interface{}) ShardingConfig {
	sc := ShardingConfig{}

	v, ok := m.(sharding.Config)
	if !ok {
		return sc
	}

	sc.ActualCount = v.ActualCount
	sc.ActualVirtualCount = v.ActualVirtualCount
	sc.DesiredCount = v.DesiredCount
	sc.DesiredVirtualCount = v.DesiredVirtualCount
	sc.Function = v.Function
	sc.Key = v.Key
	sc.Strategy = v.Strategy
	sc.VirtualPerPhysical = v.VirtualPerPhysical

	return sc
}

// ShardingConfigToModel returns an interface{} containing a sharding.Config from s.
func ShardingConfigToModel(s ShardingConfig) interface{} {
	var m sharding.Config

	m.ActualCount = s.ActualCount
	m.ActualVirtualCount = s.ActualVirtualCount
	m.DesiredCount = s.DesiredCount
	m.DesiredVirtualCount = s.DesiredVirtualCount
	m.Function = s.Function
	m.Key = s.Key
	m.Strategy = s.Strategy
	m.VirtualPerPhysical = s.VirtualPerPhysical

	return m
}

// CollectionFromClass returns a Collection copied from m.
func CollectionFromClass(m models.Class) (Collection, error) {
	c := Collection{}

	c.Name = m.Class
	c.Description = m.Description
	if m.InvertedIndexConfig != nil {
		c.InvertedIndexConfig = InvertedIndexConfigFromModel(*m.InvertedIndexConfig)
		if v, ok := m.ModuleConfig.(map[string]interface{}); ok {
			c.ModuleConfig = v
		}
	}
	if m.MultiTenancyConfig != nil {
		c.MultiTenancyConfig.Enabled = m.MultiTenancyConfig.Enabled
	}
	c.Properties = make([]Property, len(m.Properties))
	c.Vectorizer = m.Vectorizer

	for i, mp := range m.Properties {
		p := PropertyFromModel(*mp)

		c.Properties[i] = p
	}
	if m.ReplicationConfig != nil {
		c.ReplicationConfig.Factor = m.ReplicationConfig.Factor
	}
	if m.ShardingConfig != nil {
		c.ShardingConfig = ShardingConfigFromModel(m.ShardingConfig)
	}

	vIndex, ok := stringToVectorIndexType[m.VectorIndexType]
	if !ok {
		return c, fmt.Errorf("unknown vector index: %s", m.VectorIndexType)
	}

	c.VectorIndexType = vIndex
	switch vIndex {
	case VectorIndexTypeHNSW:
		c.VectorIndexConfig = m.VectorIndexConfig.(hnsw.UserConfig)
	case VectorIndexTypeFlat:
		c.VectorIndexConfig = m.VectorIndexConfig.(flat.UserConfig)
	default:
	}

	return c, nil
}

// CollectionToClass returns a models.Class from c. If the original models.Class from which c was created had nil pointers they will be replaced
// with default initialized structs in models.Class.
func CollectionToClass(c Collection) models.Class {
	var m models.Class

	m.Class = c.Name
	m.Description = c.Description
	iic := InvertedIndexConfigToModel(c.InvertedIndexConfig)
	m.InvertedIndexConfig = &iic
	m.ModuleConfig = c.ModuleConfig
	var mtc models.MultiTenancyConfig
	mtc.Enabled = c.MultiTenancyConfig.Enabled
	m.MultiTenancyConfig = &mtc
	m.Properties = make([]*models.Property, len(c.Properties))
	for i, p := range c.Properties {
		mp := PropertyToModel(p)
		m.Properties[i] = &mp
	}
	var rc models.ReplicationConfig
	rc.Factor = c.ReplicationConfig.Factor
	m.ReplicationConfig = &rc
	m.ShardingConfig = ShardingConfigToModel(c.ShardingConfig)
	m.VectorIndexType = vectorIndexTypeToString[c.VectorIndexType]
	m.VectorIndexConfig = c.VectorIndexConfig
	m.Vectorizer = c.Vectorizer
	return m
}
