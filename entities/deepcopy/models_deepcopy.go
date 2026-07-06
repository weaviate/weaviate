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

package deepcopy

import "github.com/weaviate/weaviate/entities/models"

func Schema(s *models.Schema) *models.Schema {
	classes := make([]*models.Class, len(s.Classes))
	for i, class := range s.Classes {
		classes[i] = Class(class)
	}

	return &models.Schema{Name: s.Name, Maintainer: s.Maintainer, Classes: classes}
}

func Class(c *models.Class) *models.Class {
	if c == nil {
		return nil
	}

	var properties []*models.Property = nil
	if c.Properties != nil {
		properties = make([]*models.Property, len(c.Properties))
		for i, prop := range c.Properties {
			properties[i] = Prop(prop)
		}
	}
	var replicationConf *models.ReplicationConfig = nil
	if c.ReplicationConfig != nil {
		var asyncConfig *models.ReplicationAsyncConfig

		if c.ReplicationConfig.AsyncConfig != nil {
			ac := *c.ReplicationConfig.AsyncConfig
			asyncConfig = &ac
		}

		replicationConf = &models.ReplicationConfig{
			Factor:           c.ReplicationConfig.Factor,
			DeletionStrategy: c.ReplicationConfig.DeletionStrategy,
			AsyncConfig:      asyncConfig,
		}
	}

	return &models.Class{
		Class:               c.Class,
		Description:         c.Description,
		ModuleConfig:        c.ModuleConfig,
		ShardingConfig:      c.ShardingConfig,
		VectorIndexConfig:   c.VectorIndexConfig,
		VectorIndexType:     c.VectorIndexType,
		ReplicationConfig:   replicationConf,
		Vectorizer:          c.Vectorizer,
		InvertedIndexConfig: InvertedIndexConfig(c.InvertedIndexConfig),
		Properties:          properties,
	}
}

// Prop copies every exported field of models.Property. Keep this exhaustive:
// a struct-literal copy that misses a field silently drops it on every
// caller, and that class of bug (weaviate/weaviate#11689) is only caught by
// TestProp_CopiesAllExportedFields, not by the compiler.
func Prop(p *models.Property) *models.Property {
	if p == nil {
		return nil
	}

	return &models.Property{
		BucketGeneration:            p.BucketGeneration,
		DataType:                    p.DataType,
		Description:                 p.Description,
		DisableDuplicatedReferences: ptrBoolCopy(p.DisableDuplicatedReferences),
		IndexFilterable:             ptrBoolCopy(p.IndexFilterable),
		IndexInverted:               ptrBoolCopy(p.IndexInverted),
		IndexRangeFilters:           ptrBoolCopy(p.IndexRangeFilters),
		IndexSearchable:             ptrBoolCopy(p.IndexSearchable),
		ModuleConfig:                p.ModuleConfig,
		Name:                        p.Name,
		NestedProperties:            nestedPropSlice(p.NestedProperties),
		TextAnalyzer:                TextAnalyzer(p.TextAnalyzer),
		Tokenization:                p.Tokenization,
	}
}

// NestedProp copies every exported field of models.NestedProperty, recursing
// into its own NestedProperties so an object/object[] property's whole
// nested schema is deep-copied rather than sharing slice/pointer elements
// with the source.
func NestedProp(n *models.NestedProperty) *models.NestedProperty {
	if n == nil {
		return nil
	}

	return &models.NestedProperty{
		DataType:          n.DataType,
		Description:       n.Description,
		IndexFilterable:   ptrBoolCopy(n.IndexFilterable),
		IndexRangeFilters: ptrBoolCopy(n.IndexRangeFilters),
		IndexSearchable:   ptrBoolCopy(n.IndexSearchable),
		Name:              n.Name,
		NestedProperties:  nestedPropSlice(n.NestedProperties),
		TextAnalyzer:      TextAnalyzer(n.TextAnalyzer),
		Tokenization:      n.Tokenization,
	}
}

// nestedPropSlice rebuilds the slice itself (not just its elements) so the
// copy doesn't share a backing array with the source: appending to one
// must never resize/alias the other.
func nestedPropSlice(nps []*models.NestedProperty) []*models.NestedProperty {
	if nps == nil {
		return nil
	}

	cp := make([]*models.NestedProperty, len(nps))
	for i, np := range nps {
		cp[i] = NestedProp(np)
	}
	return cp
}

// TextAnalyzer copies a *models.TextAnalyzerConfig. Its fields are value
// types (bool/[]string/string), so a single dereference-copy is enough to
// stop the pointer itself from being shared; this mirrors how
// InvertedIndexConfig below rebuilds its nested configs.
func TextAnalyzer(t *models.TextAnalyzerConfig) *models.TextAnalyzerConfig {
	if t == nil {
		return nil
	}
	cp := *t
	return &cp
}

func ptrBoolCopy(ptrBool *bool) *bool {
	if ptrBool != nil {
		b := *ptrBool
		return &b
	}
	return nil
}

func InvertedIndexConfig(i *models.InvertedIndexConfig) *models.InvertedIndexConfig {
	if i == nil {
		return nil
	}

	var bm25 *models.BM25Config = nil
	if i.Bm25 != nil {
		bm25 = &models.BM25Config{B: i.Bm25.B, K1: i.Bm25.K1}
	}

	var stopwords *models.StopwordConfig = nil
	if i.Stopwords != nil {
		stopwords = &models.StopwordConfig{Additions: i.Stopwords.Additions, Preset: i.Stopwords.Preset, Removals: i.Stopwords.Removals}
	}

	return &models.InvertedIndexConfig{
		Bm25:                   bm25,
		CleanupIntervalSeconds: i.CleanupIntervalSeconds,
		IndexNullState:         i.IndexNullState,
		IndexPropertyLength:    i.IndexPropertyLength,
		IndexTimestamps:        i.IndexTimestamps,
		Stopwords:              stopwords,
		UsingBlockMaxWAND:      i.UsingBlockMaxWAND,
	}
}
