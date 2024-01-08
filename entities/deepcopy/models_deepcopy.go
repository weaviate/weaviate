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
		replicationConf = &models.ReplicationConfig{Factor: c.ReplicationConfig.Factor}
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

func Prop(p *models.Property) *models.Property {
	return &models.Property{
		DataType:        p.DataType,
		Description:     p.Description,
		ModuleConfig:    p.ModuleConfig,
		Name:            p.Name,
		Tokenization:    p.Tokenization,
		IndexFilterable: ptrBoolCopy(p.IndexFilterable),
		IndexSearchable: ptrBoolCopy(p.IndexSearchable),
	}
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
	}
}
