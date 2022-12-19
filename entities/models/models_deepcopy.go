package models

func (s *Schema) Deepcopy() *Schema {
	classes := make([]*Class, len(s.Classes))
	for i, class := range s.Classes {
		classes[i] = class.Deepcopy()
	}

	return &Schema{Name: s.Name, Maintainer: s.Maintainer, Classes: classes}
}

func (c *Class) Deepcopy() *Class {
	if c == nil {
		return nil
	}

	var properties []*Property = nil
	if c.Properties != nil {
		properties = make([]*Property, len(c.Properties))
		for i, prop := range c.Properties {
			properties[i] = prop.Deepcopy()
		}
	}
	var ReplicationConf *ReplicationConfig = nil
	if c.ReplicationConfig != nil {
		ReplicationConf = &ReplicationConfig{Factor: c.ReplicationConfig.Factor}
	}

	return &Class{
		Class:               c.Class,
		Description:         c.Description,
		ModuleConfig:        c.ModuleConfig,
		ShardingConfig:      c.ShardingConfig,
		VectorIndexConfig:   c.VectorIndexConfig,
		VectorIndexType:     c.VectorIndexType,
		ReplicationConfig:   ReplicationConf,
		Vectorizer:          c.Vectorizer,
		InvertedIndexConfig: c.InvertedIndexConfig.Deepcopy(),
		Properties:          properties,
	}
}

func (p *Property) Deepcopy() *Property {
	var IndexInverted *bool = nil
	if p.IndexInverted != nil {
		b := *(p.IndexInverted)
		IndexInverted = &b
	}

	return &Property{
		DataType:      p.DataType,
		Description:   p.Description,
		ModuleConfig:  p.ModuleConfig,
		Name:          p.Name,
		Tokenization:  p.Tokenization,
		IndexInverted: IndexInverted,
	}
}

func (i *InvertedIndexConfig) Deepcopy() *InvertedIndexConfig {
	if i == nil {
		return nil
	}

	var Bm25 *BM25Config = nil
	if i.Bm25 != nil {
		Bm25 = &BM25Config{B: i.Bm25.B, K1: i.Bm25.K1}
	}

	var stopwords *StopwordConfig = nil
	if i.Stopwords != nil {
		stopwords = &StopwordConfig{Additions: i.Stopwords.Additions, Preset: i.Stopwords.Preset, Removals: i.Stopwords.Removals}
	}

	return &InvertedIndexConfig{
		Bm25:                   Bm25,
		CleanupIntervalSeconds: i.CleanupIntervalSeconds,
		IndexNullState:         i.IndexNullState,
		IndexPropertyLength:    i.IndexPropertyLength,
		IndexTimestamps:        i.IndexTimestamps,
		Stopwords:              stopwords,
	}
}
