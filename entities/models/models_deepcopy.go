package models

//type SafeSchema struct {
//	embeddedSchema *Schema
//	deadlock.Mutex
//}
//
//func (s *SafeSchema) ReadOnlySchema() *Schema {
//	s.Lock()
//	defer s.Unlock()
//	return s.deepcopy()
//}

func (s *Schema) Deepcopy() *Schema {
	classes := make([]*Class, len(s.Classes))
	for i, class := range s.Classes {
		classes[i] = class.Deepcopy()
	}

	return &Schema{Name: s.Name, Maintainer: s.Maintainer, Classes: classes}
}

//// GetClassByName returns the class by its name
//func (ss *SafeSchema) GetReadOnlyClassByName(className string) (*Class, error) {
//	s := ss.embeddedSchema
//	if s == nil {
//		return nil, fmt.Errorf(schema.ErrorNoSuchClass, className)
//	}
//	// For each class
//	for _, class := range s.Classes {
//		// Check if the name of the class is the given name, that's the class we need
//		if class.Class == className {
//			return class, nil
//		}
//	}
//
//	return nil, fmt.Errorf(schema.ErrorNoSuchClass, className)
//}

func (c *Class) Deepcopy() *Class {
	if c == nil {
		return nil
	}
	properties := []*Property{}
	// properties := make([]*Property, len(c.Properties))
	for _, prop := range c.Properties {
		// properties[i] = prop.deepcopy()
		properties = append(properties, prop.Deepcopy())
	}
	return &Class{
		Class:               c.Class,
		Description:         c.Description,
		ModuleConfig:        c.ModuleConfig,
		ShardingConfig:      c.ShardingConfig,
		VectorIndexConfig:   c.VectorIndexConfig,
		VectorIndexType:     c.VectorIndexType,
		Vectorizer:          c.Vectorizer,
		InvertedIndexConfig: c.InvertedIndexConfig.Deepcopy(),
		Properties:          properties,
	}
}

func (p *Property) Deepcopy() *Property {
	IndexInverted := p.IndexInverted
	if IndexInverted != nil {
		IndexInverted = &(*(p.IndexInverted))
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
	return &InvertedIndexConfig{
		Bm25:                   &(*i.Bm25),
		CleanupIntervalSeconds: i.CleanupIntervalSeconds,
		IndexNullState:         i.IndexNullState,
		IndexPropertyLength:    i.IndexPropertyLength,
		IndexTimestamps:        i.IndexTimestamps,
		Stopwords:              &(*i.Stopwords),
	}
}
