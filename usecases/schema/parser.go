//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type Parser struct {
	clusterState     clusterState
	hnswConfigParser VectorConfigParser
}

func NewParser(clusterState clusterState, hnswConfigParser VectorConfigParser) *Parser {
	return &Parser{
		clusterState:     clusterState,
		hnswConfigParser: hnswConfigParser,
	}
}

func (m *Parser) ParseClass(class *models.Class) error {
	if class == nil { // TODO-RAFT this might not be needed
		return fmt.Errorf("class cannot be nil")
	}

	if err := m.parseShardingConfig(class); err != nil {
		return fmt.Errorf("parse sharding config: %w", err)
	}

	if err := m.parseVectorIndexConfig(class); err != nil {
		return fmt.Errorf("parse vector index config: %w", err)
	}
	return nil
}

func (m *Parser) parseVectorIndexConfig(class *models.Class,
) error {
	if !hasTargetVectors(class) {
		parsed, err := m.parseGivenVectorIndexConfig(class.VectorIndexType, class.VectorIndexConfig)
		if err != nil {
			return err
		}
		class.VectorIndexConfig = parsed
	}

	if err := m.parseTargetVectorsVectorIndexConfig(class); err != nil {
		return err
	}
	return nil
}

func (m *Parser) parseShardingConfig(class *models.Class) (err error) {
	// multiTenancyConfig and shardingConfig are mutually exclusive
	cfg := sharding.Config{} // cfg is empty in case of MT
	if !schema.MultiTenancyEnabled(class) {
		cfg, err = sharding.ParseConfig(class.ShardingConfig,
			m.clusterState.NodeCount())
		if err != nil {
			return fmt.Errorf("parse sharding config: %w", err)
		}

	}
	class.ShardingConfig = cfg
	return nil
}

func (m *Parser) parseTargetVectorsVectorIndexConfig(class *models.Class) error {
	for targetVector, vectorConfig := range class.VectorConfig {
		parsed, err := m.parseGivenVectorIndexConfig(vectorConfig.VectorIndexType, vectorConfig.VectorIndexConfig)
		if err != nil {
			return fmt.Errorf("parse vector config for %s: %w", targetVector, err)
		}
		vectorConfig.VectorIndexConfig = parsed
		class.VectorConfig[targetVector] = vectorConfig
	}
	return nil
}

func (m *Parser) parseGivenVectorIndexConfig(vectorIndexType string,
	vectorIndexConfig interface{},
) (schema.VectorIndexConfig, error) {
	if vectorIndexType != "hnsw" && vectorIndexType != "flat" {
		return nil, errors.Errorf(
			"parse vector index config: unsupported vector index type: %q",
			vectorIndexType)
	}

	parsed, err := m.configParser(vectorIndexConfig, vectorIndexType)
	if err != nil {
		return nil, errors.Wrap(err, "parse vector index config")
	}
	return parsed, nil
}

func hasTargetVectors(class *models.Class) bool {
	return len(class.VectorConfig) > 0
}
