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
	"reflect"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	shardingConfig "github.com/weaviate/weaviate/usecases/sharding/config"
)

type Parser struct {
	clusterState clusterState
	configParser VectorConfigParser
	validator    validator
}

func NewParser(cs clusterState, vCfg VectorConfigParser, v validator) *Parser {
	return &Parser{
		clusterState: cs,
		configParser: vCfg,
		validator:    v,
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
		return nil
	}

	if class.VectorIndexConfig != nil {
		return fmt.Errorf("class.vectorIndexConfig can not be set if class.vectorConfig is configured")
	}

	if err := m.parseTargetVectorsVectorIndexConfig(class); err != nil {
		return err
	}
	return nil
}

func (m *Parser) parseShardingConfig(class *models.Class) (err error) {
	// multiTenancyConfig and shardingConfig are mutually exclusive
	cfg := shardingConfig.Config{} // cfg is empty in case of MT
	if !schema.MultiTenancyEnabled(class) {
		cfg, err = shardingConfig.ParseConfig(class.ShardingConfig,
			m.clusterState.NodeCount())
		if err != nil {
			return err
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
) (schemaConfig.VectorIndexConfig, error) {
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

// ParseClassUpdate parses a class after unmarshaling by setting concrete types for the fields
func (p *Parser) ParseClassUpdate(class, update *models.Class) (*models.Class, error) {
	if err := p.ParseClass(update); err != nil {
		return nil, err
	}
	mtEnabled, err := validateUpdatingMT(class, update)
	if err != nil {
		return nil, err
	}

	if class.ReplicationConfig.Factor != update.ReplicationConfig.Factor {
		return nil, fmt.Errorf("updating replication factor is not supported yet")
	}

	if err := validateImmutableFields(class, update); err != nil {
		return nil, err
	}

	// run target vectors validation first, as it will reject classes
	// where legacy vector was changed to target vectors and vice versa
	if err := validateVectorConfigsParityAndImmutables(class, update); err != nil {
		return nil, err
	}

	if err := validateVectorIndexConfigImmutableFields(class, update); err != nil {
		return nil, err
	}

	if hasTargetVectors(update) {
		if err := p.validator.ValidateVectorIndexConfigsUpdate(asVectorIndexConfigs(class), asVectorIndexConfigs(update)); err != nil {
			return nil, err
		}
	} else {
		vIdxConfig, ok1 := class.VectorIndexConfig.(schemaConfig.VectorIndexConfig)
		vIdxConfigU, ok2 := update.VectorIndexConfig.(schemaConfig.VectorIndexConfig)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("vector index config wrong type: current=%t new=%t", ok1, ok2)
		}
		if err := p.validator.ValidateVectorIndexConfigUpdate(vIdxConfig, vIdxConfigU); err != nil {
			return nil, fmt.Errorf("validate vector index config: %w", err)
		}
	}

	if err := validateShardingConfig(class, update, mtEnabled); err != nil {
		return nil, fmt.Errorf("validate sharding config: %w", err)
	}

	if !reflect.DeepEqual(class.Properties, update.Properties) {
		return nil, errors.Errorf(
			"properties cannot be updated through updating the class. Use the add " +
				"property feature (e.g. \"POST /v1/schema/{className}/properties\") " +
				"to add additional properties")
	}

	if err := p.validator.ValidateInvertedIndexConfigUpdate(class.InvertedIndexConfig, update.InvertedIndexConfig); err != nil {
		return nil, fmt.Errorf("inverted index config: %w", err)
	}

	return update, nil
}

func hasTargetVectors(class *models.Class) bool {
	return len(class.VectorConfig) > 0
}
