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
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/usecases/config"
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
	if class == nil {
		return fmt.Errorf("class cannot be nil")
	}

	if strings.EqualFold(class.Class, config.DefaultRaftDir) {
		return fmt.Errorf("parse class name: %w", fmt.Errorf("class name `raft` is reserved"))
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
	if vectorIndexType != vectorindex.VectorIndexTypeHNSW && vectorIndexType != vectorindex.VectorIndexTypeFLAT && vectorIndexType != vectorindex.VectorIndexTypeDYNAMIC {
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
		if err := p.validator.ValidateVectorIndexConfigsUpdate(
			asVectorIndexConfigs(class), asVectorIndexConfigs(update)); err != nil {
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

	if err := p.validator.ValidateInvertedIndexConfigUpdate(
		class.InvertedIndexConfig,
		update.InvertedIndexConfig); err != nil {
		return nil, fmt.Errorf("inverted index config: %w", err)
	}

	return update, nil
}

func hasTargetVectors(class *models.Class) bool {
	return len(class.VectorConfig) > 0
}

func validateVectorConfigsParityAndImmutables(initial, updated *models.Class) error {
	initialVecCount := len(initial.VectorConfig)
	updatedVecCount := len(updated.VectorConfig)

	// no cfgs for target vectors
	if initialVecCount == 0 && updatedVecCount == 0 {
		return nil
	}
	// no cfgs for target vectors in initial
	if initialVecCount == 0 && updatedVecCount > 0 {
		return fmt.Errorf("additional configs for vectors")
	}
	// no cfgs for target vectors in updated
	if initialVecCount > 0 && updatedVecCount == 0 {
		return fmt.Errorf("missing configs for vectors")
	}

	// matching cfgs on both sides
	for vecName := range initial.VectorConfig {
		if _, ok := updated.VectorConfig[vecName]; !ok {
			return fmt.Errorf("missing config for vector %q", vecName)
		}
	}

	if initialVecCount != updatedVecCount {
		for vecName := range updated.VectorConfig {
			if _, ok := initial.VectorConfig[vecName]; !ok {
				return fmt.Errorf("additional config for vector %q", vecName)
			}
		}
		// fallback, error should be returned in loop
		return fmt.Errorf("number of configs for vectors does not match")
	}

	// compare matching cfgs
	for vecName, initialCfg := range initial.VectorConfig {
		updatedCfg := updated.VectorConfig[vecName]

		// immutable vector type
		if initialCfg.VectorIndexType != updatedCfg.VectorIndexType {
			return fmt.Errorf("vector index type of vector %q is immutable: attempted change from %q to %q",
				vecName, initialCfg.VectorIndexType, updatedCfg.VectorIndexType)
		}

		// immutable vectorizer
		if imap, ok := initialCfg.Vectorizer.(map[string]interface{}); ok && len(imap) == 1 {
			umap, ok := updatedCfg.Vectorizer.(map[string]interface{})
			if !ok || len(umap) != 1 {
				return fmt.Errorf("invalid vectorizer config for vector %q", vecName)
			}

			ivectorizer := ""
			for k := range imap {
				ivectorizer = k
			}
			uvectorizer := ""
			for k := range umap {
				uvectorizer = k
			}

			if ivectorizer != uvectorizer {
				return fmt.Errorf("vectorizer of vector %q is immutable: attempted change from %q to %q",
					vecName, ivectorizer, uvectorizer)
			}
		}
	}
	return nil
}

func asVectorIndexConfigs(c *models.Class) map[string]schemaConfig.VectorIndexConfig {
	if c.VectorConfig == nil {
		return nil
	}

	cfgs := map[string]schemaConfig.VectorIndexConfig{}
	for vecName := range c.VectorConfig {
		cfgs[vecName] = c.VectorConfig[vecName].VectorIndexConfig.(schemaConfig.VectorIndexConfig)
	}
	return cfgs
}

func asVectorIndexConfig(c *models.Class) schemaConfig.VectorIndexConfig {
	validCfg, ok := c.VectorIndexConfig.(schemaConfig.VectorIndexConfig)
	if !ok {
		return nil
	}

	return validCfg
}

func validateShardingConfig(current, update *models.Class, mtEnabled bool) error {
	if mtEnabled {
		return nil
	}
	first, ok := current.ShardingConfig.(shardingConfig.Config)
	if !ok {
		return fmt.Errorf("current config is not well-formed")
	}
	second, ok := update.ShardingConfig.(shardingConfig.Config)
	if !ok {
		return fmt.Errorf("updated config is not well-formed")
	}
	if first.DesiredCount != second.DesiredCount {
		return fmt.Errorf("re-sharding not supported yet: shard count is immutable: "+
			"attempted change from \"%d\" to \"%d\"", first.DesiredCount,
			second.DesiredCount)
	}

	if first.VirtualPerPhysical != second.VirtualPerPhysical {
		return fmt.Errorf("virtual shards per physical is immutable: "+
			"attempted change from \"%d\" to \"%d\"", first.VirtualPerPhysical,
			second.VirtualPerPhysical)
	}
	return nil
}
