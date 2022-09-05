//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

// AddClass to the schema
func (m *Manager) AddClass(ctx context.Context, principal *models.Principal,
	class *models.Class,
) error {
	err := m.authorizer.Authorize(principal, "create", "schema/objects")
	if err != nil {
		return err
	}

	return m.addClass(ctx, principal, class)
}

func (m *Manager) RestoreClass(ctx context.Context, principal *models.Principal,
	d *backup.ClassDescriptor,
) error {
	// get schema and sharding state
	class := &models.Class{}
	if err := json.Unmarshal(d.Schema, &class); err != nil {
		return fmt.Errorf("marshal class schema: %w", err)
	}
	var shardingState *sharding.State
	if d.ShardingState != nil {
		shardingState = &sharding.State{}
		err := json.Unmarshal(d.ShardingState, shardingState)
		if err != nil {
			return fmt.Errorf("marshal sharding state: %w", err)
		}
	}

	m.Lock()
	defer m.Unlock()
	timer := prometheus.NewTimer(monitoring.GetMetrics().SnapshotRestoreClassDurations.WithLabelValues(class.Class))
	defer timer.ObserveDuration()

	class.Class = upperCaseClassName(class.Class)
	class.Properties = lowerCaseAllPropertyNames(class.Properties)
	m.setClassDefaults(class)

	err := m.validateCanAddClass(ctx, principal, class)
	if err != nil {
		return err
	}

	err = m.parseShardingConfig(ctx, class)
	if err != nil {
		return err
	}

	err = m.parseVectorIndexConfig(ctx, class)
	if err != nil {
		return err
	}

	err = m.invertedConfigValidator(class.InvertedIndexConfig)
	if err != nil {
		return err
	}

	semanticSchema := m.state.ObjectSchema
	semanticSchema.Classes = append(semanticSchema.Classes, class)

	m.state.ShardingState[class.Class] = shardingState
	m.state.ShardingState[class.Class].SetLocalName(m.clusterState.LocalName())
	err = m.saveSchema(ctx)
	if err != nil {
		return err
	}

	out := m.migrator.AddClass(ctx, class, shardingState)
	return out
}

func (m *Manager) addClass(ctx context.Context, principal *models.Principal,
	class *models.Class,
) error {
	m.Lock()
	defer m.Unlock()

	class.Class = upperCaseClassName(class.Class)
	class.Properties = lowerCaseAllPropertyNames(class.Properties)
	m.setClassDefaults(class)

	err := m.validateCanAddClass(ctx, principal, class)
	if err != nil {
		return err
	}

	err = m.parseShardingConfig(ctx, class)
	if err != nil {
		return err
	}

	err = m.parseVectorIndexConfig(ctx, class)
	if err != nil {
		return err
	}

	err = m.invertedConfigValidator(class.InvertedIndexConfig)
	if err != nil {
		return err
	}

	shardState, err := sharding.InitState(class.Class,
		class.ShardingConfig.(sharding.Config), m.clusterState)
	if err != nil {
		return errors.Wrap(err, "init sharding state")
	}

	tx, err := m.cluster.BeginTransaction(ctx, AddClass,
		AddClassPayload{class, shardState})
	if err != nil {
		// possible causes for errors could be nodes down (we expect every node to
		// the up for a schema transaction) or concurrent transactions from other
		// nodes
		return errors.Wrap(err, "open cluster-wide transaction")
	}

	if err := m.cluster.CommitTransaction(ctx, tx); err != nil {
		return errors.Wrap(err, "commit cluster-wide transaction")
	}

	return m.addClassApplyChanges(ctx, class, shardState)
}

func (m *Manager) addClassApplyChanges(ctx context.Context, class *models.Class,
	shardState *sharding.State,
) error {
	semanticSchema := m.state.ObjectSchema
	semanticSchema.Classes = append(semanticSchema.Classes, class)

	m.state.ShardingState[class.Class] = shardState
	err := m.saveSchema(ctx)
	if err != nil {
		return err
	}

	return m.migrator.AddClass(ctx, class, shardState)
	// TODO gh-846: Rollback state upate if migration fails
}

func (m *Manager) setClassDefaults(class *models.Class) {
	if class.Vectorizer == "" {
		class.Vectorizer = m.config.DefaultVectorizerModule
	}

	if class.VectorIndexType == "" {
		class.VectorIndexType = "hnsw"
	}

	if class.InvertedIndexConfig == nil {
		class.InvertedIndexConfig = &models.InvertedIndexConfig{}
	}

	if class.InvertedIndexConfig.CleanupIntervalSeconds == 0 {
		class.InvertedIndexConfig.CleanupIntervalSeconds = config.DefaultCleanupIntervalSeconds
	}

	if class.InvertedIndexConfig.Bm25 == nil {
		class.InvertedIndexConfig.Bm25 = &models.BM25Config{
			K1: config.DefaultBM25k1,
			B:  config.DefaultBM25b,
		}
	}

	if class.InvertedIndexConfig.Stopwords == nil {
		class.InvertedIndexConfig.Stopwords = &models.StopwordConfig{
			Preset: stopwords.EnglishPreset,
		}
	}

	for _, prop := range class.Properties {
		m.setPropertyDefaults(prop)
	}

	m.moduleConfig.SetClassDefaults(class)
}

func (m *Manager) setPropertyDefaults(prop *models.Property) {
	m.setPropertyDefaultTokenization(prop)
}

func (m *Manager) setPropertyDefaultTokenization(prop *models.Property) {
	// already set, no default needed
	if prop.Tokenization != "" {
		return
	}

	// set only for tokenization supporting data types
	if len(prop.DataType) == 1 {
		switch prop.DataType[0] {
		case string(schema.DataTypeString), string(schema.DataTypeStringArray),
			string(schema.DataTypeText), string(schema.DataTypeTextArray):
			prop.Tokenization = models.PropertyTokenizationWord
		}
	}
}

func (m *Manager) validateCanAddClass(ctx context.Context, principal *models.Principal, class *models.Class) error {
	if err := m.validateClassNameUniqueness(class.Class); err != nil {
		return err
	}

	if err := m.validateClassName(ctx, class.Class); err != nil {
		return err
	}

	existingPropertyNames := map[string]bool{}
	for _, property := range class.Properties {
		if err := m.validateProperty(property, class, existingPropertyNames, principal); err != nil {
			return err
		}
		existingPropertyNames[property.Name] = true
	}

	if err := m.validateVectorSettings(ctx, class); err != nil {
		return err
	}

	if err := m.moduleConfig.ValidateClass(ctx, class); err != nil {
		return err
	}

	// all is fine!
	return nil
}

func (m *Manager) validateProperty(property *models.Property, class *models.Class, existingPropertyNames map[string]bool, principal *models.Principal) error {
	if _, err := schema.ValidatePropertyName(property.Name); err != nil {
		return err
	}

	if err := schema.ValidateReservedPropertyName(property.Name); err != nil {
		return err
	}

	if existingPropertyNames[property.Name] {
		return fmt.Errorf("name '%s' already in use as a property name for class '%s'", property.Name, class.Class)
	}

	// Validate data type of property.
	schema, err := m.GetSchema(principal)
	if err != nil {
		return err
	}

	propertyDataType, err := (&schema).FindPropertyDataType(property.DataType)
	if err != nil {
		return fmt.Errorf("property '%s': invalid dataType: %v", property.Name, err)
	}

	if err := validatePropertyTokenization(property.Tokenization, propertyDataType); err != nil {
		return err
	}

	// all is fine!
	return nil
}

func (m *Manager) parseVectorIndexConfig(ctx context.Context,
	class *models.Class,
) error {
	if class.VectorIndexType != "hnsw" {
		return errors.Errorf(
			"parse vector index config: unsupported vector index type: %q",
			class.VectorIndexType)
	}

	parsed, err := m.hnswConfigParser(class.VectorIndexConfig)
	if err != nil {
		return errors.Wrap(err, "parse vector index config")
	}

	class.VectorIndexConfig = parsed

	return nil
}

func (m *Manager) parseShardingConfig(ctx context.Context,
	class *models.Class,
) error {
	parsed, err := sharding.ParseConfig(class.ShardingConfig,
		m.clusterState.NodeCount())
	if err != nil {
		return errors.Wrap(err, "parse vector index config")
	}

	class.ShardingConfig = parsed

	return nil
}

func upperCaseClassName(name string) string {
	if len(name) < 1 {
		return name
	}

	if len(name) == 1 {
		return strings.ToUpper(name)
	}

	return strings.ToUpper(string(name[0])) + name[1:]
}

func lowerCaseAllPropertyNames(props []*models.Property) []*models.Property {
	for i, prop := range props {
		props[i].Name = lowerCaseFirstLetter(prop.Name)
	}

	return props
}

func lowerCaseFirstLetter(name string) string {
	if len(name) < 1 {
		return name
	}

	if len(name) == 1 {
		return strings.ToLower(name)
	}

	return strings.ToLower(string(name[0])) + name[1:]
}
