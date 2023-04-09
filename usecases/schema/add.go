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
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// AddClass to the schema
func (m *Manager) AddClass(ctx context.Context, principal *models.Principal,
	class *models.Class,
) error {
	err := m.Authorizer.Authorize(principal, "create", "schema/objects")
	if err != nil {
		return err
	}

	shardState, err := m.addClass(ctx, class)
	if err != nil {
		return err
	}

	// call to migrator needs to be outside the lock that is set in addClass
	return m.migrator.AddClass(ctx, class, shardState)
	// TODO gh-846: Rollback state upate if migration fails
}

func (m *Manager) RestoreClass(ctx context.Context, d *backup.ClassDescriptor) error {
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
	metric, err := monitoring.GetMetrics().BackupRestoreClassDurations.GetMetricWithLabelValues(class.Class)
	if err == nil {
		timer := prometheus.NewTimer(metric)
		defer timer.ObserveDuration()
	}

	class.Class = schema.UppercaseClassName(class.Class)
	class.Properties = schema.LowercaseAllPropertyNames(class.Properties)
	m.setClassDefaults(class)

	err = m.validateCanAddClass(ctx, class, true)
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

	shardingState.MigrateFromOldFormat()

	m.shardingStateLock.Lock()
	m.state.ShardingState[class.Class] = shardingState
	m.state.ShardingState[class.Class].SetLocalName(m.clusterState.LocalName())
	m.shardingStateLock.Unlock()

	err = m.saveSchema(ctx)
	if err != nil {
		return err
	}

	out := m.migrator.AddClass(ctx, class, shardingState)
	return out
}

func (m *Manager) addClass(ctx context.Context, class *models.Class,
) (*sharding.State, error) {
	m.Lock()
	defer m.Unlock()

	class.Class = schema.UppercaseClassName(class.Class)
	class.Properties = schema.LowercaseAllPropertyNames(class.Properties)
	m.setClassDefaults(class)

	err := m.validateCanAddClass(ctx, class, false)
	if err != nil {
		return nil, err
	}

	err = m.parseShardingConfig(ctx, class)
	if err != nil {
		return nil, err
	}

	err = m.parseVectorIndexConfig(ctx, class)
	if err != nil {
		return nil, err
	}

	err = m.invertedConfigValidator(class.InvertedIndexConfig)
	if err != nil {
		return nil, err
	}

	shardState, err := sharding.InitState(class.Class,
		class.ShardingConfig.(sharding.Config),
		m.clusterState, class.ReplicationConfig.Factor)
	if err != nil {
		return nil, errors.Wrap(err, "init sharding state")
	}

	tx, err := m.cluster.BeginTransaction(ctx, AddClass,
		AddClassPayload{class, shardState}, DefaultTxTTL)
	if err != nil {
		// possible causes for errors could be nodes down (we expect every node to
		// the up for a schema transaction) or concurrent transactions from other
		// nodes
		return nil, errors.Wrap(err, "open cluster-wide transaction")
	}

	if err := m.cluster.CommitWriteTransaction(ctx, tx); err != nil {
		// Only log the commit error, but do not abort the changes locally. Once
		// we've told others to commit, we also need to commit ourselves!
		//
		// The idea is that if we abort our changes we are guaranteed to create an
		// inconsistency as soon as any other node honored the commit. This would
		// for example be the case in a 3-node cluster where node 1 is the
		// coordinator, node 2 honored the commit and node 3 died during the commit
		// phase.
		//
		// In this scenario it is far more desirable to make sure that node 1 and
		// node 2 stay in sync, as node 3 - who may or may not have missed the
		// update - can use a local WAL from the first TX phase to replay any
		// missing changes once it's back.
		m.logger.WithError(err).Errorf("not every node was able to commit")
	}

	if err := m.addClassApplyChanges(ctx, class, shardState); err != nil {
		return nil, err
	}
	return shardState, nil
}

func (m *Manager) addClassApplyChanges(ctx context.Context, class *models.Class,
	shardState *sharding.State,
) error {
	semanticSchema := m.state.ObjectSchema
	semanticSchema.Classes = append(semanticSchema.Classes, class)

	m.shardingStateLock.Lock()
	m.state.ShardingState[class.Class] = shardState
	m.shardingStateLock.Unlock()
	return m.saveSchema(ctx)
}

func (m *Manager) setClassDefaults(class *models.Class) {
	if class.Vectorizer == "" {
		class.Vectorizer = m.config.DefaultVectorizerModule
	}

	if class.VectorIndexType == "" {
		class.VectorIndexType = m.config.DefaultVectorIndexType
	}

	if m.config.DefaultVectorDistanceMetric != "" {
		if class.VectorIndexConfig == nil {
			class.VectorIndexConfig = map[string]interface{}{"distance": m.config.DefaultVectorDistanceMetric}
		} else if class.VectorIndexConfig.(map[string]interface{})["distance"] == nil {
			class.VectorIndexConfig.(map[string]interface{})["distance"] = m.config.DefaultVectorDistanceMetric
		}
	}

	setInvertedConfigDefaults(class)
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

func (m *Manager) validateCanAddClass(
	ctx context.Context, class *models.Class,
	relaxCrossRefValidation bool,
) error {
	if err := m.validateClassNameUniqueness(class.Class); err != nil {
		return err
	}

	if err := m.validateClassName(ctx, class.Class); err != nil {
		return err
	}

	existingPropertyNames := map[string]bool{}
	for _, property := range class.Properties {
		if err := m.validateProperty(property, class.Class, existingPropertyNames, relaxCrossRefValidation); err != nil {
			return err
		}
		existingPropertyNames[strings.ToLower(property.Name)] = true
	}

	if err := m.validateVectorSettings(ctx, class); err != nil {
		return err
	}

	if err := m.moduleConfig.ValidateClass(ctx, class); err != nil {
		return err
	}

	if err := replica.ValidateConfig(class); err != nil {
		return err
	}

	// all is fine!
	return nil
}

func (m *Manager) validateProperty(
	property *models.Property, className string,
	existingPropertyNames map[string]bool, relaxCrossRefValidation bool,
) error {
	if _, err := schema.ValidatePropertyName(property.Name); err != nil {
		return err
	}

	if err := schema.ValidateReservedPropertyName(property.Name); err != nil {
		return err
	}

	if existingPropertyNames[strings.ToLower(property.Name)] {
		return fmt.Errorf("class %q: conflict for property %q: already in use or provided multiple times", property.Name, className)
	}

	// Validate data type of property.
	sch := m.getSchema()

	propertyDataType, err := (&sch).FindPropertyDataTypeWithRefs(property.DataType,
		relaxCrossRefValidation, schema.ClassName(className))
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
	if class.VectorIndexType == "hnsw" {
		parsed, err := m.vectorConfigParser(class.VectorIndexConfig)
		if err != nil {
			return errors.Wrap(err, "parse vector index config")
		}
		class.VectorIndexConfig = parsed
	} else if class.VectorIndexType == "gemini" {
		parsed, err := m.vectorConfigParser(class.VectorIndexConfig)
		if err != nil {
			return errors.Wrap(err, "parse vector index config")
		}
		class.VectorIndexConfig = parsed
	} else {
		return errors.Errorf(
			"parse vector index config: unsupported vector index type: %q",
			class.VectorIndexType)
	}

	return nil
}

func (m *Manager) parseShardingConfig(ctx context.Context,
	class *models.Class,
) error {
	parsed, err := sharding.ParseConfig(class.ShardingConfig,
		m.clusterState.NodeCount())
	if err != nil {
		return errors.Wrap(err, "parse sharding config")
	}

	class.ShardingConfig = parsed

	return nil
}

func setInvertedConfigDefaults(class *models.Class) {
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
}
