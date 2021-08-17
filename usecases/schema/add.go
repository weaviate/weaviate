//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

// AddClass to the schema
func (m *Manager) AddClass(ctx context.Context, principal *models.Principal,
	class *models.Class) error {
	err := m.authorizer.Authorize(principal, "create", "schema/objects")
	if err != nil {
		return err
	}

	return m.addClass(ctx, principal, class)
}

func (m *Manager) addClass(ctx context.Context, principal *models.Principal,
	class *models.Class) error {
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

	shardState, err := sharding.InitState(class.Class, class.ShardingConfig.(sharding.Config))
	if err != nil {
		return errors.Wrap(err, "init sharding state")
	}

	// tx, err := m.cluster.BeginTransaction(ctx, AddClass,
	// 	AddClassPayload{class, state})
	// if err != nil {
	// 	// possible causes for errors could be nodes down (we expect every node to
	// 	// the up for a schema transaction) or concurrent transactions from other
	// 	// nodes
	// 	return errors.Wrap(err, "open cluster-wide transaction")
	// }

	// if err := m.cluster.CommitTransaction(ctx, tx); err != nil {
	// 	return errors.Wrap(err, "commit cluster-wide transaction")
	// }

	semanticSchema := m.state.ObjectSchema
	semanticSchema.Classes = append(semanticSchema.Classes, class)

	m.state.ShardingState[class.Class] = shardState
	err = m.saveSchema(ctx)
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

	m.moduleConfig.SetClassDefaults(class)
}

func (m *Manager) validateCanAddClass(ctx context.Context, principal *models.Principal, class *models.Class) error {
	// First check if there is a name clash.
	err := m.validateClassNameUniqueness(class.Class)
	if err != nil {
		return err
	}

	err = m.validateClassName(ctx, class.Class)
	if err != nil {
		return err
	}

	// Check properties
	foundNames := map[string]bool{}
	for _, property := range class.Properties {
		err = m.validatePropertyName(ctx, class.Class, property.Name,
			property.ModuleConfig)
		if err != nil {
			return err
		}

		if foundNames[property.Name] {
			return fmt.Errorf("name '%s' already in use as a property name for class '%s'", property.Name, class.Class)
		}

		foundNames[property.Name] = true

		// Validate data type of property.
		schema, err := m.GetSchema(principal)
		if err != nil {
			return err
		}

		_, err = (&schema).FindPropertyDataType(property.DataType)
		if err != nil {
			return fmt.Errorf("property '%s': invalid dataType: %v", property.Name, err)
		}
	}

	err = m.validateVectorSettings(ctx, class)
	if err != nil {
		return err
	}

	err = m.moduleConfig.ValidateClass(ctx, class)
	if err != nil {
		return err
	}

	// all is fine!
	return nil
}

func (m *Manager) parseVectorIndexConfig(ctx context.Context,
	class *models.Class) error {
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
	class *models.Class) error {
	parsed, err := sharding.ParseConfig(class.ShardingConfig)
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
