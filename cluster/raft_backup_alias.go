//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
)

// RestoreClassAlias restores aliases for a class from a backup descriptor.
// It is part of the schema.SchemaManager interface used by the backup coordinator.
func (s *Raft) RestoreClassAlias(ctx context.Context, d *backup.ClassDescriptor, m map[string]string, overwriteAlias bool) error {
	if !d.AliasesIncluded || len(d.Aliases) == 0 {
		// No aliases to restore
		return nil
	}

	aliases := make([]*models.Alias, 0)
	if err := json.Unmarshal(d.Aliases, &aliases); err != nil {
		return fmt.Errorf("unmarshal aliases: %w", err)
	}

	// Get the class to restore aliases for
	var class models.Class
	if err := json.Unmarshal(d.Schema, &class); err != nil {
		return fmt.Errorf("unmarshal class schema: %w", err)
	}

	// Apply node mapping to class name if needed
	className := class.Class
	if newNodeName, ok := m[className]; ok {
		className = newNodeName
	}

	// Restore each alias
	for _, alias := range aliases {
		// Check if alias already exists
		existingAlias, err := s.GetAlias(ctx, alias.Alias)
		aliasExists := err == nil && existingAlias != nil

		if aliasExists {
			// Alias exists
			if !overwriteAlias {
				// Skip if we don't want to overwrite
				continue
			}
			// Delete existing alias before creating new one
			if _, err := s.DeleteAlias(ctx, alias.Alias); err != nil {
				return fmt.Errorf("failed to restore alias for class: delete alias %s failed: %w", alias.Alias, err)
			}
		} else if err != nil && !errors.Is(err, schema.ErrAliasNotFound) {
			// If error is not "alias not found", it's a real error
			return fmt.Errorf("failed to check if alias exists: %w", err)
		}

		// Create the alias pointing to the (possibly mapped) class
		classForAlias := &models.Class{Class: className}
		if _, err := s.CreateAlias(ctx, alias.Alias, classForAlias); err != nil {
			return fmt.Errorf("failed to restore alias for class: create alias %s failed: %w", alias.Alias, err)
		}
	}

	return nil
}

// func (h *Handler) RestoreClass(ctx context.Context, d *backup.ClassDescriptor, m map[string]string, overwriteAlias bool) error {
// 	// get schema and sharding state
// 	class := &models.Class{}
// 	if err := json.Unmarshal(d.Schema, &class); err != nil {
// 		return fmt.Errorf("unmarshal class schema: %w", err)
// 	}
// 	var shardingState sharding.State
// 	if d.ShardingState != nil {
// 		err := json.Unmarshal(d.ShardingState, &shardingState)
// 		if err != nil {
// 			return fmt.Errorf("unmarshal sharding state: %w", err)
// 		}
// 	}

// 	aliases := make([]*models.Alias, 0)
// 	if d.AliasesIncluded {
// 		if err := json.Unmarshal(d.Aliases, &aliases); err != nil {
// 			return fmt.Errorf("unmarshal aliases: %w", err)
// 		}
// 	}

// 	metric, err := monitoring.GetMetrics().BackupRestoreClassDurations.GetMetricWithLabelValues(class.Class)
// 	if err == nil {
// 		timer := prometheus.NewTimer(metric)
// 		defer timer.ObserveDuration()
// 	}

// 	class.Class = schema.UppercaseClassName(class.Class)
// 	class.Properties = schema.LowercaseAllPropertyNames(class.Properties)

// 	if err := h.setClassDefaults(class, h.config.Replication); err != nil {
// 		return err
// 	}

// 	// no validation of reference for restore
// 	classGetterWrapper := func(name string) (*models.Class, error) {
// 		return h.schemaReader.ReadOnlyClass(name), nil
// 	}

// 	err = h.validateClassInvariants(ctx, class, classGetterWrapper, true)
// 	if err != nil {
// 		return err
// 	}
// 	// migrate only after validation in completed
// 	h.migrateClassSettings(class)

// 	if err := h.parser.ParseClass(class); err != nil {
// 		return err
// 	}

// 	shardingState.MigrateFromOldFormat()
// 	err = shardingState.MigrateShardingStateReplicationFactor()
// 	if err != nil {
// 		return fmt.Errorf("error while migrating replication factor: %w", err)
// 	}
// 	shardingState.ApplyNodeMapping(m)
// 	_, err = h.schemaManager.RestoreClass(ctx, class, &shardingState)
// 	if err != nil {
// 		return fmt.Errorf("error when trying to restore class: %w", err)
// 	}

// 	for _, alias := range aliases {
// 		resolved := h.schemaReader.ResolveAlias(alias.Alias)

// 		// Alias do exist and don't want to overwrite
// 		if resolved != "" && !overwriteAlias {
// 			continue
// 		}

// 		if resolved != "" {
// 			_, err := h.schemaManager.DeleteAlias(ctx, alias.Alias)
// 			if err != nil {
// 				return fmt.Errorf("failed to restore alias for class: delete alias %s failed: %w", alias.Alias, err)
// 			}
// 		}

// 		_, err := h.schemaManager.CreateAlias(ctx, alias.Alias, class)
// 		if err != nil {
// 			return fmt.Errorf("failed to restore alias for class: create alias %s failed: %w", alias.Alias, err)
// 		}
// 	}

// 	return nil
// }
