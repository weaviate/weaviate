//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
)

type nodeCounter interface {
	NodeCount() int
}

func ValidateConfig(class *models.Class, globalCfg replication.GlobalConfig) error {
	if class.ReplicationConfig == nil {
		class.ReplicationConfig = &models.ReplicationConfig{
			Factor:           int64(globalCfg.MinimumFactor),
			DeletionStrategy: globalCfg.DeletionStrategy,
		}
		return nil
	}

	// A negative replication factor is semantically invalid. Previously this
	// path silently normalized any Factor < 1 to MinimumFactor, which made
	// misconfiguration invisible (HTTP 200 with a stored value different
	// from the requested one). See issue #11401.
	if class.ReplicationConfig.Factor < 0 {
		return fmt.Errorf("invalid replication factor: must be >= 1, got %d",
			class.ReplicationConfig.Factor)
	}

	if class.ReplicationConfig.Factor > 0 && class.ReplicationConfig.Factor < int64(globalCfg.MinimumFactor) {
		return fmt.Errorf("invalid replication factor: setup requires a minimum replication factor of %d: got %d",
			globalCfg.MinimumFactor, class.ReplicationConfig.Factor)
	}

	if globalCfg.MaximumFactor > 0 && class.ReplicationConfig.Factor > int64(globalCfg.MaximumFactor) {
		return fmt.Errorf("invalid replication factor: setup caps replication at %d: got %d",
			globalCfg.MaximumFactor, class.ReplicationConfig.Factor)
	}

	// Factor == 0 means "use the configured default". This is preserved for
	// clients that send an empty ReplicationConfig object (the JSON zero
	// value is 0).
	if class.ReplicationConfig.Factor < 1 {
		class.ReplicationConfig.Factor = int64(globalCfg.MinimumFactor)
	}

	if globalCfg.DeletionStrategy != "" {
		class.ReplicationConfig.DeletionStrategy = globalCfg.DeletionStrategy
	}

	return nil
}

func ValidateConfigUpdate(old, updated *models.Class, nodeCounter nodeCounter) error {
	// This is not possible if schema is being updated via by a client.
	// But for a test object that wasn't created by a client, it is.
	if old.ReplicationConfig == nil || old.ReplicationConfig.Factor == 0 {
		old.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
	}

	if updated.ReplicationConfig == nil {
		updated.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
	}

	// Reject negative replication factors explicitly. Previously this path
	// fell through to the scale check, where a negative factor would never
	// trigger the node-count comparison and the bad value would be stored
	// silently. See issue #11401 (same root cause as ValidateConfig).
	if updated.ReplicationConfig.Factor < 0 {
		return fmt.Errorf("invalid replication factor: must be >= 1, got %d",
			updated.ReplicationConfig.Factor)
	}

	if old.ReplicationConfig.Factor != updated.ReplicationConfig.Factor {
		nc := nodeCounter.NodeCount()
		if int(updated.ReplicationConfig.Factor) > nc {
			return fmt.Errorf("cannot scale to %d replicas, cluster has only %d nodes",
				updated.ReplicationConfig.Factor, nc)
		}
	}

	return nil
}
