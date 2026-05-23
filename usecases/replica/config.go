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

// validateReplicationFactor rejects semantically invalid (negative) values
// up front. Previously both ValidateConfig and ValidateConfigUpdate fell
// through to a silent-normalize branch that coerced any Factor < 1 to
// MinimumFactor, making misconfiguration invisible (HTTP 200 with a stored
// value different from the requested one). See issue #11401.
//
// Factor == 0 is intentionally accepted here because callers downstream
// treat it as "use the configured default" (it is the JSON zero value when
// a client omits the field).
func validateReplicationFactor(factor int64) error {
	if factor < 0 {
		return fmt.Errorf("invalid replication factor: must be >= 1, got %d", factor)
	}
	return nil
}

func ValidateConfig(class *models.Class, globalCfg replication.GlobalConfig) error {
	if class.ReplicationConfig == nil {
		class.ReplicationConfig = &models.ReplicationConfig{
			Factor:           int64(globalCfg.MinimumFactor),
			DeletionStrategy: globalCfg.DeletionStrategy,
		}
		return nil
	}

	if err := validateReplicationFactor(class.ReplicationConfig.Factor); err != nil {
		return err
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

	if err := validateReplicationFactor(updated.ReplicationConfig.Factor); err != nil {
		return err
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
