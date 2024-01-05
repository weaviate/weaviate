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
		class.ReplicationConfig = &models.ReplicationConfig{Factor: int64(globalCfg.MinimumFactor)}
		return nil
	}

	if class.ReplicationConfig.Factor > 0 && class.ReplicationConfig.Factor < int64(globalCfg.MinimumFactor) {
		return fmt.Errorf("invalid replication factor: setup requires a minimum replication factor of %d: got %d",
			globalCfg.MinimumFactor, class.ReplicationConfig.Factor)
	}

	if class.ReplicationConfig.Factor < 1 {
		class.ReplicationConfig.Factor = int64(globalCfg.MinimumFactor)
	}

	return nil
}

func ValidateConfigUpdate(old, updated *models.Class, nodeCounter nodeCounter) error {
	// This is not possible if schema is being updated via by a client.
	// But for a test object that wasn't created by a client, it is.
	if old.ReplicationConfig == nil {
		old.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
	}

	if updated.ReplicationConfig == nil {
		updated.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
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
