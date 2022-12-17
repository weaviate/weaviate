package replica

import (
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
)

type nodeCounter interface {
	NodeCount() int
}

func ValidateConfig(class *models.Class) error {
	if class.Replication == nil {
		class.Replication = &models.ReplicationConfig{Factor: 1}
		return nil
	}

	if class.Replication.Factor < 1 {
		class.Replication.Factor = 1
	}

	return nil
}

func ValidateConfigUpdate(old, updated *models.Class, nodeCounter nodeCounter) error {
	// This is not possible if schema is being updated via by a client.
	// But for a test object that wasn't created by a client, it is.
	if old.Replication == nil {
		old.Replication = &models.ReplicationConfig{Factor: 1}
	}

	if updated.Replication == nil {
		updated.Replication = &models.ReplicationConfig{Factor: 1}
	}

	if old.Replication.Factor != updated.Replication.Factor {
		nc := nodeCounter.NodeCount()
		if int(updated.Replication.Factor) > nc {
			return fmt.Errorf("cannot scale to %d replicas, cluster has only %d nodes",
				updated.Replication.Factor, nc)
		}
	}

	return nil
}
