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

package sharding

import (
	"fmt"
)

type nodeCounter interface {
	NodeCount() int
}

func ValidateConfigUpdate(old, updated Config, nodeCounter nodeCounter) error {
	if old.DesiredCount != updated.DesiredCount {
		return fmt.Errorf("re-sharding not supported yet: shard count is immutable: "+
			"attempted change from \"%d\" to \"%d\"", old.DesiredCount,
			updated.DesiredCount)
	}

	if old.VirtualPerPhysical != updated.VirtualPerPhysical {
		return fmt.Errorf("virtual shards per physical is immutable: "+
			"attempted change from \"%d\" to \"%d\"", old.VirtualPerPhysical,
			updated.VirtualPerPhysical)
	}

	if old.Replicas != updated.Replicas {
		nc := nodeCounter.NodeCount()
		if updated.Replicas > nc {
			return fmt.Errorf("cannot scale to %d replicas, cluster has only %d nodes",
				updated.Replicas, nc)
		}
	}

	return nil
}
