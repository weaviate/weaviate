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

package schema

import (
	"maps"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type TenantTransition struct {
	Class        string
	Tenant       string
	CloudAttempt uint64
	Transition   *sharding.Transition
}

func (s *SchemaManager) RunningTransitions() []TenantTransition {
	return s.schema.runningTransitions()
}

// runningTransitions holds only per-class locks, so a class add or delete does not wait out the walk.
func (s *schema) runningTransitions() []TenantTransition {
	s.mu.RLock()
	classes := maps.Clone(s.classes)
	s.mu.RUnlock()

	var running []TenantTransition
	for class, meta := range classes {
		meta.RLock()
		for name, p := range meta.Sharding.Physical {
			if p.Transition != nil {
				running = append(running, TenantTransition{
					Class: class, Tenant: name, CloudAttempt: p.CloudAttempt,
					Transition: p.Transition.DeepCopy(),
				})
			}
		}
		meta.RUnlock()
	}
	return running
}

func hasCloudCopy(t *models.Tenant) bool {
	switch t.ActivityStatus {
	case models.TenantActivityStatusFROZEN, models.TenantActivityStatusFREEZING, models.TenantActivityStatusUNFREEZING:
		return true
	default:
		return false
	}
}
