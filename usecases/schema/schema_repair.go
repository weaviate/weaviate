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
	"fmt"
	"time"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type clusterSyncRepairs struct {
	classes []*models.Class
	props   map[string][]*models.Property
	tenants map[string][]sharding.Physical
}

func newClusterSyncRepairs() clusterSyncRepairs {
	return clusterSyncRepairs{
		props:   make(map[string][]*models.Property),
		tenants: make(map[string][]sharding.Physical),
	}
}

type comparisonState struct {
	*State
	classSet map[string]*models.Class
}

func newComparisonState(state *State) comparisonState {
	return comparisonState{
		State:    state,
		classSet: classSliceToMap(state.ObjectSchema.Classes),
	}
}

type comparisonUnit struct {
	anomaly, comparator comparisonState
}

func (m *Manager) repairSchema(ctx context.Context, remote *State) error {
	m.logger.WithField("action", "repair_schema").
		Debug("Attempting to repair schema")
	before := time.Now()

	local := &m.schemaCache.State
	if local.ObjectSchema == nil || remote.ObjectSchema == nil {
		return fmt.Errorf("nil schema found, local: %v, remote: %v",
			local.ObjectSchema, remote.ObjectSchema)
	}

	local2Remote := comparisonUnit{
		anomaly:    newComparisonState(local),
		comparator: newComparisonState(remote),
	}
	localRepairs := determineRepairsNeeded(local2Remote)

	// TODO: repair remote node schemas as well
	// remote2Local := comparisonUnit{
	// 	 anomaly:    newComparisonState(remote),
	//	 comparator: newComparisonState(local),
	// }
	// remoteRepairs := determineRepairsNeeded(remote2Local)

	for _, class := range localRepairs.classes {
		shardState, err := sharding.InitState(class.Class,
			class.ShardingConfig.(sharding.Config),
			m.clusterState, class.ReplicationConfig.Factor,
			schema.MultiTenancyEnabled(class))
		if err != nil {
			return fmt.Errorf("init sharding state: %w", err)
		}
		if err = m.addClassApplyChanges(ctx, class, shardState); err != nil {
			return fmt.Errorf("add class %q locally: %w", class.Class, err)
		}
	}

	for class, props := range localRepairs.props {
		for _, prop := range props {
			_, err := m.schemaCache.addProperty(class, prop)
			if err != nil {
				return fmt.Errorf(`add prop "%s.%s" locally: %w`, class, prop.Name, err)
			}
		}
	}

	m.schemaCache.LockGuard(func() {
		for class, tenants := range localRepairs.tenants {
			ss := m.schemaCache.State.ShardingState[class]
			if ss.Physical == nil {
				ss.Physical = make(map[string]sharding.Physical)
			}
			for _, tenant := range tenants {
				ss.Physical[tenant.Name] = tenant
			}
		}
	})

	m.logger.WithField("action", "repair_schema").
		Debugf("Schema repair complete, took %s", time.Since(before))
	return nil
}

func classSliceToMap(cs []*models.Class) map[string]*models.Class {
	m := make(map[string]*models.Class)
	for _, c := range cs {
		m[c.Class] = c
	}
	return m
}

func determineRepairsNeeded(states comparisonUnit) clusterSyncRepairs {
	anomaly, comparator := states.anomaly, states.comparator

	repairs := newClusterSyncRepairs()
	for className, comparatorClass := range comparator.classSet {
		anomalyClass, found := anomaly.classSet[className]
		if !found {
			repairs.classes = append(repairs.classes, comparatorClass)
			continue
		}
		if anomalyClass.MultiTenancyConfig != nil && anomalyClass.MultiTenancyConfig.Enabled {
			anomalySS := anomaly.ShardingState[className]
			comparatorSS := comparator.ShardingState[className]
			for name, phys := range comparatorSS.Physical {
				if anomalySS.Physical == nil {
					repairs.tenants[className] = append(repairs.tenants[className], phys)
					continue
				}
				_, found = anomalySS.Physical[name]
				if !found {
					repairs.tenants[className] = append(repairs.tenants[className], phys)
				}
			}
		}
		var propsToRepair []*models.Property
		for _, comparatorProp := range comparatorClass.Properties {
			var propFound bool
			for _, anomalyProp := range anomalyClass.Properties {
				if anomalyProp.Name == comparatorProp.Name {
					propFound = true
					break
				}
			}
			if !propFound {
				propsToRepair = append(propsToRepair, comparatorProp)
			}
		}
		repairs.props[className] = propsToRepair
	}
	return repairs
}
