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

package schema

import (
	"context"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func (m *Manager) repairSchema(ctx context.Context, remote *State) error {
	m.logger.WithField("action", "repair_schema").
		Info("Attempting to repair schema")
	before := time.Now()

	local := &m.schemaCache.State
	if local.ObjectSchema == nil || remote.ObjectSchema == nil {
		return fmt.Errorf("nil schema found, local: %v, remote: %v",
			local.ObjectSchema, remote.ObjectSchema)
	}

	if err := (&clusterSyncRepairs{}).start(ctx, m, local, remote); err != nil {
		return err
	}

	m.logger.WithField("action", "repair_schema").
		Infof("Schema repair complete, took %s", time.Since(before))
	return nil
}

func (m *Manager) repairLocalClasses(ctx context.Context, classes []*models.Class) error {
	for _, class := range classes {
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
	return nil
}

func (m *Manager) repairLocalProperties(properties map[string][]*models.Property) error {
	for class, props := range properties {
		for _, prop := range props {
			_, err := m.schemaCache.addProperty(class, prop)
			if err != nil {
				return fmt.Errorf(`add prop "%s.%s" locally: %w`, class, prop.Name, err)
			}
		}
	}
	return nil
}

func (m *Manager) repairLocalTenants(tenantsByClass map[string][]sharding.Physical) {
	m.schemaCache.LockGuard(func() {
		for class, tenants := range tenantsByClass {
			ss := m.schemaCache.State.ShardingState[class]
			if ss.Physical == nil {
				ss.Physical = make(map[string]sharding.Physical, len(tenants))
			}
			for _, tenant := range tenants {
				ss.Physical[tenant.Name] = tenant
			}
		}
	})
}

func (m *Manager) repairRemoteClasses(ctx context.Context, classes []*models.Class) error {
	for _, class := range classes {
		err := m.schemaCache.RLockGuard(func() error {
			err := m.remoteRepairTx(ctx, RepairClass,
				AddClassPayload{class, m.schemaCache.ShardingState[class.Class]})
			if err != nil {
				return fmt.Errorf("repair remote classes: %w", err)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) repairRemoteProperties(ctx context.Context, properties map[string][]*models.Property) error {
	for class, props := range properties {
		for _, prop := range props {
			err := m.remoteRepairTx(ctx, RepairProperty, AddPropertyPayload{class, prop})
			if err != nil {
				return fmt.Errorf("repair remote props: %w", err)
			}
		}
	}
	return nil
}

func (m *Manager) repairRemoteTenants(ctx context.Context, tenants map[string][]sharding.Physical) error {
	for class, phys := range tenants {
		request := AddTenantsPayload{
			Class:   class,
			Tenants: make([]TenantCreate, 0, len(phys)),
		}
		for _, p := range phys {
			request.Tenants = append(request.Tenants, TenantCreate{
				Name:   p.Name,
				Nodes:  p.BelongsToNodes,
				Status: schema.ActivityStatus(models.TenantActivityStatusHOT),
			})
		}
		err := m.remoteRepairTx(ctx, RepairTenant, request)
		if err != nil {
			return fmt.Errorf("repair remote tenants: %w", err)
		}
	}
	return nil
}

func (m *Manager) remoteRepairTx(ctx context.Context,
	txType cluster.TransactionType, payload interface{},
) error {
	tx, err := m.cluster.BeginTransaction(ctx, txType, payload, DefaultTxTTL)
	if err != nil {
		return fmt.Errorf("open cluster-wide transaction: %w", err)
	}

	if err = m.cluster.CommitWriteTransaction(ctx, tx); err != nil {
		return fmt.Errorf("not every node was able to commit: %w", err)
	}

	return nil
}

type repairSet struct {
	classes []*models.Class
	props   map[string][]*models.Property
	tenants map[string][]sharding.Physical
}

func newRepairSet() repairSet {
	return repairSet{
		props:   make(map[string][]*models.Property),
		tenants: make(map[string][]sharding.Physical),
	}
}

type clusterSyncRepairs struct {
	local, remote repairSet
}

func (repairs *clusterSyncRepairs) init(local, remote *State) {
	local2Remote := comparisonUnit{
		anomaly:    newComparisonState(local),
		comparator: newComparisonState(remote),
	}
	repairs.local = determineRepairsNeeded(local2Remote)

	remote2Local := comparisonUnit{
		anomaly:    newComparisonState(remote),
		comparator: newComparisonState(local),
	}
	repairs.remote = determineRepairsNeeded(remote2Local)
}

func (repairs *clusterSyncRepairs) start(ctx context.Context, m *Manager, local, remote *State) error {
	repairs.init(local, remote)
	if err := repairs.startLocal(ctx, m); err != nil {
		return err
	}
	return repairs.startRemote(ctx, m)
}

func (repairs *clusterSyncRepairs) startLocal(ctx context.Context, m *Manager) error {
	if err := m.repairLocalClasses(ctx, repairs.local.classes); err != nil {
		return err
	}
	if err := m.repairLocalProperties(repairs.local.props); err != nil {
		return err
	}
	m.repairLocalTenants(repairs.local.tenants)
	return nil
}

func (repairs *clusterSyncRepairs) startRemote(ctx context.Context, m *Manager) error {
	m.cluster.StartAcceptIncoming()
	if err := m.repairRemoteClasses(ctx, repairs.remote.classes); err != nil {
		return err
	}
	if err := m.repairRemoteProperties(ctx, repairs.remote.props); err != nil {
		return err
	}
	return m.repairRemoteTenants(ctx, repairs.remote.tenants)
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

func determineRepairsNeeded(states comparisonUnit) repairSet {
	repairs := newRepairSet()
	anomaly, comparator := states.anomaly, states.comparator
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

func classSliceToMap(cs []*models.Class) map[string]*models.Class {
	m := make(map[string]*models.Class, len(cs))
	for _, c := range cs {
		m[c.Class] = c
	}
	return m
}
