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
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// activationSpySM records whether the leader was queried and whether an implicit activation
// (a RAFT UpdateTenants write) was issued.
type activationSpySM struct {
	*fakeSchemaManager
	leaderStatus map[string]string

	queryCount  int
	updateCount int
}

func (a *activationSpySM) QueryTenantsShards(class string, tenants ...string) (map[string]string, uint64, error) {
	a.queryCount++
	res := make(map[string]string, len(tenants))
	for _, t := range tenants {
		if s, ok := a.leaderStatus[t]; ok {
			res[t] = s
		}
	}
	return res, 0, nil
}

func (a *activationSpySM) UpdateTenants(_ context.Context, _ string, _ *command.UpdateTenantsRequest) (uint64, error) {
	a.updateCount++
	return 0, nil
}

// TestOptimisticTenantStatus_ImplicitActivation pins who may activate a tenant.
//
// Auto tenant activation encodes a user's intent: a request arrived for this tenant, so make
// it HOT. The leader fallback therefore activates a COLD tenant, which is right for external
// requests — reads as well as writes. Internal work carries no such intent: with
// allowImplicitActivation=false the lookup stays local, so it can neither revive a tenant an
// operator deactivated nor pay for a leader query on every background cycle.
func TestOptimisticTenantStatus_ImplicitActivation(t *testing.T) {
	const (
		className  = "TestClass"
		tenantName = "tenant1"
	)

	for _, tt := range []struct {
		name string

		// allowActivation is what the caller passes: true for external request paths.
		allowActivation bool

		localStatus    string // tenant's status in the local schema; "" means absent
		autoActivation bool   // is auto tenant activation enabled on the collection
		leaderStatus   string // status the leader reports

		wantStatus      string
		wantLeaderQuery bool
		wantActivation  bool
	}{
		{
			// Happy path: local state is trusted, nothing is consulted or written.
			name:            "external request: HOT locally, no leader lookup, no activation",
			allowActivation: true,
			localStatus:     models.TenantActivityStatusHOT,
			autoActivation:  true,
			leaderStatus:    models.TenantActivityStatusHOT,
			wantStatus:      models.TenantActivityStatusHOT,
			wantLeaderQuery: false,
			wantActivation:  false,
		},
		{
			// External request semantics: a COLD tenant is activated on access.
			name:            "external request: COLD tenant is activated",
			allowActivation: true,
			localStatus:     models.TenantActivityStatusCOLD,
			autoActivation:  true,
			leaderStatus:    models.TenantActivityStatusCOLD,
			wantStatus:      models.TenantActivityStatusHOT,
			wantLeaderQuery: true,
			wantActivation:  true,
		},
		{
			// Without auto tenant activation nothing is activated, even externally.
			name:            "external request: COLD tenant stays COLD without auto activation",
			allowActivation: true,
			localStatus:     models.TenantActivityStatusCOLD,
			autoActivation:  false,
			leaderStatus:    models.TenantActivityStatusCOLD,
			wantStatus:      models.TenantActivityStatusCOLD,
			wantLeaderQuery: true,
			wantActivation:  false,
		},
		{
			// The bug this prevents: a background async-replication cycle reaching a COLD
			// tenant must NOT turn it back on, even with auto activation set. It stays local:
			// no leader query, and above all no activating RAFT write.
			name:            "internal caller: COLD tenant is never activated",
			localStatus:     models.TenantActivityStatusCOLD,
			autoActivation:  true,
			leaderStatus:    models.TenantActivityStatusCOLD,
			wantStatus:      models.TenantActivityStatusCOLD,
			wantLeaderQuery: false,
			wantActivation:  false,
		},
		{
			// A stale local read is fine here even though the leader would say otherwise:
			// internal work runs on a loop, so it just skips the tenant and the next cycle
			// picks it up. Cheaper than a leader query per shard per cycle.
			name:            "internal caller: stale local state is used as-is, no leader query",
			localStatus:     models.TenantActivityStatusCOLD,
			autoActivation:  true,
			leaderStatus:    models.TenantActivityStatusHOT,
			wantStatus:      models.TenantActivityStatusCOLD,
			wantLeaderQuery: false,
			wantActivation:  false,
		},
		{
			// Tenant not in the local schema at all: reported as absent, still no leader query.
			name:            "internal caller: unknown tenant is absent, no leader query",
			localStatus:     "",
			autoActivation:  true,
			leaderStatus:    models.TenantActivityStatusHOT,
			wantStatus:      "",
			wantLeaderQuery: false,
			wantActivation:  false,
		},
		{
			name:            "internal caller: HOT locally short-circuits the same way",
			localStatus:     models.TenantActivityStatusHOT,
			autoActivation:  true,
			leaderStatus:    models.TenantActivityStatusHOT,
			wantStatus:      models.TenantActivityStatusHOT,
			wantLeaderQuery: false,
			wantActivation:  false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			class := &models.Class{
				Class: className,
				MultiTenancyConfig: &models.MultiTenancyConfig{
					Enabled:              true,
					AutoTenantActivation: tt.autoActivation,
				},
			}

			state := &sharding.State{Physical: map[string]sharding.Physical{}}
			if tt.localStatus != "" {
				state.Physical[tenantName] = sharding.Physical{Name: tenantName, Status: tt.localStatus}
			}

			// Read serves both the local tenant probe (sharding state) and
			// AllowImplicitTenantActivation (class config).
			sr := &fakeSchemaManager{}
			sr.On("Read", className, mock.Anything).
				Run(func(args mock.Arguments) {
					reader := args.Get(1).(func(*models.Class, *sharding.State) error)
					_ = reader(class, state)
				}).
				Return(nil)

			sm := &activationSpySM{
				fakeSchemaManager: sr,
				leaderStatus:      map[string]string{tenantName: tt.leaderStatus},
			}

			m := &Manager{Handler: Handler{schemaManager: sm, schemaReader: sr}}

			status, err := m.OptimisticTenantStatus(context.Background(), className, tenantName, tt.allowActivation)
			require.NoError(t, err)
			require.Equal(t, tt.wantStatus, status[tenantName], "reported tenant status")

			if tt.wantLeaderQuery {
				require.Equal(t, 1, sm.queryCount, "leader should have been consulted")
			} else {
				require.Zero(t, sm.queryCount, "happy path must not pay for a leader lookup")
			}

			if tt.wantActivation {
				require.Equal(t, 1, sm.updateCount, "tenant should have been implicitly activated")
			} else {
				require.Zero(t, sm.updateCount,
					"no implicit activation (RAFT UpdateTenants) may be issued here")
			}
		})
	}
}
