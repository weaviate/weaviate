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

// tenantVersionSM serves a different version from the query and from the
// activation, so a test can tell which one survived to the caller.
type tenantVersionSM struct {
	*fakeSchemaManager
	statuses          map[string]string
	queriedVersion    uint64
	activationVersion uint64
	updateCalls       int
}

func (s *tenantVersionSM) QueryTenantsShards(class string, tenants ...string) (map[string]string, uint64, error) {
	out := make(map[string]string, len(tenants))
	for _, tenant := range tenants {
		if status, ok := s.statuses[tenant]; ok {
			out[tenant] = status
			continue
		}
		out[tenant] = models.TenantActivityStatusHOT
	}
	return out, s.queriedVersion, nil
}

func (s *tenantVersionSM) UpdateTenants(_ context.Context, _ string,
	_ *command.UpdateTenantsRequest,
) (uint64, error) {
	s.updateCalls++
	return s.activationVersion, nil
}

// Every write path waits on this version before resolving the shard. AddTenants
// bumps ShardVersion and never ClassVersion, so the class version the caller
// already holds cannot cover tenant creation: dropping this one leaves
// WaitForUpdate with nothing to wait for, and a follower that has not applied
// the tenant answers the write with "shard not found".
// Reproduces https://github.com/weaviate/weaviate/issues/12632.
func TestEnsureTenantActiveForWriteKeepsTheLeaderVersion(t *testing.T) {
	const (
		className  = "TestClass"
		tenantName = "tenant1"
	)

	tests := []struct {
		name              string
		autoActivation    bool
		status            string
		queriedVersion    uint64
		activationVersion uint64
		wantVersion       uint64
		wantUpdateCalls   int
	}{
		{
			// The reported case: nothing to activate, so the queried version used to
			// be discarded along with the activation's.
			name:            "a tenant already HOT still reports the queried version",
			autoActivation:  true,
			status:          models.TenantActivityStatusHOT,
			queriedVersion:  42,
			wantVersion:     42,
			wantUpdateCalls: 0,
		},
		{
			name:              "a tenant that had to be activated reports the activation version",
			autoActivation:    true,
			status:            models.TenantActivityStatusCOLD,
			queriedVersion:    42,
			activationVersion: 43,
			wantVersion:       43,
			wantUpdateCalls:   1,
		},
		{
			name:              "an activation behind the query keeps the query's version",
			autoActivation:    true,
			status:            models.TenantActivityStatusCOLD,
			queriedVersion:    42,
			activationVersion: 7,
			wantVersion:       42,
			wantUpdateCalls:   1,
		},
		{
			// Negative control: this arm never reached the activation code.
			name:            "without auto-activation the queried version is returned",
			autoActivation:  false,
			status:          models.TenantActivityStatusHOT,
			queriedVersion:  42,
			wantVersion:     42,
			wantUpdateCalls: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			class := &models.Class{
				Class: className,
				MultiTenancyConfig: &models.MultiTenancyConfig{
					Enabled:              true,
					AutoTenantActivation: tc.autoActivation,
				},
			}

			fake := &fakeSchemaManager{}
			fake.On("Read", className, mock.Anything).
				Run(func(args mock.Arguments) {
					reader := args.Get(1).(func(*models.Class, *sharding.State) error)
					_ = reader(class, nil)
				}).
				Return(nil).Maybe()

			sm := &tenantVersionSM{
				fakeSchemaManager: fake,
				statuses:          map[string]string{tenantName: tc.status},
				queriedVersion:    tc.queriedVersion,
				activationVersion: tc.activationVersion,
			}

			m := &Manager{Handler: Handler{schemaManager: sm, schemaReader: fake}}

			version, err := m.EnsureTenantActiveForWrite(context.Background(), className, tenantName)
			require.NoError(t, err)
			require.Equal(t, tc.wantVersion, version,
				"a version that is too low makes WaitForUpdate return early and the "+
					"write races tenant creation")
			require.Equal(t, tc.wantUpdateCalls, sm.updateCalls, "activation calls")
		})
	}
}
