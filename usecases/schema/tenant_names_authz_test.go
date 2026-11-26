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
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// readableTenantsAuthorizer allows READ on the listed tenants of one class
// (or everything when all is set) and records the resources it was asked about.
type readableTenantsAuthorizer struct {
	class string
	allow []string
	all   bool
}

func (a *readableTenantsAuthorizer) permitted(resource string) bool {
	if a.all {
		return true
	}
	for _, name := range a.allow {
		if resource == authorization.ShardsMetadata(a.class, name)[0] {
			return true
		}
	}
	return false
}

func (a *readableTenantsAuthorizer) Authorize(_ context.Context, _ *models.Principal, _ string, resources ...string) error {
	for _, r := range resources {
		if !a.permitted(r) {
			return assert.AnError
		}
	}
	return nil
}

func (a *readableTenantsAuthorizer) AuthorizeSilent(ctx context.Context, p *models.Principal, verb string, resources ...string) error {
	return a.Authorize(ctx, p, verb, resources...)
}

func (a *readableTenantsAuthorizer) FilterAuthorizedResources(_ context.Context, _ *models.Principal, _ string, resources ...string) ([]string, error) {
	out := []string{}
	for _, r := range resources {
		if a.permitted(r) {
			out = append(out, r)
		}
	}
	return out, nil
}

func TestGetConsistentTenants_NamesAuthorizedBeforeRead(t *testing.T) {
	t.Parallel()
	const class = "MTClass"

	existing := map[string]string{"A": "HOT", "B": "COLD", "C": "HOT"}

	tests := []struct {
		name        string
		rbac        bool
		allow       []string
		allowAll    bool
		requested   []string
		wantRead    []string // nil: the schema must not be read at all
		wantTenants []string
	}{
		{
			name:        "only authorized names reach the read",
			rbac:        true,
			allow:       []string{"A"},
			requested:   []string{"A", "B", "Z", "A", "A"},
			wantRead:    []string{"A"},
			wantTenants: []string{"A"},
		},
		{
			name:      "no permission reads nothing",
			rbac:      true,
			requested: []string{"A", "B", "Z"},
		},
		{
			name:      "no permission and one duplicated name reads nothing",
			rbac:      true,
			requested: []string{"B", "B", "B"},
		},
		{
			name:        "duplicates are removed preserving first-seen order",
			rbac:        true,
			allowAll:    true,
			requested:   []string{"C", "A", "C", "B", "A"},
			wantRead:    []string{"C", "A", "B"},
			wantTenants: []string{"C", "A", "B"},
		},
		{
			name:        "authorized nonexistent name is read and not found",
			rbac:        true,
			allow:       []string{"A", "Z"},
			requested:   []string{"Z", "A", "B"},
			wantRead:    []string{"Z", "A"},
			wantTenants: []string{"A"},
		},
		{
			name:      "empty and wildcard names never address the all-tenants resource",
			rbac:      true,
			allowAll:  true,
			requested: []string{"", "*"},
		},
		{
			name:        "empty and wildcard names are dropped next to real names",
			rbac:        true,
			allowAll:    true,
			requested:   []string{"", "A", "*"},
			wantRead:    []string{"A"},
			wantTenants: []string{"A"},
		},
		{
			name:        "rbac disabled with permission is unchanged",
			allowAll:    true,
			requested:   []string{"A", "B", "Z"},
			wantRead:    []string{"A", "B", "Z"},
			wantTenants: []string{"A", "B"},
		},
		{
			name:      "rbac disabled without permission reads nothing",
			requested: []string{"A", "B"},
		},
	}

	for _, tc := range tests {
		for _, consistency := range []bool{true, false} {
			t.Run(tc.name+"/consistency="+strconv.FormatBool(consistency), func(t *testing.T) {
				t.Parallel()
				authorizer := &readableTenantsAuthorizer{class: class, allow: tc.allow, all: tc.allowAll}
				handler, sm := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)
				handler.config.Authorization.Rbac.Enabled = tc.rbac

				spy := &tenantReadSpy{fakeSchemaManagerWithAlias: &fakeSchemaManagerWithAlias{fakeSchemaManager: sm, aliasMap: map[string]string{}}, existing: existing}
				handler.schemaManager = spy
				handler.schemaReader = spy
				if !consistency {
					sm.On("ClassInfo", class).Return(clusterSchema.ClassInfo{
						Exists: true, Tenants: len(existing), MultiTenancy: models.MultiTenancyConfig{Enabled: true},
					}).Maybe()
					sm.On("Read", class, mock.Anything).Run(func(args mock.Arguments) {
						state := &sharding.State{Physical: map[string]sharding.Physical{}}
						for n, st := range existing {
							state.Physical[n] = sharding.Physical{Name: n, Status: st}
						}
						require.NoError(t, args.Get(1).(func(*models.Class, *sharding.State) error)(nil, state))
					}).Return(nil).Maybe()
				}

				got, err := handler.GetConsistentTenants(context.Background(), &models.Principal{Username: "u"}, class, consistency, tc.requested)
				require.NoError(t, err)

				names := make([]string, len(got))
				for i, g := range got {
					names[i] = g.Name
				}
				assert.Equal(t, orEmpty(tc.wantTenants), orEmpty(names))
				assert.NotNil(t, got)

				if tc.wantRead == nil {
					assert.Zero(t, spy.reads(), "schema was read for unauthorized names")
				} else if consistency {
					assert.Equal(t, tc.wantRead, spy.lastRead())
				} else {
					assert.Equal(t, 1, spy.reads())
				}
			})
		}
	}
}

// tenantReadSpy records the names handed to the tenant read, on both the
// consistent (QueryTenants) and non-consistent (Read) paths.
type tenantReadSpy struct {
	*fakeSchemaManagerWithAlias
	existing map[string]string

	mu    sync.Mutex
	calls [][]string
}

func (s *tenantReadSpy) QueryTenants(_ string, names []string) ([]*models.Tenant, uint64, error) {
	s.mu.Lock()
	s.calls = append(s.calls, slices.Clone(names))
	s.mu.Unlock()
	return tenantsFor(s.existing, names), 0, nil
}

// ClassInfo is the first call of the non-consistent read.
func (s *tenantReadSpy) ClassInfo(class string) clusterSchema.ClassInfo {
	s.mu.Lock()
	s.calls = append(s.calls, nil)
	s.mu.Unlock()
	return s.fakeSchemaManager.ClassInfo(class)
}

func (s *tenantReadSpy) Read(class string, retry bool, reader func(*models.Class, *sharding.State) error) error {
	return s.fakeSchemaManager.Read(class, retry, reader)
}

func (s *tenantReadSpy) reads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *tenantReadSpy) lastRead() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[len(s.calls)-1]
}

func orEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func tenantsFor(existing map[string]string, names []string) []*models.Tenant {
	out := []*models.Tenant{}
	for _, n := range names {
		if status, ok := existing[n]; ok {
			out = append(out, &models.Tenant{Name: n, ActivityStatus: status})
		}
	}
	return out
}
