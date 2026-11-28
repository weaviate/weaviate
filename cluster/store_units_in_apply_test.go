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

package cluster

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
)

// The apply builds a submit's units from the sharding state its log index has reached, and its refusals reach the
// caller as permanent sentinels, also after the gRPC hop a forwarded submit takes.
func TestExecute_UnitsBuiltInTheApply(t *testing.T) {
	deactivateT2, err := gproto.Marshal(&cmd.UpdateTenantsRequest{
		Tenants: []*cmd.Tenant{{Name: "t2", Status: models.TenantActivityStatusCOLD}},
	})
	require.NoError(t, err)
	addT3, err := gproto.Marshal(&cmd.AddTenantsRequest{
		ClusterNodes: []string{"Node-1"},
		Tenants:      []*cmd.Tenant{{Name: "t3", Status: models.TenantActivityStatusHOT}},
	})
	require.NoError(t, err)
	deactivate := &cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_UPDATE_TENANT, Class: "Books", SubCommand: deactivateT2}

	for _, tc := range []struct {
		name         string
		before       *cmd.ApplyRequest
		metadataOnly bool
		tenants      []string
		want         error
		wantUnits    []string
	}{
		{name: "scoped run names a COLD tenant", before: deactivate, tenants: []string{"t2"}, want: distributedtask.ErrTaskTenantsNotHot},
		{name: "scoped run names a missing tenant", tenants: []string{"t1", "t9"}, want: distributedtask.ErrTaskTenantsMissing},
		{name: "full run over a COLD tenant", before: deactivate, want: distributedtask.ErrTaskTenantsNotHot},
		{name: "scoped run names a HOT tenant", before: deactivate, tenants: []string{"t1"}, wantUnits: []string{"t1__Node-1"}},
		{
			name:      "a tenant added in the entry before the submit gets its unit",
			before:    &cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_ADD_TENANT, Class: "Books", SubCommand: addT3},
			wantUnits: []string{"t1__Node-1", "t2__Node-1", "t3__Node-1"},
		},
		{name: "collection deleted", before: &cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_DELETE_CLASS, Class: "Books"}, want: distributedtask.ErrTaskCollectionGone},
		{
			name:   "collection deleted under another case on a metadata-only voter",
			before: &cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_DELETE_CLASS, Class: "BOOKS"}, metadataOnly: true,
			want: distributedtask.ErrTaskCollectionGone,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, m := reindexTenantsStore(t, func(c *Config) {
				c.MetadataOnlyVoters = tc.metadataOnly
				c.DistributedTaskRules = map[string]distributedtask.TaskRules{db.ReindexNamespace: db.ReindexTaskRules{}}
			})
			m.indexer.On("AddTenants", mock.Anything, mock.Anything).Return(nil).Maybe()
			if tc.before != nil {
				_, err := s.Execute(tc.before)
				require.NoError(t, err)
			}

			_, err := s.Execute(reindexTitleTaskCmd(t, "Books", tc.tenants...))
			tasks, listErr := s.distributedTasksManager.ListDistributedTasks(context.Background())
			require.NoError(t, listErr)
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				require.ErrorIs(t, err, distributedtask.ErrPermanentRejection)
				require.ErrorIs(t, distributedtask.RehydratePermanentRejection(distributedtask.ToRPCError(err)), tc.want)
				require.Empty(t, tasks[db.ReindexNamespace])
				return
			}
			require.NoError(t, err)
			require.Len(t, tasks[db.ReindexNamespace], 1)
			units := tasks[db.ReindexNamespace][0].Units
			ids := make([]string, 0, len(units))
			for id, u := range units {
				require.Equal(t, "Node-1", u.NodeID)
				ids = append(ids, id)
			}
			require.Equal(t, tc.wantUnits, slices.Sorted(slices.Values(ids)))
		})
	}
}
