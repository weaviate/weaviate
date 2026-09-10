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
	"encoding/json"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// recordingIndexer records the class deletions the reload asks for. The
// embedded interface is nil, so an unexpected call panics.
type recordingIndexer struct {
	Indexer

	deleted []string
	frozen  map[string]bool
	dropErr error
}

func (r *recordingIndexer) DropOrphanedClass(_ context.Context, class string, hasFrozen bool) error {
	r.deleted = append(r.deleted, class)
	if r.frozen == nil {
		r.frozen = map[string]bool{}
	}
	r.frozen[class] = hasFrozen
	return r.dropErr
}

func (r *recordingIndexer) TriggerSchemaUpdateCallbacks() {}

func (r *recordingIndexer) ReloadLocalDB(context.Context, []command.UpdateClassRequest) error {
	return nil
}

func addClass(t *testing.T, sm *SchemaManager, class string) {
	t.Helper()
	sub, err := json.Marshal(command.AddClassRequest{
		Class: &models.Class{Class: class},
		State: &sharding.State{Physical: map[string]sharding.Physical{
			"shard1": {Name: "shard1", BelongsToNodes: []string{"node1"}},
		}},
	})
	require.NoError(t, err)
	require.NoError(t, sm.AddClass(&command.ApplyRequest{
		Type: command.ApplyRequest_TYPE_ADD_CLASS, Class: class, SubCommand: sub,
	}, "node1", true, false))
}

// TestReloadDropsClassesTheSchemaNoLongerNames reproduces the two RAFT paths
// that leave a deleted collection's data on disk forever.
func TestReloadDropsClassesTheSchemaNoLongerNames(t *testing.T) {
	tests := []struct {
		name        string
		seed        func(t *testing.T, sm *SchemaManager)
		wantDeleted []string
	}{
		{
			name: "delete applied schema-only during catch-up",
			seed: func(t *testing.T, sm *SchemaManager) {
				addClass(t, sm, "Orphan")
				deleteClassSchemaOnly(t, sm, "Orphan")
			},
			wantDeleted: []string{"Orphan"},
		},
		{
			name: "snapshot restored without the class",
			seed: func(t *testing.T, sm *SchemaManager) {
				addClass(t, sm, "Kept")
				addClass(t, sm, "Orphan")
				require.NoError(t, sm.Restore(snapshotWithout(t, sm, "Orphan"), sm.parser))
			},
			wantDeleted: []string{"Orphan"},
		},
		{
			name: "delete then re-add keeps the re-added data",
			seed: func(t *testing.T, sm *SchemaManager) {
				addClass(t, sm, "Revived")
				deleteClassSchemaOnly(t, sm, "Revived")
				addClass(t, sm, "Revived")
			},
			wantDeleted: nil,
		},
		{
			name: "re-add then delete again still drops",
			seed: func(t *testing.T, sm *SchemaManager) {
				addClass(t, sm, "Churned")
				deleteClassSchemaOnly(t, sm, "Churned")
				addClass(t, sm, "Churned")
				deleteClassSchemaOnly(t, sm, "Churned")
			},
			wantDeleted: []string{"Churned"},
		},
		{
			name: "a restore that brings the class back keeps its data",
			seed: func(t *testing.T, sm *SchemaManager) {
				addClass(t, sm, "Kept")
				addClass(t, sm, "Revived")
				full := snapshotWithout(t, sm, "")
				require.NoError(t, sm.Restore(snapshotWithout(t, sm, "Revived"), sm.parser))
				require.NoError(t, sm.Restore(full, sm.parser))
			},
			wantDeleted: nil,
		},
		{
			name: "one class survives while another is dropped",
			seed: func(t *testing.T, sm *SchemaManager) {
				addClass(t, sm, "Kept")
				addClass(t, sm, "Orphan")
				deleteClassSchemaOnly(t, sm, "Orphan")
			},
			wantDeleted: []string{"Orphan"},
		},
		{
			name: "legacy-format snapshot that drops a class (#651)",
			seed: func(t *testing.T, sm *SchemaManager) {
				addClass(t, sm, "Kept")
				addClass(t, sm, "Orphan")
				require.NoError(t, sm.RestoreLegacy(legacySnapshotWithout(t, sm, "Orphan"), sm.parser))
			},
			wantDeleted: []string{"Orphan"},
		},
		{
			name: "deleting a class that never existed records nothing",
			seed: func(t *testing.T, sm *SchemaManager) {
				addClass(t, sm, "Kept")
				// Accepted and replayed even though neither is a collection.
				deleteClassSchemaOnly(t, sm, "Raft")
				deleteClassSchemaOnly(t, sm, "NeverExisted")
			},
			wantDeleted: nil,
		},
		{
			name: "a class the schema still names is never dropped",
			seed: func(t *testing.T, sm *SchemaManager) {
				addClass(t, sm, "Kept")
			},
			wantDeleted: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := &recordingIndexer{}
			parser := fakes.NewMockParser()
			parser.On("ParseClass", mock.Anything).Return(nil)
			sm := NewSchemaManager("node1", idx, parser, prometheus.NewPedanticRegistry(), logrus.New())

			tt.seed(t, sm)
			sm.ReloadDBFromSchema()

			require.ElementsMatch(t, tt.wantDeleted, idx.deleted)
		})
	}
}

// TestMetadataOnlyNodesRecordNoOrphans pins that a node holding no class data
// accumulates none: schemaOnly is true for its every command, and it never
// reloads, so a record would sit there for the process lifetime.
func TestMetadataOnlyNodesRecordNoOrphans(t *testing.T) {
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	idx := &recordingIndexer{}
	sm := NewSchemaManager("node1", idx, parser, prometheus.NewPedanticRegistry(), logrus.New())
	sm.SetMetadataOnly(true)

	addClass(t, sm, "Orphan")
	deleteClassSchemaOnly(t, sm, "Orphan")
	require.NoError(t, sm.Restore(snapshotWithout(t, sm, ""), sm.parser))

	sm.orphansMu.Lock()
	defer sm.orphansMu.Unlock()
	require.Empty(t, sm.orphanedClasses, "metadata-only node recorded orphans nothing drains")
}

// TestFailedDropIsRetriedOnTheNextReload pins that a failed drop goes back on
// the pending set, since nothing else would name that data again.
func TestFailedDropIsRetriedOnTheNextReload(t *testing.T) {
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	idx := &recordingIndexer{dropErr: errors.New("disk gone")}
	sm := NewSchemaManager("node1", idx, parser, prometheus.NewPedanticRegistry(), logrus.New())

	addClass(t, sm, "Orphan")
	deleteClassSchemaOnly(t, sm, "Orphan")

	sm.ReloadDBFromSchema()
	require.Equal(t, []string{"Orphan"}, idx.deleted)

	// The failure is retried rather than forgotten.
	idx.dropErr = nil
	sm.ReloadDBFromSchema()
	require.Equal(t, []string{"Orphan", "Orphan"}, idx.deleted)
}

// deleteClassSchemaOnly applies DELETE_CLASS the way catch-up does: the entry
// goes, the store is left untouched.
func deleteClassSchemaOnly(t *testing.T, sm *SchemaManager, class string) {
	t.Helper()
	require.NoError(t, sm.DeleteClass(&command.ApplyRequest{
		Type: command.ApplyRequest_TYPE_DELETE_CLASS, Class: class,
	}, true, false))
}

// legacySnapshotWithout is snapshotWithout in the pre-Snapshot wire format,
// which Store.Restore routes through RestoreLegacy.
func legacySnapshotWithout(t *testing.T, sm *SchemaManager, class string) []byte {
	t.Helper()
	classes := sm.schema.MetaClasses()
	delete(classes, class)
	data, err := json.Marshal(snapshot{NodeID: "node1", Classes: classes})
	require.NoError(t, err)
	return data
}

// snapshotWithout stands in for the leader's snapshot, taken after class was
// deleted there.
func snapshotWithout(t *testing.T, sm *SchemaManager, class string) []byte {
	t.Helper()
	classes := sm.schema.MetaClasses()
	delete(classes, class)
	data, err := json.Marshal(classes)
	require.NoError(t, err)
	return data
}

// TestRestoreCarriesTheFrozenFlag pins that a class restored away keeps the
// offloaded-tenant flag its pre-restore state carried. Without it the drop
// removes the local directory and skips the cloud copy, which nothing else
// would ever name again.
func TestRestoreCarriesTheFrozenFlag(t *testing.T) {
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	idx := &recordingIndexer{}
	sm := NewSchemaManager("node1", idx, parser, prometheus.NewPedanticRegistry(), logrus.New())

	addFrozenTenantClass(t, sm, "Frozen")
	addClass(t, sm, "Hot")
	// A snapshot naming no class at all: both were deleted while away.
	empty, err := json.Marshal(map[string]*metaClass{})
	require.NoError(t, err)
	require.NoError(t, sm.Restore(empty, sm.parser))

	sm.ReloadDBFromSchema()

	require.ElementsMatch(t, []string{"Frozen", "Hot"}, idx.deleted)
	require.True(t, idx.frozen["Frozen"], "the offloaded class lost its frozen flag")
	require.False(t, idx.frozen["Hot"])
}

// addFrozenTenantClass seeds a multi-tenant class with one offloaded tenant.
func addFrozenTenantClass(t *testing.T, sm *SchemaManager, class string) {
	t.Helper()
	sub, err := json.Marshal(command.AddClassRequest{
		Class: &models.Class{
			Class:              class,
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		},
		State: &sharding.State{
			PartitioningEnabled: true,
			Physical: map[string]sharding.Physical{
				"tenant1": {
					Name:           "tenant1",
					BelongsToNodes: []string{"node1"},
					Status:         models.TenantActivityStatusFROZEN,
				},
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, sm.AddClass(&command.ApplyRequest{
		Type: command.ApplyRequest_TYPE_ADD_CLASS, Class: class, SubCommand: sub,
	}, "node1", true, false))
}
