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

package db

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/sharding"
)

// A tenant this node never loaded is invisible to ForEachShard, so a delete it
// missed leaves the data on disk and a re-created tenant of the same name
// serves it. The cases that keep a directory are the ones that constrain the
// fix: sweeping everything absent from the sharding state destroys live data.
func TestUpdateIndexDeleteTenants_UnloadedTenantDirectory(t *testing.T) {
	const (
		className = "Abc"
		tenant    = "doomed"
	)

	tests := []struct {
		name        string
		dirName     string // defaults to tenant
		noLSM       bool   // directory holds no LSM store, so it is not a shard
		inSchema    bool   // tenant still present in the incoming sharding state
		wantDirGone bool
	}{
		{name: "deleted while unloaded", wantDirGone: true},
		{name: "still in schema while unloaded", inSchema: true},
		{name: "lsmkv snapshots dir", dirName: ".snapshots", noLSM: true},
		{name: "unrecognised index-level dir", dirName: "some-future-metadata", noLSM: true},
		// Holds an LSM store, so only the suffix keeps it.
		{name: "pending async delete", dirName: "doomed.123.abcdef01.deleteme"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, hook := newDropTestIndex(t)

			name := tt.dirName
			if name == "" {
				name = tenant
			}

			// Resident but absent from idx.shards: never loaded here.
			dir := shardPath(idx.path(), name)
			payload := dir
			if !tt.noLSM {
				payload = shardPathLSM(idx.path(), name)
			}
			require.NoError(t, os.MkdirAll(payload, 0o755))
			require.NoError(t, os.WriteFile(
				filepath.Join(payload, "objects.db"), []byte("pre-delete data"), 0o644))
			require.Nil(t, idx.shards.Load(name), "precondition: not loaded")

			incomingSS := &sharding.State{
				PartitioningEnabled: true,
				Physical:            map[string]sharding.Physical{},
			}
			if tt.inSchema {
				incomingSS.Physical[name] = sharding.Physical{Name: name}
			}

			m := newDropTestMigrator(idx, className, nil)
			require.NoError(t, m.updateIndexDeleteTenants(context.Background(), idx, incomingSS))

			// The only signal an operator gets that a delete was missed.
			var warned *logrus.Entry
			for _, e := range hook.AllEntries() {
				if e.Data["action"] == "reconcile_tenant_dirs" {
					warned = e
				}
			}

			if tt.wantDirGone {
				require.NoDirExists(t, dir,
					"%q is absent from the schema, so its data must not survive on disk", name)
				require.NotNil(t, warned, "an orphaned tenant must be logged")
				require.Equal(t, logrus.WarnLevel, warned.Level)
				require.Equal(t, className, warned.Data["class"])
				require.Equal(t, 1, warned.Data["count"])
				require.Equal(t, []string{name}, warned.Data["sample"])
				return
			}
			require.Nil(t, warned, "nothing was orphaned, so nothing should be logged")
			require.DirExists(t, dir, "%q must not be swept", name)
		})
	}
}
