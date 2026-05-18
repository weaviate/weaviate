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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/models"
)

// TestMetaClass_ReplicationVersion: ReplicationVersion folds into version()
// and ClassInfo.Version() so the per-write WaitForUpdate fence covers
// op-mutating applies that don't touch Class/Shard versions.
func TestMetaClass_ReplicationVersion(t *testing.T) {
	t.Run("zero by default and excluded from version()", func(t *testing.T) {
		m := &metaClass{Class: models.Class{Class: "C"}, ClassVersion: 5, ShardVersion: 3}
		assert.EqualValues(t, 0, m.ReplicationVersion)
		assert.EqualValues(t, 5, m.version())
	})

	t.Run("BumpReplicationVersion sets the field and lifts version() when largest", func(t *testing.T) {
		m := &metaClass{Class: models.Class{Class: "C"}, ClassVersion: 5, ShardVersion: 3}
		m.BumpReplicationVersion(42)
		assert.EqualValues(t, 42, m.ReplicationVersion)
		assert.EqualValues(t, 42, m.version(), "ReplicationVersion is the new max")
	})

	t.Run("version() returns max across all three fields", func(t *testing.T) {
		cases := []struct {
			name           string
			classV, shardV uint64
			replV          uint64
			wantVersion    uint64
		}{
			{"ClassVersion largest", 100, 50, 25, 100},
			{"ShardVersion largest", 50, 100, 25, 100},
			{"ReplicationVersion largest", 50, 25, 100, 100},
			{"all equal", 7, 7, 7, 7},
			{"all zero", 0, 0, 0, 0},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				m := &metaClass{
					Class:              models.Class{Class: "C"},
					ClassVersion:       tc.classV,
					ShardVersion:       tc.shardV,
					ReplicationVersion: tc.replV,
				}
				assert.EqualValues(t, tc.wantVersion, m.version())
			})
		}
	})

	t.Run("BumpReplicationVersion is monotonic — older values do not regress it", func(t *testing.T) {
		m := &metaClass{Class: models.Class{Class: "C"}}
		m.BumpReplicationVersion(50)
		m.BumpReplicationVersion(10) // older RAFT index arriving late must not lower
		assert.EqualValues(t, 50, m.ReplicationVersion)
	})

	t.Run("ClassInfo surfaces ReplicationVersion and Version() includes it", func(t *testing.T) {
		m := &metaClass{
			Class:              models.Class{Class: "C"},
			ClassVersion:       5,
			ShardVersion:       3,
			ReplicationVersion: 42,
		}
		ci := m.ClassInfo()
		assert.EqualValues(t, 42, ci.ReplicationVersion)
		assert.EqualValues(t, 42, ci.Version())
	})

	t.Run("nil metaClass safely returns 0 from version()", func(t *testing.T) {
		var m *metaClass
		assert.EqualValues(t, 0, m.version())
	})
}
