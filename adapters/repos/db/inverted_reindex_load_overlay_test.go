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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// With no records, the task list (a manager lock) must not be read.
func TestAnEmptyRecordStoreSkipsTheTaskListRead(t *testing.T) {
	tests := []struct {
		name string
		ask  func(s *Shard)
	}{
		{"restore overlays on load", func(s *Shard) { s.restoreMigratedOverlays(&models.Class{Class: "Books"}, nil) }},
		{"migrated index stands", func(s *Shard) { s.migratedIndexStands("title", "filterable") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			reads := 0
			db := &DB{logger: logger}
			db.migrationCluster.local = func(string) ([]distributedtask.TaskStatusEntry, bool) {
				reads++
				return nil, true
			}
			s := &Shard{
				name:             "shard",
				index:            &Index{db: db, logger: logger},
				migrationRecords: NewMigrationRecordStore(t.TempDir(), logger),
			}

			tt.ask(s)

			require.Zero(t, reads)
		})
	}
}
