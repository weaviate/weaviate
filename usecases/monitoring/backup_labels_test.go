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

package monitoring

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBackupClassLabel pins the class recovered from a storage object key.
//
// The backup byte counters used to pass the literal string "class" for
// class_name, collapsing every collection onto one series. The class is only
// recoverable from the key, which usecases/backup.chunkKey formats as
// "<class>/chunk-<n>"; backends prepend their own path segments on top.
func TestBackupClassLabel(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{"bare chunk key", "Article/chunk-0", "Article"},
		{"double digit chunk", "Article/chunk-12", "Article"},
		{"backend prefixed", "my-backup/node1/Article/chunk-3", "Article"},
		{"class with underscore", "My_Class/chunk-1", "My_Class"},
		{"metadata object", "backup_config.json", "n/a"},
		{"nested metadata", "my-backup/backup_config.json", "n/a"},
		{"empty key", "", "n/a"},
		{"chunk with no class", "chunk-0", "n/a"},
		{"empty class segment", "/chunk-0", "n/a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, BackupClassLabel(tt.key))
		})
	}
}

// TestBackupClassLabelGrouped pins that PROMETHEUS_MONITORING_GROUP collapses
// the label, matching every other per-class backup metric. The module-level
// byte counters used to bypass this guard entirely.
func TestBackupClassLabelGrouped(t *testing.T) {
	orig := GetMetrics().Group
	t.Cleanup(func() { GetMetrics().Group = orig })

	GetMetrics().Group = true
	assert.Equal(t, "n/a", BackupClassLabel("Article/chunk-0"))
}
