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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

// TestBackupShardWithoutHardlinks_RefusalLeavesNoProtection pins that a gate
// refusal on the cold path releases the shard: no stranded protection marker
// or held backupLock.
func TestBackupShardWithoutHardlinks_RefusalLeavesNoProtection(t *testing.T) {
	const shards = 4

	idx, _, _, _ := newTransferGateTestIndex(t, shards, true, liveOnEveryShard)

	var desc backup.ClassDescriptor
	err := idx.descriptorWithoutHardlinks(context.Background(), "refusal-drain-backup", &desc, nil)
	require.ErrorIs(t, err, backup.ErrBackupBlockedByInFlightReindex)

	// descriptorWithoutHardlinks releases in a goroutine on error, so drive it
	// synchronously here rather than polling for the background one.
	require.NoError(t, idx.ReleaseBackup(context.Background(), "refusal-drain-backup"))

	idx.backupProtectedShards.Range(func(key, _ any) bool {
		t.Fatalf("shard %v stayed protected after a refused backup", key)
		return false
	})
	for s := 0; s < shards; s++ {
		name := transferGateShardName(s)
		require.True(t, idx.backupLock.TryRLock(name),
			"a refused backup must not leave %q locked", name)
		idx.backupLock.RUnlock(name)
	}
}
