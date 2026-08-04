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

//go:build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/errorcompounder"
)

// noopTenantProcessor satisfies the migrator's processor dependency; the
// frozen path only nil-checks it.
type noopTenantProcessor struct{}

func (noopTenantProcessor) UpdateTenantsProcess(context.Context, string, *command.TenantProcessRequest) (uint64, error) {
	return 0, nil
}

// Freezing a tenant whose shard is not loaded deletes the tenant's on-disk
// subtree with no Store to shut each bucket down, exactly like the dropShards
// unloaded branch, so it must also purge the subtree's registry keys: residue
// stranded there would otherwise block the tenant's next activation with
// ErrBucketAlreadyRegistered. The sibling assertion pins the purge to the
// tenant's own lsm dir rather than a broader prefix.
func TestMigratorFrozen_StrandedEntryPurged(t *testing.T) {
	idx := newEmptyMTIndex(t)
	m := &Migrator{cluster: noopTenantProcessor{}, logger: logrus.New()}

	p1 := tenantIDBucketPath(idx, "t1")
	p10 := tenantIDBucketPath(idx, "t10")
	require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p1))
	require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p10))
	t.Cleanup(func() {
		lsmkv.GlobalBucketRegistry.Remove(p1)
		lsmkv.GlobalBucketRegistry.Remove(p10)
	})

	ec := errorcompounder.NewSafe()
	m.frozen(context.Background(), idx, []string{"t1"}, ec)
	require.NoError(t, ec.ToError(), "freezing an unloaded tenant must succeed")

	require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p1),
		"frozen must purge the unloaded tenant's registry residue")
	lsmkv.GlobalBucketRegistry.Remove(p1)
	require.ErrorIs(t, lsmkv.GlobalBucketRegistry.TryAdd(p10), lsmkv.ErrBucketAlreadyRegistered,
		"frozen must not purge a sibling tenant's registry entry")
}
