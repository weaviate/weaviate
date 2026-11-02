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
	"fmt"
	"os"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func (m *Migrator) frozen(ctx context.Context, idx *Index, frozen []string, ec errorcompounder.ErrorCompounder) {
	if m.cluster == nil {
		ec.Add(fmt.Errorf("no cluster exists in the migrator"))
		return
	}

	eg := enterrors.NewErrorGroupWrapper(m.logger)
	eg.SetLimit(_NUMCPU * 2)

	for _, name := range frozen {
		eg.Go(func() error {
			idx.backupLock.RLock(name)
			defer idx.backupLock.RUnlock(name)

			idx.shardCreateLocks.Lock(name)
			defer idx.shardCreateLocks.Unlock(name)

			err := idx.removeFrozenLocal(name)
			ec.Add(err)
			return err
		})
	}
	eg.Wait()
}

// removeFrozenLocal requires the caller to hold the tenant's shardCreateLocks.
func (i *Index) removeFrozenLocal(name string) error {
	if shard, ok := i.shards.LoadAndDelete(name); ok {
		return shard.drop(false)
	}
	if err := os.RemoveAll(fmt.Sprintf("%s/%s", i.path(), name)); err != nil {
		return fmt.Errorf("attempt to delete local fs for shard %s: %w", name, err)
	}
	lsmkv.GlobalBucketRegistry.RemoveByPrefixes(shardPathLSM(i.path(), name))
	return nil
}
