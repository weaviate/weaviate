//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func (i *Index) checkSingleShardMigration(shardState *sharding.State) error {
	res, err := os.ReadDir(i.Config.RootPath)
	if err != nil {
		return err
	}

	for _, entry := range res {
		if !strings.HasPrefix(entry.Name(), i.ID()+"_single") {
			// either not part of this index, or not a "_single" shard
			continue
		}

		// whatever is left now, needs to be migrated
		shards := shardState.AllPhysicalShards()
		if len(shards) != 1 {
			return errors.Errorf("cannot migrate '_single' shard into config with %d "+
				"desired shards", len(shards))
		}

		shardName := shards[0]
		newName := i.ID() + "_" + shardName + strings.TrimPrefix(entry.Name(), i.ID()+"_single")
		oldPath := filepath.Join(i.Config.RootPath, entry.Name())
		newPath := filepath.Join(i.Config.RootPath, newName)

		if err := os.Rename(oldPath, newPath); err != nil {
			return errors.Wrapf(err, "migrate shard %q to %q", oldPath, newPath)
		}

		i.logger.WithField("action", "index_startup_migrate_shards_successful").
			WithField("old_shard", oldPath).
			WithField("new_shard", newPath).
			Infof("successfully migrated shard file %q (created in an earlier version) to %q",
				oldPath, newPath)
	}

	return nil
}
