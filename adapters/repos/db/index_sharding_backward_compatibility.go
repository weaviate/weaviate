//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"

	"github.com/pkg/errors"
)

func (i *Index) checkSingleShardMigration() error {
	dirEntries, err := os.ReadDir(i.Config.RootPath)
	if err != nil {
		return err
	}

	singleIndexId := i.ID() + "_single"
	if !needsSingleShardMigration(dirEntries, singleIndexId) {
		return nil
	}

	var singleShardName string
	className := i.Config.ClassName.String()

	err = i.schemaReader.Read(className, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return fmt.Errorf("unable to retrieve sharding state for class %q", className)
		}

		shards := state.AllPhysicalShards()
		if len(shards) != 1 {
			return errors.Errorf("cannot migrate '_single' shard into config with %d desired shards", len(shards))
		}

		singleShardName = shards[0]
		return nil
	})
	if err != nil {
		return err
	}

	for _, entry := range dirEntries {
		if !strings.HasPrefix(entry.Name(), singleIndexId) {
			continue
		}

		newName := i.ID() + "_" + singleShardName + strings.TrimPrefix(entry.Name(), singleIndexId)
		oldPath := filepath.Join(i.Config.RootPath, entry.Name())
		newPath := filepath.Join(i.Config.RootPath, newName)

		if err := os.Rename(oldPath, newPath); err != nil {
			return errors.Wrapf(err, "migrate shard %q to %q", oldPath, newPath)
		}

		i.logger.WithField("action", "index_startup_migrate_shards_successful").
			WithField("old_shard", oldPath).
			WithField("new_shard", newPath).
			Infof("successfully migrated shard file %q to %q", oldPath, newPath)
	}

	return nil
}

func needsSingleShardMigration(dirEntries []os.DirEntry, indexID string) bool {
	singleShardPrefix := indexID + "_single"
	for _, dirEntry := range dirEntries {
		if strings.HasPrefix(dirEntry.Name(), singleShardPrefix) {
			return true
		}
	}
	return false
}
