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

package lsmkv

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/diskio"
)

var logOnceWhenRecoveringFromWAL sync.Once

func (b *Bucket) mayRecoverFromCommitLogs() error {
	beforeAll := time.Now()
	defer b.metrics.TrackStartupBucketRecovery(beforeAll)

	list, err := os.ReadDir(b.dir)
	if err != nil {
		return err
	}

	var walFileNames []string
	for _, fileInfo := range list {
		if filepath.Ext(fileInfo.Name()) != ".wal" {
			// skip, this could be disk segments, etc.
			continue
		}

		path := filepath.Join(b.dir, fileInfo.Name())

		stat, err := os.Stat(path)
		if err != nil {
			return errors.Wrap(err, "stat commit log")
		}

		if stat.Size() == 0 {
			err := os.Remove(path)
			if err != nil {
				return errors.Wrap(err, "remove empty wal file")
			}
			continue
		}

		walFileNames = append(walFileNames, fileInfo.Name())
	}

	if len(walFileNames) > 0 {
		logOnceWhenRecoveringFromWAL.Do(func() {
			b.logger.WithField("action", "lsm_recover_from_active_wal").
				WithField("path", b.dir).
				Warning("active write-ahead-log found")
		})
	}

	// recover from each log
	for i, fname := range walFileNames {
		walForActiveMemtable := i == len(walFileNames)-1

		path := filepath.Join(b.dir, strings.TrimSuffix(fname, ".wal"))

		cl, err := newCommitLogger(path)
		if err != nil {
			return errors.Wrap(err, "init commit logger")
		}
		if !walForActiveMemtable {
			defer cl.close()
		}

		cl.pause()
		defer cl.unpause()

		mt, err := newMemtable(path, b.strategy, b.secondaryIndices,
			cl, b.metrics, b.logger, b.enableChecksumValidation)
		if err != nil {
			return err
		}

		meteredReader := diskio.NewMeteredReader(bufio.NewReader(cl.file), b.metrics.TrackStartupReadWALDiskIO)

		err = newCommitLoggerParser(b.strategy, meteredReader, mt).Do()
		if err != nil {
			b.logger.WithField("action", "lsm_recover_from_active_wal_corruption").
				WithField("path", filepath.Join(b.dir, fname)).
				Error(errors.Wrap(err, "write-ahead-log ended abruptly, some elements may not have been recovered"))
		}

		if walForActiveMemtable {
			b.active = mt
		} else {
			if err := mt.flush(); err != nil {
				return errors.Wrap(err, "flush memtable after WAL recovery")
			}
		}

		b.logger.WithField("action", "lsm_recover_from_active_wal_success").
			WithField("path", filepath.Join(b.dir, fname)).
			Info("successfully recovered from write-ahead-log")
	}

	return nil
}
