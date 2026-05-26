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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const asyncDeleteSuffix = ".deleteme"

func renameForAsyncDelete(path string, logger logrus.FieldLogger) (string, error) {
	parent := filepath.Dir(path)
	base := filepath.Base(path)

	// timestamp + 8 hex of crypto random; repeated drop→recreate→drop on
	// the same class must not collide with an existing .deleteme dir.
	var randSuffix [4]byte
	if _, err := rand.Read(randSuffix[:]); err != nil {
		return "", fmt.Errorf("generate async-delete suffix: %w", err)
	}
	target := filepath.Join(parent,
		fmt.Sprintf("%s.%d.%s%s",
			base, time.Now().UnixNano(), hex.EncodeToString(randSuffix[:]),
			asyncDeleteSuffix))

	if err := os.Rename(path, target); err != nil {
		return "", fmt.Errorf("rename %q to %q: %w", path, target, err)
	}

	// Without the parent fsync a crash between rename and fsync could
	// leave the original name on disk, so the next startup would treat
	// the directory as live. Best-effort: log on failure, continue.
	if err := fsyncDir(parent); err != nil {
		logger.WithFields(logrus.Fields{
			"action": "async_delete_rename",
			"path":   target,
		}).Warnf("fsync parent after rename: %v", err)
	}
	return target, nil
}

func spawnAsyncDelete(path string, logger logrus.FieldLogger) {
	enterrors.GoWrapper(func() {
		start := time.Now()
		if err := os.RemoveAll(path); err != nil {
			logger.WithFields(logrus.Fields{
				"action": "async_delete",
				"path":   path,
			}).Errorf("remove pending-delete dir: %v", err)
			return
		}
		logger.WithFields(logrus.Fields{
			"action":   "async_delete",
			"path":     path,
			"duration": time.Since(start),
		}).Debug("removed pending-delete dir")
	}, logger)
}

func scanAndAsyncDeletePending(root string, logger logrus.FieldLogger) {
	scanLevel := func(dir string) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if !os.IsNotExist(err) {
				logger.WithFields(logrus.Fields{
					"action": "async_delete_scan",
					"dir":    dir,
				}).Warnf("read dir: %v", err)
			}
			return
		}
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), asyncDeleteSuffix) {
				continue
			}
			spawnAsyncDelete(filepath.Join(dir, e.Name()), logger)
		}
	}

	// .deleteme dirs live at the root (index-level) and one dir deep
	// (shard-level inside live class dirs); walk both.
	scanLevel(root)
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasSuffix(e.Name(), asyncDeleteSuffix) {
			continue
		}
		scanLevel(filepath.Join(root, e.Name()))
	}
}

func fsyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
