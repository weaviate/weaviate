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

// asyncDeleteSuffix is appended to the renamed directory of a dropped
// index or shard. Any directory with this suffix is owned by the
// async-delete machinery and is safe to remove on startup.
const asyncDeleteSuffix = ".deleteme"

// renameForAsyncDelete renames `path` to a sibling whose name carries the
// async-delete suffix and a unix-nano timestamp. The rename is atomic and
// the parent directory is fsync'd before returning so the on-disk state
// survives a crash. Concurrent drop + re-create of the same class is
// supported because the timestamp ensures the renamed name is unique.
//
// The caller treats a successful rename as "the on-disk delete is now
// committed" — even if every subsequent step fails, the startup scanner
// picks up the renamed directory and finishes the removal on next boot.
func renameForAsyncDelete(path string, logger logrus.FieldLogger) (string, error) {
	parent := filepath.Dir(path)
	base := filepath.Base(path)

	// timestamp + 8 hex of crypto random. Repeated delete-recreate-delete
	// within the same nanosecond (or after a clock skew) still produces a
	// unique target, so we never collide with an existing .deleteme dir.
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

	// fsync the parent so the rename is durable. Without this, a crash
	// between rename and fsync could leave the rename in the kernel page
	// cache but not on disk — i.e. the original name would reappear and
	// the next startup would treat the directory as live.
	if err := fsyncDir(parent); err != nil {
		// The rename already happened; the marker IS on disk. Log and
		// continue — startup recovery still works without the fsync, just
		// with a tiny crash-resistance gap.
		logger.WithFields(logrus.Fields{
			"action": "async_delete_rename",
			"path":   target,
		}).Warnf("fsync parent after rename: %v", err)
	}
	return target, nil
}

// spawnAsyncDelete runs os.RemoveAll(path) in a background goroutine. The
// caller has already renamed the path with the async-delete suffix, so a
// crash mid-removal is recoverable — the startup scanner re-spawns the
// cleanup. Errors are logged; they do not propagate because the caller's
// in-memory state has already moved on.
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

// scanAndAsyncDeletePending walks `root` (and its immediate child
// directories that aren't themselves async-delete targets) looking for
// entries with the async-delete suffix, and spawns a cleanup goroutine
// for each. Designed to be called once during startup so leftovers from
// pre-crash drops finish asynchronously without blocking boot.
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

	// Level 1: root contains class-level (index-level) pending deletes.
	scanLevel(root)

	// Level 2: each live class dir may contain shard-level pending
	// deletes. Read the root again to walk live entries; skip any that
	// are themselves pending (already queued by level 1).
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), asyncDeleteSuffix) {
			continue
		}
		scanLevel(filepath.Join(root, e.Name()))
	}
}

// fsyncDir opens a directory and calls Sync on its file descriptor. On
// macOS / Linux this commits the directory entry change (e.g. a Rename)
// to the underlying storage. Best-effort: returns the first error.
func fsyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
