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
	"fmt"
	"os"
	"path/filepath"

	"github.com/weaviate/weaviate/entities/diskio"
)

// bucketDirs guards every handle→path join: an empty or escaping one resolves to the root.
type bucketDirs struct {
	root string
}

func shardBucketDirs(lsmPath string) bucketDirs { return bucketDirs{root: lsmPath} }

func (b bucketDirs) Trackers() bucketDirs {
	return bucketDirs{root: filepath.Join(b.root, migrationsDir)}
}

func (b bucketDirs) Path(dir, what string) (string, error) {
	if !migrationHandleIsOneElement(dir) {
		return "", fmt.Errorf("refusing to act on %s %q: it does not name a single directory under %q",
			what, dir, b.root)
	}
	return filepath.Join(b.root, dir), nil
}

// A stat failure must stop the caller: "cannot see it" is not proof a rename ran.
func (b bucketDirs) Exists(dir string) (bool, error) {
	if dir == "" {
		return false, nil
	}
	path, err := b.Path(dir, "a recorded directory")
	if err != nil {
		return false, err
	}
	there, err := diskio.DirExists(path)
	if err != nil {
		return false, fmt.Errorf("stat migration directory %q: %w", path, err)
	}
	return there, nil
}

func (b bucketDirs) Discard(dir, what string) error {
	if dir == "" {
		return nil
	}
	path, err := b.Path(dir, what)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove %s %q: %w", what, dir, err)
	}
	return nil
}

// The Promoted record rests on this rename, so the rename must be durable too.
func (b bucketDirs) Promote(from, to string) error {
	fromPath, err := b.Path(from, "the directory to promote")
	if err != nil {
		return err
	}
	toPath, err := b.Path(to, "the name to promote onto")
	if err != nil {
		return err
	}
	if err := diskio.RenameAndSync(fromPath, toPath); err != nil {
		return fmt.Errorf("promote %q to %q: %w", from, to, err)
	}
	return nil
}
