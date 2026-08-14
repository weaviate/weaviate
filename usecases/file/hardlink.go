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

package file

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// SnapshotNameMaxLabel caps the human-readable label in a staging dir name.
// The label is operator convenience only; uniqueness comes from the appended
// hash, not the label.
const SnapshotNameMaxLabel = 20

// SafeStagingDirName builds a staging directory name guaranteed to fit within
// filesystem path-component limits (255 bytes) regardless of input length,
// while staying unique via a hash suffix of the full input.
//
// Example: SafeStagingDirName(".backup-staging-", "backup1", "myclass")
// → ".backup-staging-backup1-myclass-a1b2c3d4e5f6"
func SafeStagingDirName(prefix string, parts ...string) string {
	body := strings.Join(parts, "-")
	h := sha256.Sum256([]byte(prefix + body))
	hashSuffix := hex.EncodeToString(h[:6]) // 12 hex chars

	label := body
	if len(label) > SnapshotNameMaxLabel {
		label = label[:SnapshotNameMaxLabel]
	}
	return prefix + label + "-" + hashSuffix
}

// ProbeHardlinkSupport reports whether the filesystem backing dir supports hardlinks.
func ProbeHardlinkSupport(dir string) bool {
	// Test-only: forces fallback mode without needing a non-hardlink FS.
	if os.Getenv("WEAVIATE_TEST_FORCE_NO_HARDLINK") == "true" {
		return false
	}

	f, err := os.CreateTemp(dir, ".hardlink-probe-*")
	if err != nil {
		return false
	}
	src := f.Name()
	f.Close()
	defer os.Remove(src)

	dst := src + ".link"
	defer os.Remove(dst)

	return os.Link(src, dst) == nil
}

// HardlinkPair is one src→dst hardlink request; both are absolute paths.
type HardlinkPair struct {
	Src string
	Dst string
}

// HardlinkFiles hardlinks each pair, creating Dst's parent directories as needed.
//
// The caller must keep the Src files stable for the call (e.g. by halting the
// shard) and pass only immutable files: a shared inode would let post-snapshot
// writes corrupt the staged copy. Use CopyFile for mutable files.
func HardlinkFiles(pairs []HardlinkPair) error {
	for _, p := range pairs {
		if err := os.MkdirAll(filepath.Dir(p.Dst), 0o755); err != nil {
			return fmt.Errorf("create staging subdir for %s: %w", p.Dst, err)
		}
		if err := os.Link(p.Src, p.Dst); err != nil {
			return fmt.Errorf("hardlink %s to staging: %w", p.Dst, err)
		}
	}
	return nil
}

// CopyFile copies src to dst with an independent inode — unlike a hardlink,
// later writes to src can't corrupt dst. The destination is fsynced.
//
// NOTE: not atomic (in-place write, no temp+rename), so a crash mid-copy can
// leave a partial dst. Does not create dst's parent directory.
func CopyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create destination: %w", err)
	}
	defer func() {
		if closeErr := out.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close destination: %w", closeErr)
		}
	}()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy data: %w", err)
	}

	return out.Sync()
}
