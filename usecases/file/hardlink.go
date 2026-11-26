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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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
// A non-nil error means the probe could not decide; the boolean is then
// meaningless.
func ProbeHardlinkSupport(dir string) (bool, error) {
	// Test-only: forces an unsupported result without needing a filesystem that lacks hard links.
	if os.Getenv("WEAVIATE_TEST_FORCE_NO_HARDLINK") == "true" {
		return false, nil
	}

	f, err := os.CreateTemp(dir, ".hardlink-probe-*")
	if err != nil {
		return false, fmt.Errorf("create probe file: %w", err)
	}
	src := f.Name()
	f.Close()
	defer os.Remove(src)

	dst := src + ".link"
	defer os.Remove(dst)

	if err := os.Link(src, dst); err != nil {
		if hardlinkUnsupported(err) {
			return false, nil
		}
		return false, fmt.Errorf("link probe file: %w", err)
	}
	return true, nil
}

// hardlinkUnsupported reports whether a Link error means the filesystem refuses
// hard links. EPERM counts as a refusal. Linux protected_hardlinks only blocks
// links to files the caller does not own, and the probe links a file it just
// created in the same directory.
func hardlinkUnsupported(err error) bool {
	return errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.EOPNOTSUPP) ||
		errors.Is(err, syscall.EXDEV) ||
		errors.Is(err, syscall.EPERM) ||
		errors.Is(err, syscall.ENOSYS)
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
	var lastDir string
	for _, p := range pairs {
		// Callers list a directory's files in one run, so MkdirAll's stat runs
		// once per directory rather than once per file.
		if dir := filepath.Dir(p.Dst); dir != lastDir {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return fmt.Errorf("create staging subdir for %s: %w", p.Dst, err)
			}
			lastDir = dir
		}
		if err := os.Link(p.Src, p.Dst); err != nil {
			return fmt.Errorf("hardlink %s to staging: %w", p.Dst, err)
		}
	}
	return nil
}

// CopyFile copies src to dst with an independent inode — unlike a hardlink,
// later writes to src can't corrupt dst. CopyFile neither fsyncs dst nor writes
// it atomically, so a crash can leave dst partial. Callers must create dst's
// parent directory and copy only into staging that startup deletes.
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

	return nil
}
