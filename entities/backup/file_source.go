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

package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/entities/diskio"
)

const (
	// stagedHashLen is the length of the SHA-256 hex every staged name starts
	// with: two hex digits per digest byte.
	stagedHashLen = 2 * sha256.Size
	// maxStagedNameLen is the longest staged name, whatever a filesystem reports.
	maxStagedNameLen = 255
	// relPathSeparators holds both separators, because on Windows a segment's
	// relPath mixes them.
	relPathSeparators = "/" + string(filepath.Separator)
)

// ShardStaging holds one shard's flat staging dir and the byte length
// StagedFilePath cuts each staged name to.
type ShardStaging struct {
	Dir     string
	NameMax int
}

func NewShardStaging(stagingRoot, shardName string, nameMax int) ShardStaging {
	return ShardStaging{Dir: filepath.Join(stagingRoot, shardName), NameMax: nameMax}
}

// StagedFilePath returns relPath's staged path in s.Dir. Its name is relPath's
// SHA-256 hex, parent dir and base name, cut to s.NameMax bytes and never into
// the hash. s.NameMax must come from StagedNameMax.
func StagedFilePath(s ShardStaging, relPath string) string {
	base, bucket := relPath, ""
	if i := strings.LastIndexAny(relPath, relPathSeparators); i >= 0 {
		base, bucket = relPath[i+1:], relPath[:i]
		if j := strings.LastIndexAny(bucket, relPathSeparators); j >= 0 {
			bucket = bucket[j+1:]
		}
	}

	// For a relPath of up to 512 bytes both arrays stay on the stack, so the
	// call allocates only its result.
	var relBuf [512]byte
	sum := sha256.Sum256(append(relBuf[:0], relPath...))
	var name [maxStagedNameLen]byte
	n := hex.Encode(name[:], sum[:])
	n += copy(name[n:], "-")
	n += copy(name[n:], bucket)
	n += copy(name[n:], "-")
	n += copy(name[n:], base)
	nameLen := min(n, max(s.NameMax, stagedHashLen))
	return s.Dir + string(filepath.Separator) + string(name[:nameLen])
}

// StagedNameMax turns the name limit a filesystem reports into the one
// StagedFilePath cuts to. It returns an error for a limit of 1 to 63 bytes,
// which cannot hold the 64-byte hash.
func StagedNameMax(fsNameMax int) (int, error) {
	if fsNameMax > 0 && fsNameMax < stagedHashLen {
		return 0, fmt.Errorf("the filesystem takes names of at most %d bytes, "+
			"shorter than the %d-byte hash every staged name starts with", fsNameMax, stagedHashLen)
	}
	return stagedNameLimit(fsNameMax), nil
}

// stagedNameLimit returns 255 for a limit of 0 or below, which means unknown,
// and for one above 255.
func stagedNameLimit(nameMax int) int {
	if nameMax <= 0 || nameMax > maxStagedNameLen {
		return maxStagedNameLen
	}
	return nameMax
}

// FileSource resolves a shard file's relPath against the root its constructor
// names, so a reader never joins a root itself. A zero FileSource panics on
// Stat and Open, so build one with NewTreeFileSource or NewFlatFileSource.
type FileSource struct {
	// pathFor maps a relPath to the path holding it, or refuses one the layout
	// does not cover.
	pathFor func(relPath string) (string, error)
}

// NewTreeFileSource returns a source resolving each relPath below root, as the
// live shard tree lays its files out. It refuses a relPath that escapes root,
// without resolving root's symlinks, which would cost a syscall per file.
func NewTreeFileSource(root string) FileSource {
	return FileSource{pathFor: func(relPath string) (string, error) {
		return diskio.JoinInRoot(root, relPath)
	}}
}

// NewFlatFileSource returns a source resolving each relPath to its staged
// name in s.Dir. A hashed name cannot escape s.Dir, so it refuses none.
func NewFlatFileSource(s ShardStaging) FileSource {
	return FileSource{pathFor: func(relPath string) (string, error) {
		return StagedFilePath(s, relPath), nil
	}}
}

// ShardFileSource resolves the files the class staged for shardName.
func (c *ClassDescriptor) ShardFileSource(shardName string) FileSource {
	return NewFlatFileSource(NewShardStaging(c.StagingDir, shardName, c.StagingNameMax))
}

func (s FileSource) Stat(relPath string) (os.FileInfo, error) {
	path, err := s.path(relPath)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %q at %q: %w", relPath, path, err)
	}
	return info, nil
}

func (s FileSource) Open(relPath string) (*os.File, error) {
	path, err := s.path(relPath)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %q at %q: %w", relPath, path, err)
	}
	return f, nil
}

func (s FileSource) path(relPath string) (string, error) {
	path, err := s.pathFor(relPath)
	if err != nil {
		return "", fmt.Errorf("sanitize file path %q: %w", relPath, err)
	}
	return path, nil
}
