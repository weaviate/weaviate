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
	"fmt"
	"os"

	"github.com/weaviate/weaviate/entities/diskio"
)

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

// ShardFileSource resolves the files the class staged for shardName. Each
// relPath already names its shard, so every shard resolves under the one
// StagingDir root.
func (c *ClassDescriptor) ShardFileSource(shardName string) FileSource {
	return NewTreeFileSource(c.StagingDir)
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
