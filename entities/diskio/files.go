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

package diskio

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func FileExists(file string) (bool, error) {
	_, err := os.Stat(file)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func Fsync(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return f.Sync()
}

// SanitizeFilePathJoin joins a root path and a relative file path, ensuring that the resulting path is within the root
// path. It assumes that the relativeFilePath is attacker controlled.
func SanitizeFilePathJoin(rootPath string, relativeFilePath string) (string, error) {
	// Resolve symlinks in root path
	rootPath, err := filepath.EvalSymlinks(rootPath)
	if err != nil {
		return "", fmt.Errorf("resolve symlinks for root path %q: %w", rootPath, err)
	}

	// clean the path to remove any ../ or ./ sequences
	cleanFilePath := filepath.Clean(relativeFilePath)
	if filepath.IsAbs(cleanFilePath) {
		return "", fmt.Errorf("relative file path %q is an absolute path", relativeFilePath)
	}
	combinedPath := filepath.Join(rootPath, cleanFilePath)
	finalPath := filepath.Clean(combinedPath)

	rel, err := filepath.Rel(rootPath, finalPath)
	if err != nil {
		return "", fmt.Errorf("make %q relative to %q: %w", finalPath, rootPath, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("file path %q is outside shard root %q", finalPath, rootPath)
	}
	return finalPath, nil
}
