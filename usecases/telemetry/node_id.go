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

package telemetry

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

const nodeIDFileName = "node-id"

// ReadOrCreateNodeID reads the stable per-node UUID from dataPath/node-id,
// minting one on first use. It writes via unique-tmp + os.Link + read-back:
// Link (not Rename) fails EEXIST when another caller already won the create
// race, so the loser reads back and returns the winner's id instead of
// clobbering it. The id persists across restarts until the data volume is
// wiped.
func ReadOrCreateNodeID(dataPath string) (string, error) {
	path := filepath.Join(dataPath, nodeIDFileName)

	if id, ok, err := readExistingNodeID(path); err != nil {
		return "", err
	} else if ok {
		return id, nil
	}

	tmp, err := writeNodeIDTmp(dataPath, uuid.NewString())
	if err != nil {
		return "", err
	}
	// Removes tmp on every exit path. After a successful Link, tmp and path
	// are separate directory entries for the same inode, so this never
	// touches path.
	defer func() { _ = os.Remove(tmp) }()

	if err := os.Link(tmp, path); err != nil {
		if !os.IsExist(err) {
			return "", fmt.Errorf("link node-id: %w", err)
		}
		// EEXIST: either a concurrent caller won the race (read and return its
		// real id), or path holds an empty/torn-write leftover, which Link
		// can't overwrite but Rename can.
		if winnerID, ok, err := readExistingNodeID(path); err != nil {
			return "", err
		} else if ok {
			return winnerID, nil
		}
		if err := os.Rename(tmp, path); err != nil {
			return "", fmt.Errorf("rename node-id over empty file: %w", err)
		}
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("re-read node-id after write: %w", err)
	}
	id := strings.TrimSpace(string(b))
	if id == "" {
		return "", fmt.Errorf("re-read node-id after write: file is empty")
	}
	return id, nil
}

func readExistingNodeID(path string) (id string, ok bool, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("read node-id: %w", err)
	}
	if id := strings.TrimSpace(string(b)); id != "" {
		return id, true, nil
	}
	return "", false, nil
}

func writeNodeIDTmp(dataPath, content string) (string, error) {
	// os.CreateTemp gives each caller a unique tmp name, so concurrent
	// callers don't stomp each other before Link picks a winner.
	f, err := os.CreateTemp(dataPath, nodeIDFileName+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("create node-id tmp: %w", err)
	}
	tmp := f.Name()
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		return tmp, fmt.Errorf("write node-id tmp: %w", err)
	}
	// 0o600: node-id is private per-node state, not served to clients.
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return tmp, fmt.Errorf("chmod node-id tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return tmp, fmt.Errorf("close node-id tmp: %w", err)
	}
	return tmp, nil
}
