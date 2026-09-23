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

// ReadOrCreateNodeID reads the stable per-node UUID from dataPath/node-id. If the
// file does not exist it mints a new UUID and persists it with a unique-tmp +
// os.Link + read-back atomic write. os.Link is used instead of os.Rename for the
// create step because Link fails with EEXIST when another caller already won the
// race, which is the signal to stop minting and read the winner's file instead;
// Rename would silently clobber the winner. Every caller, winner or loser, always
// returns what is actually on disk, never the value it locally minted, so
// concurrent first-boot callers on one data volume (e.g. embedded mode, which
// defaults multiple instances to a shared data path) converge on one id.
// The id is tied to the data volume, not the process: it survives restarts and
// resets only if the volume is wiped. Callers on an I/O error fall back to an
// ephemeral id for that boot rather than blocking startup.
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
	// Clean up the tmp file on any exit path: on the happy Link, the file has
	// already been consumed as the target's content and the target keeps its
	// own inode, so removing the tmp name is a no-op that doesn't touch path.
	defer func() { _ = os.Remove(tmp) }()

	if err := os.Link(tmp, path); err != nil {
		if !os.IsExist(err) {
			return "", fmt.Errorf("link node-id: %w", err)
		}
		// EEXIST means path already exists. Two causes, and they need different
		// handling: (1) a concurrent caller already won the race and its content
		// is a real id, so read and return it; (2) path holds an empty or
		// whitespace-only file left over from a torn write (readExistingNodeID
		// above already treats that as "absent", which is how we got here), and
		// Link can never claim an already-existing name, so fall through to
		// Rename, which atomically replaces the target regardless of its
		// current content.
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
	// A per-caller unique tmp name (os.CreateTemp) avoids concurrent callers
	// stomping the same tmp path before the create-if-absent Link below decides
	// a winner.
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
