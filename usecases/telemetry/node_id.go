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
// file does not exist it mints a new UUID and persists it with a tmp+rename
// atomic write, mirroring adapters/repos/db/inverted/new_prop_length_tracker.go.
// The id is tied to the data volume, not the process: it survives restarts and
// resets only if the volume is wiped. Callers on an I/O error fall back to an
// ephemeral id for that boot rather than blocking startup.
func ReadOrCreateNodeID(dataPath string) (string, error) {
	path := filepath.Join(dataPath, nodeIDFileName)

	if b, err := os.ReadFile(path); err == nil {
		if id := strings.TrimSpace(string(b)); id != "" {
			return id, nil
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("read node-id: %w", err)
	}

	id := uuid.NewString()
	tmp := path + ".tmp"
	// 0o600: node-id is private per-node state, not served to clients.
	if err := os.WriteFile(tmp, []byte(id), 0o600); err != nil {
		return "", fmt.Errorf("write node-id tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", fmt.Errorf("rename node-id: %w", err)
	}

	return id, nil
}
