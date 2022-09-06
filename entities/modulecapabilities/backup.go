//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modulecapabilities

import (
	"context"
)

type BackupBackend interface {
	// HomeDir is the home directory of all backup files
	HomeDir(snapshotID string) string

	// GetObject giving snapshotID and key
	GetObject(ctx context.Context, snapshotID, key string) ([]byte, error)

	// WriteToFile writes an object in the specified file with path destPath
	// The file will be created if it doesn't exist
	// The file will be overwritten if it exists
	WriteToFile(ctx context.Context, snapshotID, key, destPath string) error

	// SourceDataPath is data path of all source files
	SourceDataPath() string

	// PutFile reads a file from srcPath and uploads it to the destination folder
	PutFile(ctx context.Context, snapshotID, key, srcPath string) error
	// PutObject writes bytes to the object with key key
	PutObject(ctx context.Context, snapshotID, key string, byes []byte) error
	// Initialize initializes backup provider and make sure that app have access rights to write into the object store.
	Initialize(ctx context.Context, snapshotID string) error
}
