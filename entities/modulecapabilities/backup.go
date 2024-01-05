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

package modulecapabilities

import (
	"context"
	"io"
)

type BackupBackend interface {
	// IsExternal returns whether the storage is an external storage (e.g. gcs, s3)
	IsExternal() bool
	// Name returns backend's name
	Name() string
	// HomeDir is the home directory of all backup files
	HomeDir(backupID string) string

	// GetObject giving backupID and key
	GetObject(ctx context.Context, backupID, key string) ([]byte, error)

	// WriteToFile writes an object in the specified file with path destPath
	// The file will be created if it doesn't exist
	// The file will be overwritten if it exists
	WriteToFile(ctx context.Context, backupID, key, destPath string) error

	// SourceDataPath is data path of all source files
	SourceDataPath() string

	// PutFile reads a file from srcPath and uploads it to the destination folder
	PutFile(ctx context.Context, backupID, key, srcPath string) error
	// PutObject writes bytes to the object with key `key`
	PutObject(ctx context.Context, backupID, key string, byes []byte) error
	// Initialize initializes backup provider and make sure that app have access rights to write into the object store.
	Initialize(ctx context.Context, backupID string) error

	Write(ctx context.Context, backupID, key string, r io.ReadCloser) (int64, error)
	Read(ctx context.Context, backupID, key string, w io.WriteCloser) (int64, error)
}
