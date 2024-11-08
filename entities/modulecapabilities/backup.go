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

	"github.com/weaviate/weaviate/entities/backup"
)

type BackupBackend interface {
	// IsExternal returns whether the storage is an external storage (e.g. gcs, s3)
	IsExternal() bool
	// Name returns backend's name
	Name() string
	// HomeDir is the base storage location of all backup files, which can be a bucket, a directory, etc.
	HomeDir(backupID, overrideBucket, overridePath string) string

	// GetObject giving backupID and key
	GetObject(ctx context.Context, backupID, key, overrideBucket, overridePath string) ([]byte, error)
	// AllBackups returns the top level metadata for all attempted backups
	AllBackups(ctx context.Context) ([]*backup.DistributedBackupDescriptor, error)

	// WriteToFile writes an object in the specified file with path destPath
	// The file will be created if it doesn't exist
	// The file will be overwritten if it exists
	WriteToFile(ctx context.Context, backupID, key, destPath, overrideBucket, overridePath string) error

	// SourceDataPath is data path of all source files
	SourceDataPath() string

	// PutObject writes bytes to the object with key `key`
	// bucketName and bucketPath override the initialised bucketName and bucketPath
	PutObject(ctx context.Context, backupID, key, overrideBucket, overridePath string, byes []byte) error

	// Initialize initializes backup provider and make sure that app have access rights to write into the object store.
	Initialize(ctx context.Context, backupID, overrideBucket, overridePath string) error

	// Write writes the content of the reader to the object with key
	// bucketName and bucketPath override the initialised bucketName and bucketPath
	// Allows restores from a different bucket to the designated backup bucket
	Write(ctx context.Context, backupID, key, overrideBucket, overridePath string, r io.ReadCloser) (int64, error)
	Read(ctx context.Context, backupID, key, overrideBucket, overridePath string, w io.WriteCloser) (int64, error)
}
