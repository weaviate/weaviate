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

package export

import (
	"context"
	"io"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

// backendWrapper wraps a modulecapabilities.BackupBackend to implement BackendStore
type backendWrapper struct {
	backend modulecapabilities.BackupBackend
}

// NewBackendWrapper creates a wrapper around BackupBackend
func NewBackendWrapper(backend modulecapabilities.BackupBackend) BackendStore {
	return &backendWrapper{backend: backend}
}

// HomeDir returns the base path for exports
func (w *backendWrapper) HomeDir(exportID, bucket, path string) string {
	return w.backend.HomeDir(exportID, bucket, path)
}

// Initialize initializes the backend storage
func (w *backendWrapper) Initialize(ctx context.Context, exportID, bucket, path string) error {
	return w.backend.Initialize(ctx, exportID, bucket, path)
}

// PutObject uploads data to the backend
func (w *backendWrapper) PutObject(ctx context.Context, exportID, key, bucket, path string, data io.ReadCloser) (int64, error) {
	return w.backend.Write(ctx, exportID, key, bucket, path, data)
}

// GetObject retrieves data from the backend
func (w *backendWrapper) GetObject(ctx context.Context, exportID, key, bucket, path string) ([]byte, error) {
	return w.backend.GetObject(ctx, exportID, key, bucket, path)
}

// backupBackendProvider defines the interface for getting backup backends
// This matches what modules.Provider provides
type backupBackendProvider interface {
	BackupBackend(backend string) (modulecapabilities.BackupBackend, error)
}

// BackendProviderWrapper wraps backupBackendProvider to implement BackendProvider
type BackendProviderWrapper struct {
	provider backupBackendProvider
}

// NewBackendProviderWrapper creates a wrapper around backupBackendProvider
func NewBackendProviderWrapper(provider backupBackendProvider) BackendProvider {
	return &BackendProviderWrapper{provider: provider}
}

// BackupBackend returns a wrapped backup backend
func (w *BackendProviderWrapper) BackupBackend(backend string) (BackendStore, error) {
	b, err := w.provider.BackupBackend(backend)
	if err != nil {
		return nil, err
	}
	return NewBackendWrapper(b), nil
}
