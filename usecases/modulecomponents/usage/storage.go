//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package usage

import (
	"context"

	"github.com/weaviate/weaviate/cluster/usage/types"
)

// StorageConfig contains common configuration fields for storage backends
type StorageConfig struct {
	Bucket            string
	Prefix            string
	NodeID            string
	Version           string
	VerifyPermissions bool
}

// StorageBackend defines the interface that storage implementations must implement
type StorageBackend interface {
	// VerifyPermissions checks if the backend can access the storage location
	VerifyPermissions(ctx context.Context) error

	// UploadUsageData uploads the usage data to the storage backend
	UploadUsageData(ctx context.Context, usage *types.Report) error

	// Close cleans up any resources used by the storage backend
	Close() error

	// UpdateConfig updates the backend configuration from the provided config
	UpdateConfig(config StorageConfig) (bool, error) // returns true if any changes were made
}
