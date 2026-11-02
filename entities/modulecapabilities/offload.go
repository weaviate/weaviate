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

package modulecapabilities

import (
	"context"
)

type OffloadCloud interface {
	// VerifyBucket verify if the offload bucket is created
	VerifyBucket(ctx context.Context) error
	// Upload uploads the content of a shard assigned to specific node to
	// cloud provider (S3, Azure Blob storage, Google cloud storage)
	// {cloud_provider}://{configured_bucket}/{className}/{shardName}/{nodeName}/{shard content}
	// Callers pass {attempt}/{node} as nodeName.
	Upload(ctx context.Context, className, shardName, nodeName string) error
	// Download downloads the content of a shard to desired node from
	// cloud provider (S3, Azure Blob storage, Google cloud storage)
	// {dataPath}/{className}/{shardName}/{content}
	Download(ctx context.Context, className, shardName, nodeName string) error
	// DownloadToPath downloads the content of a shard into localPath. It creates
	// nothing if the copy is empty.
	DownloadToPath(ctx context.Context, className, shardName, nodeName, localPath string) error
	// HasData reports whether a copy holds any object.
	HasData(ctx context.Context, className, shardName, nodeName string) (bool, error)
	// ListCopies returns the names directly under a shard's cloud path.
	ListCopies(ctx context.Context, className, shardName string) ([]string, error)
	// Delete deletes content of a shard assigned to specific node in
	// cloud provider (S3, Azure Blob storage, Google cloud storage)
	// Careful: if shardName and nodeName is passed empty it will delete all class frozen shards in cloud storage
	// {cloud_provider}://{configured_bucket}/{className}/{shardName}/{nodeName}/{shard content}
	// Callers pass {attempt} as nodeName to delete every node's copy of it.
	// A class delete keeps the copies Retain marked.
	Delete(ctx context.Context, className, shardName, nodeName string) error
	// Retain marks a shard's {attempt} as one backupID restores from.
	Retain(ctx context.Context, className, shardName, attempt, backupID string) error
	// Retained returns the attempts of a shard that a backup restores from.
	Retained(ctx context.Context, className, shardName string) ([]string, error)
}
