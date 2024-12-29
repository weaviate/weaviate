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
)

type OffloadCloud interface {
	// VerifyBucket verify if the offload bucket is created
	VerifyBucket(ctx context.Context) error
	// Upload uploads the content of a shard assigned to specific node to
	// cloud provider (S3, Azure Blob storage, Google cloud storage)
	// {cloud_provider}://{configured_bucket}/{className}/{shardName}/{nodeName}/{shard content}
	Upload(ctx context.Context, className, shardName, nodeName string) error
	// Download downloads the content of a shard to desired node from
	// cloud provider (S3, Azure Blob storage, Google cloud storage)
	// {dataPath}/{className}/{shardName}/{content}
	Download(ctx context.Context, className, shardName, nodeName string) error
	// Delete deletes content of a shard assigned to specific node in
	// cloud provider (S3, Azure Blob storage, Google cloud storage)
	// Careful: if shardName and nodeName is passed empty it will delete all class frozen shards in cloud storage
	// {cloud_provider}://{configured_bucket}/{className}/{shardName}/{nodeName}/{shard content}
	Delete(ctx context.Context, className, shardName, nodeName string) error
}
