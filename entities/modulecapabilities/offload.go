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
	// Upload uploads the content of a shard to s3
	// upload shard content assigned to specific node
	// s3://{configured_bucket}/{className}/{shardName}/{nodeName}/{shard content}
	Upload(ctx context.Context, className, shardName, nodeName string) error
	// Download uploads the content of a shard to s3
	// download s3 bucket content to desired node
	// {dataPath}/{className}/{shardName}/{content}
	Download(ctx context.Context, className, shardName, nodeName string) error
	// Delete deletes content of a shard to s3
	// upload shard content assigned to specific node
	// s3://{configured_bucket}/{className}/{shardName}/{nodeName}/{shard content}
	Delete(ctx context.Context, className, shardName, nodeName string) error
}

type OffloadProvider interface {
	OffloadBackend(backend string) (OffloadCloud, error)
}
