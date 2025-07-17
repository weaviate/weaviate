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

package types

import (
	"time"

	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// UsageConfig holds configuration for usage data collection and upload
type UsageConfig struct {
	GCSBucket *runtime.DynamicValue[string] `json:"usage_gcs_bucket" yaml:"usage_gcs_bucket"`
	GCSPrefix *runtime.DynamicValue[string] `json:"usage_gcs_prefix" yaml:"usage_gcs_prefix"`

	S3Bucket            *runtime.DynamicValue[string]        `json:"usage_s3_bucket" yaml:"usage_s3_bucket"`
	S3Prefix            *runtime.DynamicValue[string]        `json:"usage_s3_prefix" yaml:"usage_s3_prefix"`
	ScrapeInterval      *runtime.DynamicValue[time.Duration] `json:"usage_scrape_interval" yaml:"usage_scrape_interval"`
	ShardJitterInterval *runtime.DynamicValue[time.Duration] `json:"usage_shard_jitter_interval" yaml:"usage_shard_jitter_interval"`
	PolicyVersion       *runtime.DynamicValue[string]        `json:"usage_policy_version" yaml:"usage_policy_version"`
}
