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

package modstgs3

type clientConfig struct {
	Endpoint string
	Bucket   string
	UseSSL   bool

	// this is an optional value, allowing for
	// the backup to be stored in a specific
	// directory inside the provided bucket
	BackupPath string
}

func newConfig(endpoint, bucket, path string, useSSL bool) *clientConfig {
	const DEFAULT_ENDPOINT = "s3.amazonaws.com"
	if endpoint == "" {
		endpoint = DEFAULT_ENDPOINT
	}
	return &clientConfig{endpoint, bucket, useSSL, path}
}
