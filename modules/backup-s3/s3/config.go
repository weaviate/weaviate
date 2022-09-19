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

package s3

const DEFAULT_ENDPOINT = "s3.amazonaws.com"

type Config interface {
	Endpoint() string
	BucketName() string
	BackupPath() string
	UseSSL() bool
}

type config struct {
	endpoint string
	bucket   string
	useSSL   bool

	// this is an optional value, allowing for
	// the backup to be stored in a specific
	// directory inside the provided bucket
	backupPath string
}

func NewConfig(endpoint, bucket, path string, useSSL bool) Config {
	return &config{endpoint, bucket, useSSL, path}
}

func (c *config) Endpoint() string {
	if len(c.endpoint) > 0 {
		return c.endpoint
	}
	return DEFAULT_ENDPOINT
}

func (c *config) BucketName() string {
	return c.bucket
}

func (c *config) BackupPath() string {
	return c.backupPath
}

func (c *config) UseSSL() bool {
	return c.useSSL
}
