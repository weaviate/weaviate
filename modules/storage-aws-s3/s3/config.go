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

const (
	DEFAULT_ENDPOINT = "s3.amazonaws.com"
	DEFAULT_BUCKET   = "weaviate-snapshots"
	DEFAULT_ROOT     = "snapshots"
)

type Config interface {
	Endpoint() string
	BucketName() string
	RootName() string
	UseSSL() bool
}

type config struct {
	endpoint string
	bucket   string
	root     string
	useSSL   bool
}

func NewConfig(endpoint, bucket, root string, useSSL bool) Config {
	return &config{endpoint, bucket, root, useSSL}
}

func (c *config) Endpoint() string {
	if len(c.endpoint) > 0 {
		return c.endpoint
	}
	return DEFAULT_ENDPOINT
}

func (c *config) BucketName() string {
	if len(c.bucket) > 0 {
		return c.bucket
	}
	return DEFAULT_BUCKET
}

func (c *config) RootName() string {
	if len(c.root) > 0 {
		return c.root
	}
	return DEFAULT_ROOT
}

func (c *config) UseSSL() bool {
	return c.useSSL
}
