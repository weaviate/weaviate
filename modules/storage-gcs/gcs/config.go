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

package gcs

const DEFAULT_BUCKET = "weaviate-snapshots"

type Config interface {
	BucketName() string
	SnapshotRoot() string
}

type config struct {
	bucket string

	// this is an optional value, allowing for
	// the snapshot to be stored in a specific
	// directory inside the provided bucket
	snapshotRoot string
}

func NewConfig(bucket, root string) Config {
	return &config{bucket, root}
}

func (c *config) BucketName() string {
	if len(c.bucket) > 0 {
		return c.bucket
	}
	return DEFAULT_BUCKET
}

func (c *config) SnapshotRoot() string {
	return c.snapshotRoot
}
