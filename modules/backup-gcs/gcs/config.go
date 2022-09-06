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

type Config interface {
	BucketName() string
	BackupPath() string
}

type config struct {
	bucket string

	// this is an optional value, allowing for
	// the backup to be stored in a specific
	// directory inside the provided bucket
	backupPath string
}

func NewConfig(bucket, path string) Config {
	return &config{bucket, path}
}

func (c *config) BucketName() string {
	return c.bucket
}

func (c *config) BackupPath() string {
	return c.backupPath
}
