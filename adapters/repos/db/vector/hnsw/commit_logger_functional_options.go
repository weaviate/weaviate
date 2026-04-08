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

package hnsw

import "github.com/weaviate/weaviate/adapters/repos/db/vector/common"

type CommitlogOption func(l *hnswCommitLogger) error

func WithCommitlogThreshold(size int64) CommitlogOption {
	return func(l *hnswCommitLogger) error {
		l.maxSizeIndividual = size
		return nil
	}
}

// WithForceNewFile forces the commit logger to create a new file instead of
// appending to the existing one. This is used after crash recovery to ensure
// we don't append to a potentially corrupted file.
func WithForceNewFile() CommitlogOption {
	return func(l *hnswCommitLogger) error {
		l.forceNewFile = true
		return nil
	}
}

func WithFS(fs common.FS) CommitlogOption {
	return func(l *hnswCommitLogger) error {
		l.fs = fs
		return nil
	}
}
