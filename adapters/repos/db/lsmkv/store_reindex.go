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

package lsmkv

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// PauseObjectBucketCompaction pauses the compaction cycle for the objects bucket.
// This is so that the BMW migration can run without interference from the
// compaction process, as they both use the same locks.
func (s *Store) PauseObjectBucketCompaction(ctx context.Context) error {
	s.bucketAccessLock.RLock()
	defer s.bucketAccessLock.RUnlock()

	// Bucket() would recursively RLock and deadlock against a queued writer (weaviate/0-weaviate-issues#251).
	b := s.bucketNoLock(helpers.ObjectsBucketLSM)
	if b == nil {
		return fmt.Errorf("no bucket named 'objects' found in store %s", s.dir)
	}

	return b.pauseCompaction(ctx)
}

// ResumeObjectBucketCompaction resumes the compaction cycle for the objects bucket.
func (s *Store) ResumeObjectBucketCompaction(ctx context.Context) error {
	s.bucketAccessLock.RLock()
	defer s.bucketAccessLock.RUnlock()

	b := s.bucketNoLock(helpers.ObjectsBucketLSM)
	if b == nil {
		return fmt.Errorf("no bucket named 'objects' found in store %s", s.dir)
	}

	return b.resumeCompaction(ctx)
}
