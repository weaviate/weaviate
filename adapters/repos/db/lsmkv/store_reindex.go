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

	// Already holding bucketAccessLock — use the non-locking accessor. Calling
	// the public Bucket() here would take RLock recursively and deadlock if a
	// setBucket writer is waiting (see weaviate/0-weaviate-issues#251).
	b := s.bucket(helpers.ObjectsBucketLSM)

	b.disk.compactionCallbackCtrl.Deactivate(ctx)
	b.doStartPauseTimer()
	return nil
}

// ResumeObjectBucketCompaction resumes the compaction cycle for the objects bucket.
func (s *Store) ResumeObjectBucketCompaction(ctx context.Context) error {
	s.bucketAccessLock.RLock()
	defer s.bucketAccessLock.RUnlock()

	// Non-locking accessor: we already hold bucketAccessLock (see above / #251).
	b := s.bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return fmt.Errorf("no bucket named 'objects' found in store %s", s.dir)
	}

	if err := b.disk.compactionCallbackCtrl.Activate(); err != nil {
		return err
	}

	b.doStopPauseTimer()

	return nil
}
