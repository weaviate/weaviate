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

package lsmkv

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// PauseCompaction waits for all ongoing compactions to finish,
// then makes sure that no new compaction can be started.
//
// This is a preparatory stage for creating backups.
//
// A timeout should be specified for the input context as some
// compactions are long-running, in which case it may be better
// to fail the backup attempt and retry later, than to block
// indefinitely.
func (s *Store) PauseObjectBucketCompaction(ctx context.Context) error {
	s.bucketAccessLock.RLock()
	defer s.bucketAccessLock.RUnlock()

	b := s.bucketsByName["objects"]

	b.disk.compactionCallbackCtrl.Deactivate(ctx)
	label := b.GetDir()
	if monitoring.GetMetrics().Group {
		label = "n/a"
	}
	if metric, err := monitoring.GetMetrics().BucketPauseDurations.GetMetricWithLabelValues(label); err == nil {
		b.pauseTimer = prometheus.NewTimer(metric)
	}
	return nil
}

// ResumeCompaction starts the compaction cycle again.
// It errors if compactions were not paused
func (s *Store) ResumeObjectBucketCompaction(ctx context.Context) error {
	s.bucketAccessLock.RLock()
	defer s.bucketAccessLock.RUnlock()

	b := s.bucketsByName["objects"]
	if b == nil {
		return fmt.Errorf("no bucket named 'objects' found in store %s", s.dir)
	}

	if err := b.disk.compactionCallbackCtrl.Activate(); err != nil {
		return err
	}
	s.bucketAccessLock.RLock()
	defer s.bucketAccessLock.RUnlock()

	if b.pauseTimer != nil {
		b.pauseTimer.ObserveDuration()
	}

	return nil
}
