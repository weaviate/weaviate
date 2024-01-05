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

	"github.com/pkg/errors"
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
func (s *Store) PauseCompaction(ctx context.Context) error {
	if err := s.cycleCallbacks.compactionCallbacksCtrl.Deactivate(ctx); err != nil {
		return errors.Wrap(err, "long-running compaction in progress")
	}

	// TODO common_cycle_manager maybe not necessary, or to be replaced with store pause stats
	for _, b := range s.bucketsByName {
		if metric, err := monitoring.GetMetrics().BucketPauseDurations.GetMetricWithLabelValues(b.dir); err == nil {
			b.pauseTimer = prometheus.NewTimer(metric)
		}
	}

	return nil
}

// ResumeCompaction starts the compaction cycle again.
// It errors if compactions were not paused
func (s *Store) ResumeCompaction(ctx context.Context) error {
	s.cycleCallbacks.compactionCallbacksCtrl.Activate()

	// TODO common_cycle_manager maybe not necessary, or to be replaced with store pause stats
	for _, b := range s.bucketsByName {
		if b.pauseTimer != nil {
			b.pauseTimer.ObserveDuration()
		}
	}

	return nil
}

// FlushMemtable flushes any active memtable and returns only once the memtable
// has been fully flushed and a stable state on disk has been reached.
//
// This is a preparatory stage for creating backups.
//
// A timeout should be specified for the input context as some
// flushes are long-running, in which case it may be better
// to fail the backup attempt and retry later, than to block
// indefinitely.
func (s *Store) FlushMemtables(ctx context.Context) error {
	if err := s.cycleCallbacks.flushCallbacksCtrl.Deactivate(ctx); err != nil {
		return errors.Wrap(err, "long-running memtable flush in progress")
	}
	defer s.cycleCallbacks.flushCallbacksCtrl.Activate()

	flushMemtable := func(ctx context.Context, b *Bucket) (interface{}, error) {
		return nil, b.FlushMemtable()
	}
	_, err := s.runJobOnBuckets(ctx, flushMemtable, nil)
	return err
}
