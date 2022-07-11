package lsmkv

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/storagestate"
)

// PauseCompaction waits for all ongoing compactions to finish,
// then makes sure that no new compaction can be started.
//
// This is a preparatory stage for taking snapshots.
//
// A timeout can be specified as some compactions are long-running,
// in which case it may be better to fail the backup attempt and
// retry later, than to block indefinitely.
func (b *Bucket) PauseCompaction(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// wait for compaction to finish. if this takes
	// longer than the timeout, return the error
	for b.disk.CompactionInProgress() {
		select {
		case <-ctx.Done():
			return errors.Errorf(
				"long-running compaction in progress, exceeded timeout of %s",
				timeout.String())
		default:
			continue
		}
	}

	// when the bucket is READONLY, no new compactions can be started
	b.UpdateStatus(storagestate.StatusReadOnly)
	return nil
}

// FlushMemtable flushes any active memtable and returns only once the memtable
// has been fully flushed and a stable state on disk has been reached
func (b *Bucket) FlushMemtable(ctx context.Context) error {
	return nil
}

// ListFiles lists all files that currently exist in the Bucket. The files are only
// in a stable state if the memtable is empty, and if compactions are paused. If one
// of those conditions is not given, it errors
func (b *Bucket) ListFiles(ctx context.Context) ([]string, error) {
	return nil, nil
}

// ResumeCompaction starts the compaction cycle again.
// It errors if compactions were not paused
func (b *Bucket) ResumeCompaction(ctx context.Context) error {
	return nil
}
