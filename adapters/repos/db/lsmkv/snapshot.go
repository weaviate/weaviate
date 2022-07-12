package lsmkv

import (
	"context"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/storagestate"
)

// PauseCompaction waits for all ongoing compactions to finish,
// then makes sure that no new compaction can be started.
//
// This is a preparatory stage for taking snapshots.
//
// A timeout should be specified for the input context as some
// compactions are long-running, in which case it may be better
// to fail the backup attempt and retry later, than to block
// indefinitely.
func (b *Bucket) PauseCompaction(ctx context.Context) error {
	// wait for compaction to finish. if this takes
	// longer than the timeout, return the error
	for b.disk.CompactionInProgress() {
		select {
		case <-ctx.Done():
			return errors.New(
				"long-running compaction in progress, context deadline exceeded")
		default:
			continue
		}
	}

	// stopping this for a snapshot makes sense, otherwise the
	// compaction cycle will continuously emit log messages
	// indicating that there is nothing to compact. the cycle
	// is resumed with a call to ResumeCompaction
	b.disk.stopCompactionCycle <- struct{}{}

	// when the bucket is READONLY, no new compactions can be started
	if !b.isReadOnly() {
		b.UpdateStatus(storagestate.StatusReadOnly)
	}
	return nil
}

// FlushMemtable flushes any active memtable and returns only once the memtable
// has been fully flushed and a stable state on disk has been reached.
//
// A timeout should be specified for the input context as some
// flushes are long-running, in which case it may be better
// to fail the backup attempt and retry later, than to block
// indefinitely.
func (b *Bucket) FlushMemtable(ctx context.Context) error {
	// when the bucket is READONLY, no new compactions can be started
	if !b.isReadOnly() {
		b.UpdateStatus(storagestate.StatusReadOnly)
	}

	b.stopFlushCycle <- struct{}{}

	select {
	case <-ctx.Done():
		return errors.New("long-running flush in progress, context deadline exceeded")
	default:
		if b.active != nil {
			if err := b.active.flush(); err != nil {
				return errors.Errorf("failed to flush active memtable: %s", err)
			}
		}
	}

	select {
	case <-ctx.Done():
		return errors.New("long-running flush in progress, context deadline exceeded")
	default:
		if b.flushing != nil {
			if err := b.flushing.flush(); err != nil {
				return errors.Errorf("failed to flush flushing memtable: %s", err)
			}
		}
	}

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
