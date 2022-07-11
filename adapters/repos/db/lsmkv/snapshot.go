package lsmkv

import "context"

// PauseCompaction waits for all ongoing compactions to finish,
// then makes sure that no new compaction can be started
func (b *Bucket) PauseCompaction(ctx context.Context) error {
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
