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

package postingedge

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// The backup of the index, as a shard takes it (integration-plan.md, "Backup
// (revisado el 23 de noviembre)").
//
// A shard does not stop its writes for a backup. It prepares its indexes,
// flushes the memtables of its store, lists the files of the store and of
// every index, has every index copy the files it changes in place
// (SnapshotMutableFiles), links every other one and resumes. It prepares
// again on every halt, and resumes once, after the last.
//
// The buckets of the index are in the store of the shard: the backup holds
// them as the flush left them. The files of its directory are copied or
// linked after the flush, and the journal tells every write since the
// backup began: while a backup holds the index the journal is not emptied,
// so the copy of it is every write the buckets of the backup may be missing,
// and an index that opens from the backup applies them again. The index a
// backup restores is the one there was when its files were copied. The
// vectors kept aside before there is a base are only in a bucket: of them
// the backup holds what the flush wrote, as of every bucket of the shard.
//
// What else the backup holds back: a pass over the vectors, which ends by
// naming another generation in the meta and removing the files of the one
// the backup links, waits at its next step and none starts; and no build of
// the base starts, nor is a backup taken while one runs.

// PrepareForBackup holds the index for a backup until ResumeAfterBackup. The
// first call syncs the index, so that its journal starts empty; every call
// prepares the centroid graph as an HNSW prepares itself, starting another
// file of its commit log. It fails while the base is built, with the error
// a shard gives to try again later.
func (p *Persistent) PrepareForBackup(ctx context.Context) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.closed:
		return errors.New("prepare for backup: the index is shut down")
	case p.failed != nil:
		return errors.Wrap(p.failed, "prepare for backup: a write stopped halfway and the index stopped")
	case p.building:
		return errors.Wrap(enterrors.ErrShardBusyStructuralOp, "prepare for backup: the index is building its base")
	}
	if p.g != nil {
		if !p.backupHeld {
			if err := p.g.sync(); err != nil {
				return errors.Wrap(err, "prepare for backup")
			}
		}
		if err := p.g.st.centroids.graph.PrepareForBackup(ctx); err != nil {
			return errors.Wrap(err, "prepare the centroid graph for backup")
		}
		p.g.keepJournal = true
	}
	if !p.backupHeld {
		p.backupHeld, p.resumed = true, make(chan struct{})
	}
	return nil
}

// ResumeAfterBackup lets go of the index: the next flush empties the journal,
// and what waited in the background is told to go on. Without a backup that
// holds the index it does nothing, which is what a shard whose halt failed
// calls.
func (p *Persistent) ResumeAfterBackup(ctx context.Context) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.mu.Lock()
	if !p.backupHeld {
		p.mu.Unlock()
		return nil
	}
	p.releaseBackup()
	var err error
	if p.g != nil {
		// The journal holds what it held for the backup, which the next
		// flush hands to the store and forgets.
		p.dirty = true
		err = errors.Wrap(p.g.st.centroids.graph.ResumeAfterBackup(ctx), "resume the centroid graph after backup")
	}
	p.mu.Unlock()
	p.wakeMaintenance()
	return err
}

// releaseBackup ends the hold of a backup. The caller holds writeMu and mu.
func (p *Persistent) releaseBackup() {
	if !p.backupHeld {
		return
	}
	p.backupHeld = false
	close(p.resumed)
	if p.g != nil {
		p.g.keepJournal = false
	}
}

// lockOutsideBackup takes writeMu and mu once no backup holds the index,
// waiting for the one that does.
func (p *Persistent) lockOutsideBackup(ctx context.Context) error {
	for {
		p.writeMu.Lock()
		p.mu.Lock()
		if !p.backupHeld {
			return nil
		}
		resumed := p.resumed
		p.mu.Unlock()
		p.writeMu.Unlock()
		select {
		case <-resumed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ListFiles lists, relative to basePath, the files of the index a backup
// links: those that never change in place. The mean of the quantizer, the
// file of vectors of the generation the meta names, which only grows, and the
// closed files of the commit log of the centroid graph. The directory of a
// build of the base goes too when it holds the list of what the build wrote:
// without it the index that is restored does not open (integration-plan.md,
// section 9). What changes in place is copied by SnapshotMutableFiles, and
// the buckets of the index are listed by the store.
func (p *Persistent) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.backupHeld {
		return nil, errors.New("list the files of the index: no backup holds it")
	}
	var paths, files []string
	if p.g != nil {
		paths = append(paths, filepath.Join(p.dir, diskMeanFile))
		if gen := p.g.meta.VectorGen; gen > 0 {
			paths = append(paths, filepath.Join(p.dir, growVectorsName(gen)))
		}
		graph, err := p.g.st.centroids.graph.ListFiles(ctx, basePath)
		if err != nil {
			return nil, errors.Wrap(err, "list the files of the centroid graph")
		}
		files = append(files, graph...)
	}
	list := filepath.Join(p.buildDir, buildListFile)
	switch _, err := os.Stat(list); {
	case err == nil:
		paths = append(paths, list)
		sources := filepath.Join(p.buildDir, buildSourcesFile)
		if _, err := os.Stat(sources); err == nil {
			paths = append(paths, sources)
		} else if !os.IsNotExist(err) {
			return nil, errors.Wrap(err, "look for the doc ids of a build of the base")
		}
	case !os.IsNotExist(err):
		return nil, errors.Wrap(err, "look for the list of a build of the base")
	}
	for _, path := range paths {
		rel, err := filepath.Rel(basePath, path)
		if err != nil {
			return nil, errors.Wrapf(err, "the path of %s under %s", path, basePath)
		}
		files = append(files, rel)
	}
	return files, nil
}

// SnapshotMutableFiles copies to stagingDir, under their paths relative to
// basePath, the files of the index that change in place, all of them at one
// point between two writes: the meta, the journal, the slots of the vectors,
// the table of when their edges were built, the centroids, and the mark of
// the centroid graph when there is one. Of an index whose vectors are at
// their doc ids, the file of vectors too, which is written in place. It
// returns their paths relative to basePath, and fails unless a backup holds
// the index.
func (p *Persistent) SnapshotMutableFiles(ctx context.Context, basePath, stagingDir string) ([]string, error) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	switch {
	case !p.backupHeld:
		return nil, errors.New("copy the files of the index: no backup holds it")
	case p.closed:
		return nil, errors.New("copy the files of the index: it is shut down")
	case p.failed != nil:
		return nil, errors.Wrap(p.failed, "copy the files of the index: a write stopped halfway and the index stopped")
	case p.g == nil:
		return nil, nil
	}
	g := p.g
	// The meta of now, the commit log of the centroid graph in the system,
	// and the journal kept.
	if err := g.sync(); err != nil {
		return nil, errors.Wrap(err, "sync the index before copying its files")
	}
	names := []string{diskMetaFile, growJournalFile, growBuiltFile, diskCentroidFile}
	if gen := g.meta.VectorGen; gen > 0 {
		names = append(names, growSlotsName(gen))
	} else {
		names = append(names, growVectorsName(0))
	}
	if g.graphMarked {
		names = append(names, growGraphMark)
	}
	rels := make([]string, 0, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		src := filepath.Join(p.dir, name)
		rel, err := filepath.Rel(basePath, src)
		if err != nil {
			return nil, errors.Wrapf(err, "the path of %s under %s", src, basePath)
		}
		if err := copyFileTo(src, filepath.Join(stagingDir, rel)); err != nil {
			return nil, errors.Wrapf(err, "copy %s", name)
		}
		rels = append(rels, rel)
	}
	return rels, nil
}

// RewritesFilesInPlace tells a shard that a copy of the files of the index is
// only whole when SnapshotMutableFiles takes it: the meta, the journal, the
// slots and the centroids are rewritten in place. A backup that uploads the
// files of a live shard from where they are refuses it.
func (p *Persistent) RewritesFilesInPlace() bool {
	return true
}

// copyFileTo copies a file whole to dst, making its directory, and has the
// copy reach the disk.
func copyFileTo(src, dst string) (err error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if closed := out.Close(); err == nil {
			err = closed
		}
	}()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
