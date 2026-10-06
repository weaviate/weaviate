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
	"bufio"
	"context"
	errors2 "errors"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	bolterrors "go.etcd.io/bbolt/errors"

	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/usecases/config"
)

var logOnceWhenRecoveringFromWAL sync.Once

// walTooShortForRecord reports whether a .wal file is shorter than one record
// checksum, so it holds no record. newCommitLogger reads its checksum seed from
// the last crc32.Size bytes and cannot open such a file.
func walTooShortForRecord(size int64) bool {
	return size < crc32.Size
}

func (b *Bucket) mayRecoverFromCommitLogs(ctx context.Context, sg *SegmentGroup, files map[string]int64) (err error) {
	// the context is only ever checked once at the beginning, as there is no
	// point in aborting an ongoing recovery. It makes more sense to let it
	// complete and have the next recovery (this is called once per bucket) run
	// into this error. This way in a crashloop we'd eventually recover each
	// bucket until there is nothing left to recover and startup could complete
	// in time
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "recover commit log")
	}

	var walFileNames []string
	for file, size := range files {
		if filepath.Ext(file) != ".wal" {
			// skip, this could be disk segments, etc.
			continue
		}

		path := filepath.Join(b.dir, file)

		if walTooShortForRecord(size) {
			if size > 0 {
				b.logger.WithField("action", "lsm_recover_from_active_wal_corruption").
					WithField("path", path).
					Warnf("removing write-ahead-log of %d bytes, too short to hold a record", size)
			}
			if err := os.Remove(path); err != nil {
				return errors.Wrap(err, "remove wal file without a record")
			}
			continue
		}

		walFileNames = append(walFileNames, file)
	}

	if len(walFileNames) == 0 {
		// nothing to do
		return nil
	}

	// Names are segment-<unix-nano>.wal (fixed-width, so lexicographic == chronological).
	// Recovery relies on order: the last WAL becomes the active memtable, but only
	// where it was replayed whole. The source is a map, whose iteration order is random.
	sort.Strings(walFileNames)

	logOnceWhenRecoveringFromWAL.Do(func() {
		b.logger.WithField("action", "lsm_recover_from_active_wal").
			WithField("path", b.dir).
			Debug("active write-ahead-log found")
	})

	start := time.Now()

	b.metrics.IncWalRecoveryCount(b.strategy)
	b.metrics.IncWalRecoveryInProgress(b.strategy)

	defer func() {
		b.metrics.DecWalRecoveryInProgress(b.strategy)

		if err != nil {
			b.metrics.IncWalRecoveryFailureCount(b.strategy)
			return
		}

		b.metrics.ObserveWalRecoveryDuration(b.strategy, time.Since(start))
	}()

	recovered := false
	memtableThreshold := b.walReplayMaxMemtableSize()

	// Data in these WALs predates any strip progress the edit-ops sidecar has
	// recorded: keeping the last WAL's memtable as the live one would hold
	// pre-strip bytes OUTSIDE the pending-segment bookkeeping, and a drop that
	// already recorded those bytes as stripped would see them resurrect — with
	// nothing left to re-clean them once its op is gone. With ops present,
	// every recovered WAL is flushed into a segment, and that segment is
	// durably pended for every op BEFORE the flush deletes the WAL
	// (PendForAllOps below). The pend is load-bearing: a WAL can hold PRE-ARM
	// bytes the arm's snapshot never covered — sidecars written by an older
	// binary whose b.flushing clobber (since fixed by flushAndSwitchLocked's
	// leftover drain) orphaned a failed flush's memtable, or any future
	// regression of that shape. Without the pend such a segment reads as
	// clean and the dropped vector survives finalize.
	sidecarHasOps := false
	sidecarUsable := false
	if sg.editOps != nil {
		hasOps, opsErr := sg.editOps.HasOps()
		switch {
		case opsErr == nil:
			sidecarHasOps, sidecarUsable = hasOps, true
		case errors2.Is(opsErr, bolterrors.ErrTimeout):
			// Still flocked by a previous instance — same hard-fail as the
			// sidecar recovery in newSegmentGroup: loading blind is how a
			// completed drop's data got resurrected.
			return errors.Wrap(opsErr, "probe edit-ops sidecar before WAL recovery")
		default:
			// Torn/corrupt-but-unlocked sidecar: mirror recoverEditOps's
			// policy — never brick the shard over drop-progress bookkeeping.
			// Fail-safe direction: assume ops exist, so every WAL flushes to
			// segments; the pending cover cannot be recorded through the
			// broken sidecar (sidecarUsable stays false), so the drop stalls
			// on this shard — every sidecar read fails too, which blocks the
			// drain poll and the finalize-time drained check from ever
			// reporting success falsely.
			b.logger.WithField("path", b.dir).
				Warnf("probe edit-ops sidecar before WAL recovery failed; flushing all WALs as a precaution: %v", opsErr)
			sidecarHasOps = true
		}
	}

	// coverRecovered runs on every written-out replay BEFORE its WAL is deleted.
	var coverRecovered func(fname string, segIDs []string) error
	switch {
	case sidecarHasOps && sidecarUsable:
		// Durably cover the written-out segments BEFORE the WAL is deleted.
		// Covering after the delete would leave a crash window in which the
		// WAL is gone and the segments read clean; a startup crash-loop would
		// run that window repeatedly. A crash before the delete leaves the
		// WAL, so removeSegmentsOfSurvivingWALs drops the segments on the next
		// start and the sidecar recovery prunes their rows. Fatal on failure:
		// the probe just read this sidecar cleanly, so a write error is a real
		// anomaly, and deleting the WAL without the cover is exactly the
		// escape this exists to close.
		coverRecovered = func(_ string, segIDs []string) error {
			for _, segID := range segIDs {
				if err := sg.editOps.PendForAllOps(segID); err != nil {
					return errors.Wrap(err, "pend WAL-recovery flush target")
				}
			}
			return nil
		}
	case sidecarHasOps:
		// Torn sidecar: the cover cannot be recorded, and failing the load
		// would brick the shard over bookkeeping. Write out anyway — the
		// broken sidecar blocks every drain/finalize read, so the drop stalls
		// loudly instead of completing falsely.
		coverRecovered = func(fname string, _ []string) error {
			b.logger.WithField("path", b.dir).
				Warnf("flushing WAL %s without recording pending cover (sidecar unreadable); drop-vector cleanup on this shard is stalled until the sidecar is repaired", fname)
			return nil
		}
	}

	// recover from each log
	for i, fname := range walFileNames {
		walForActiveMemtable := i == len(walFileNames)-1 && !sidecarHasOps
		if err := b.recoverFromWAL(sg, fname, files[fname], walForActiveMemtable,
			memtableThreshold, coverRecovered); err != nil {
			return err
		}

		recovered = true
	}

	// force re-sort if any segment was added
	if recovered {
		sort.Slice(sg.segments, func(i, j int) bool {
			return sg.segments[i].getPath() < sg.segments[j].getPath()
		})
	}

	return nil
}

// defaultWALReplayMaxMemtableSize caps a replay chunk on a bucket whose memtable
// resizer is absent or inactive. BenchmarkWALReplay sweeps the threshold around it.
const defaultWALReplayMaxMemtableSize = config.DefaultPersistenceMemtablesMaxSize * 1024 * 1024

// walReplayMaxMemtableSize is the held size at which a replay cuts a chunk, as
// Memtable.Size or the replace cache counts it. That is record payload, and the
// heap behind a chunk runs several times larger. An active resizer supplies its max.
func (b *Bucket) walReplayMaxMemtableSize() uint64 {
	// the size a memtable may reach, not the size this bucket is flushing at: the
	// resizer exists to track load, and a startup has none to track. Fewer, larger
	// segments also leave less for sg.add to recount per chunk.
	if b.memtableResizer != nil && b.memtableResizer.active {
		return uint64(b.memtableResizer.Max())
	}

	return defaultWALReplayMaxMemtableSize
}

// chunkSegmentPath gives chunk 0 the WAL's own name, so a whole-WAL replay and the
// first chunk of a chunked one write the same file.
func chunkSegmentPath(dir string, baseID int64, chunk int) string {
	return segmentPathForID(dir, baseID+int64(chunk))
}

// logReplayProblems reports what a replay could not do but carried on past. A
// damaged tail and a refused entry both leave the bucket short of what the
// write-ahead-log held, and neither fails the open.
func (b *Bucket) logReplayProblems(walPath string, errRecovery error, parser *commitloggerParser) {
	if errRecovery != nil {
		b.logger.WithField("action", "lsm_recover_from_active_wal_corruption").
			WithField("path", walPath).
			Errorf("write-ahead-log ended abruptly, some elements may not have been recovered: %v",
				errRecovery)
	}

	if parser.memtableRejectErr != nil {
		b.logger.WithField("action", "lsm_recover_from_active_wal_refused_entry").
			WithField("path", walPath).
			WithField("refused", parser.refusedEntries).
			Errorf("the memtable refused %d entries of %q, so they reached no segment and the write-ahead-log is removed regardless. The first: %v",
				parser.refusedEntries, walPath, parser.memtableRejectErr)
	}
}

// writeOutReplay stages the replay's tail and commits the whole run. The
// write-ahead-log is still the only copy until it returns, so every failure
// leaves it where it is.
func (b *Bucket) writeOutReplay(run *recoveredRun, mt *Memtable, walPath string) error {
	// the tail commits with the chunks, so a crash between the two cannot leave
	// one mounted without the other
	if err := run.stage(mt); err != nil {
		run.discardStaged(b.logger)
		b.logAbandonedReplay(walPath, run.committed, "write the tail", err)
		return errors.Wrapf(err, "write the tail of write-ahead-log %q", walPath)
	}

	if err := run.commit(); err != nil {
		// the chunks already renamed keep their final names, and discardStaged
		// removes the staged spelling, which is no longer theirs
		run.discardStaged(b.logger)
		b.logAbandonedReplay(walPath, run.committed, "commit the replay", err)
		return errors.Wrapf(err, "commit the replay of write-ahead-log %q", walPath)
	}

	return nil
}

// logAbandonedReplay reports a replay that stopped with segments already on
// disk. Those are provisional only while the WAL is there.
func (b *Bucket) logAbandonedReplay(walPath string, committed int, stage string, err error) {
	b.logger.WithField("action", "lsm_recover_from_active_wal_abandoned").
		WithField("path", walPath).
		WithField("committed_chunks", committed).
		Errorf("could not %s of %q; %d chunks are committed and left in place; free space or remount and restart, and leave the write-ahead-log where it is: %v",
			stage, walPath, committed, err)
}

// recoveredRun collects the segments one WAL replay has written. Each is staged under
// DeleteMarkerSuffix, which newSegmentGroup's mount loop deletes rather than opens, so
// a replay interrupted before commit leaves nothing mounted. commit's rename loop is
// itself interruptible, and removeSegmentsOfSurvivingWALs is what covers that window.
type recoveredRun struct {
	sg  *SegmentGroup
	dir string
	// baseSegmentID is the id chunk 0 takes, and chunk n takes baseSegmentID+n.
	baseSegmentID int64
	stagedPaths   []string
	// chunkingAllowed reports whether this WAL's name can derive chunk ids the
	// cleanup walk would follow. A run that may chunk still writes one segment
	// when neither threshold fires.
	chunkingAllowed bool
	// avgPropLength and propLengthCount carry the inverted corpus across the run's
	// chunks. sg.add runs only once the whole run commits, so without them every
	// chunk would encode its block bounds against the corpus standing before it.
	avgPropLength   float64
	propLengthCount uint64
	// committed counts the chunks already renamed into place; a commit that fails
	// part-way leaves them on disk, and only the surviving WAL marks them provisional
	committed int
}

// stage writes mt out under a staged name. An empty memtable writes no file and
// consumes no id, which is what keeps the committed run gapless.
func (r *recoveredRun) stage(mt *Memtable) error {
	if mt.Size() == 0 {
		return nil
	}

	if mt.strategy == StrategyInverted {
		mt.averagePropLength, mt.propLengthCount = r.avgPropLength, r.propLengthCount
	}

	// a run that cannot chunk is never cut, so this is the whole replay and it
	// takes the name the memtable carries, the file an unchunked replay wrote
	segment := mt.path
	if r.chunkingAllowed {
		segment = chunkSegmentPath(r.dir, r.baseSegmentID, len(r.stagedPaths))
	}

	path, err := mt.writeSegmentTo(segment, DeleteMarkerSuffix)
	if err != nil {
		return err
	}

	// the write blended this chunk's rows into the pair, so the next chunk is
	// stamped with a corpus that includes this one
	if mt.strategy == StrategyInverted {
		r.avgPropLength, r.propLengthCount = mt.averagePropLength, mt.propLengthCount
	}

	r.stagedPaths = append(r.stagedPaths, path)

	return nil
}

// discardStaged removes the chunks of a run that will not be committed. A failed
// remove is not an error: the mount loop deletes the leftovers on the next start.
func (r *recoveredRun) discardStaged(logger logrus.FieldLogger) {
	for _, staged := range r.stagedPaths {
		if err := os.Remove(staged); err != nil && !os.IsNotExist(err) {
			logger.WithField("action", "lsm_recover_from_active_wal_discard_chunk").
				WithField("path", staged).
				Warnf("could not remove the staged chunk of an abandoned replay: %v", err)
		}
	}
}

// commit renames every staged segment into place. The caller may dispose of the
// WAL only once this returns, because until then the run is indistinguishable
// from one that never ran.
func (r *recoveredRun) commit() error {
	for _, staged := range r.stagedPaths {
		final := strings.TrimSuffix(staged, DeleteMarkerSuffix)

		// a chunk id is the WAL's id plus its position and nothing reserves those
		// ids, so a clock that stepped back can leave a live segment where this run
		// is about to write. os.Rename would replace it silently.
		if _, err := os.Stat(final); err == nil {
			return errors.Errorf("commit recovered segment %q: a segment already exists at that id", final)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return errors.Wrapf(err, "look for an existing segment at %q", final)
		}

		if err := os.Rename(staged, final); err != nil {
			return errors.Wrapf(err, "commit recovered segment %q", final)
		}
		// counted on the rename, not the fsync below: the file already carries its
		// final name, so a failed fsync leaves it behind like any committed chunk
		r.committed++

		// each rename is durable before the next is issued, so an interrupted commit
		// leaves a prefix of the run rather than an arbitrary subset of it. The next
		// start's cleanup walk relies on that to stop at the first missing id.
		if err := diskio.Fsync(r.dir); err != nil {
			return errors.Wrapf(err, "fsync segment directory %q", r.dir)
		}
	}

	return nil
}

// segmentIDs names the segments the run committed.
func (r *recoveredRun) segmentIDs() []string {
	ids := make([]string, 0, len(r.stagedPaths))
	for _, staged := range r.stagedPaths {
		ids = append(ids, segmentID(strings.TrimSuffix(staged, DeleteMarkerSuffix)))
	}
	return ids
}

// mount adds the committed segments to the group. A failure fails the bucket open
// but loses nothing. The next start reads the segments off disk and rebuilds
// whatever sidecar this one did not reach.
func (r *recoveredRun) mount() error {
	for _, staged := range r.stagedPaths {
		if err := r.sg.add(strings.TrimSuffix(staged, DeleteMarkerSuffix)); err != nil {
			return err
		}
	}

	return nil
}

// recoverFromWAL replays one WAL, either adopting the memtable it built or writing
// it out as a run of segments and unlinking the log. A chunked WAL leaves nothing to
// adopt, and adopting it would overwrite chunk 0 on its next flush.
func (b *Bucket) recoverFromWAL(sg *SegmentGroup, fname string, walSize int64,
	forActiveMemtable bool, memtableThreshold uint64,
	coverRecovered func(fname string, segIDs []string) error,
) error {
	path := filepath.Join(b.dir, strings.TrimSuffix(fname, ".wal"))
	walPath := filepath.Join(b.dir, fname)
	start := time.Now()

	cl, err := newCommitLogger(path, b.strategy, walSize)
	if err != nil {
		return errors.Wrap(err, "init commit logger")
	}

	// the commit log is handed to b.active on one path and closed on every other,
	// so each terminal arm releases it and the rest are covered here
	ownsCommitLog := true
	defer func() {
		if ownsCommitLog {
			cl.close()
		}
	}()

	cl.pause()
	defer cl.unpause()

	mt, err := b.newMemtableAt(cl, path)
	if err != nil {
		return err
	}

	if _, err := cl.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	parser, run, readWALBytes := b.newWALParser(sg, cl, mt, fname, memtableThreshold)

	errRecovery := parser.Do()

	if parser.chunkWriteErr != nil {
		// returning the error aborts startup, so the next start is the next
		// crashloop iteration. Remove the chunks now rather than leaving them on a
		// disk that has just filled up.
		run.discardStaged(b.logger)

		// the index is the one stage would derive next, so it names the chunk the
		// write was on whichever of writeChunk's two failure points fired
		chunkPath := chunkSegmentPath(run.dir, run.baseSegmentID, len(run.stagedPaths))

		b.logger.WithField("action", "lsm_recover_from_active_wal_chunk_write").
			WithField("path", walPath).
			WithField("chunk_path", chunkPath).
			Errorf("could not write chunk %q of the write-ahead-log; nothing was committed, so %q may be moved aside to start without it: %v",
				chunkPath, walPath, parser.chunkWriteErr)

		return errors.Wrapf(parser.chunkWriteErr, "write chunk %q of write-ahead-log %q",
			chunkPath, walPath)
	}

	b.logReplayProblems(walPath, errRecovery, parser)

	// only a memtable holding everything the WAL says can stand in for it, so the
	// log survives adoption: nothing cut, nothing lost, nothing refused
	replayIsFaithful := len(run.stagedPaths) == 0 && errRecovery == nil &&
		parser.memtableRejectErr == nil

	if forActiveMemtable && replayIsFaithful {
		if err := b.adoptAsActiveMemtable(sg, cl, parser.memtable); err != nil {
			return err
		}
		ownsCommitLog = false
	} else {
		if err := b.writeOutReplay(run, parser.memtable, walPath); err != nil {
			return err
		}

		if coverRecovered != nil {
			if err := coverRecovered(fname, run.segmentIDs()); err != nil {
				return err
			}
		}

		if err := cl.close(); err != nil {
			return errors.Wrap(err, "close commit log file")
		}
		ownsCommitLog = false

		// unlinking the WAL is the single commit point for every segment it
		// produced, so it happens only once all of them are on disk. Mounting
		// them is not part of that: it reads what commit already made durable,
		// and holding the WAL across it would widen the window in which a build
		// without the cleanup walk sees the chunks beside a live WAL.
		if err := cl.delete(); err != nil {
			return errors.Wrap(err, "delete commit log file")
		}

		if err := run.mount(); err != nil {
			// the log is already unlinked, so an operator looking for it finds
			// nothing and the segments are easy to mistake for lost
			b.logger.WithField("action", "lsm_recover_from_active_wal_mount").
				WithField("path", walPath).
				WithField("committed_chunks", run.committed).
				Errorf("replayed %q into %d segments and removed it, but could not mount them. They are on disk and the next start reads them: %v",
					walPath, run.committed, err)

			return errors.Wrapf(err, "mount the segments replayed from write-ahead-log %q", walPath)
		}
	}

	if b.strategy == StrategyReplace && b.monitorCount {
		// having just flushed the memtable we now have the most up2date count which
		// is a good place to update the metric
		b.metrics.ObjectCount(sg.count())
	}

	success := b.logger.WithField("action", "lsm_recover_from_active_wal_success").
		WithField("path", walPath).
		WithField("chunks", len(run.stagedPaths))

	if len(run.stagedPaths) > 1 {
		success.Infof("replayed %d bytes of write-ahead-log as %d segments in %s",
			readWALBytes(), len(run.stagedPaths), time.Since(start))
	} else {
		success.Debugf("recovered from the write-ahead-log in %s", time.Since(start))
	}

	return nil
}

// newWALParser wires the parser to a run of staged chunks. A WAL whose name
// carries no id, or one whose id does not survive a parse, cannot name its
// chunks and is replayed whole.
func (b *Bucket) newWALParser(sg *SegmentGroup, cl *commitLogger, mt *Memtable,
	fname string, memtableThreshold uint64,
) (*commitloggerParser, *recoveredRun, func() int64) {
	// counts bytes pulled from the file, so it runs ahead of what the parser has
	// consumed by at most one bufio fill
	var walBytesRead int64
	readWALBytes := func() int64 { return walBytesRead }
	meteredReader := diskio.NewMeteredReader(cl.file, func(read, nanoseconds int64) {
		walBytesRead += read
		b.metrics.TrackStartupReadWALDiskIO(read, nanoseconds)
	})
	parser := newCommitLoggerParser(b.strategy, bufio.NewReaderSize(meteredReader, 32*1024), mt)

	// chunking a name removeSegmentsOfSurvivingWALs declines would put chunks at
	// ids nothing ever cleans up, so both ask isSegmentWALName
	baseID, canonical := canonicalSegmentTimestamp(fname)

	run := &recoveredRun{
		sg: sg, dir: b.dir, baseSegmentID: baseID,
		chunkingAllowed: canonical && isSegmentWALName(fname),
	}
	run.avgPropLength, run.propLengthCount = sg.GetAveragePropertyLength()
	if !run.chunkingAllowed {
		return parser, run, readWALBytes
	}

	parser.setChunkedReplay(chunkedReplay{
		memtableThreshold: memtableThreshold,
		walThreshold:      int64(b.walThreshold),
		readWALBytes:      readWALBytes,
		writeChunk: func(full *Memtable) (*Memtable, error) {
			if err := run.stage(full); err != nil {
				return nil, err
			}

			// only the first memtable can be adopted, so only it needs the real log.
			// A successor holding it would delete the WAL mid-replay if anything ever
			// reached for flush instead of writeSegmentTo.
			return b.newMemtableAt(&noopMemtableCommitLogger{}, mt.path)
		},
	})

	return parser, run, readWALBytes
}

// adoptAsActiveMemtable hands the commit log and the memtable it was replayed
// into to the bucket, so writes append to the WAL that is already there.
func (b *Bucket) adoptAsActiveMemtable(sg *SegmentGroup, cl *commitLogger, mt *Memtable) error {
	if mt.strategy == StrategyInverted {
		mt.averagePropLength, mt.propLengthCount = sg.GetAveragePropertyLength()
	}

	if _, err := cl.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	b.active = mt

	return nil
}
