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

package db

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/replication/changelog"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/diskio"
)

var (
	errNoSuchChangeLog = errors.New("shard: " + changelog.ErrMsgNoActiveChangeCaptureLog + " for that op-id")
	errChangeLogLost   = errors.New("shard: " + changelog.ErrMsgChangeLogLost + " for that op-id, writes it captured are gone")
)

const (
	changelogDirName       = "changelog"
	changelogFileExtension = ".log"
	// A 0-byte <op>.lost marker outlives a log discarded before the movement stopped it.
	changelogLostExtension = ".lost"
	// Kept small because retries run under docIdLock + asyncReplicationRWMux.RLock.
	changelogRetryAttempts = 2
)

// ActivateChangeLog opens a fresh log for opID and registers it, replacing a
// log already registered under opID (a resumed op). It first sweeps any .log
// files whose op-id is not registered — the safety net for orphans left by
// prior failed movements on a long-lived shard.
//
// The keep-snapshot, sweep, O_EXCL Open, and Register run under
// changeLogsActivateMu so two concurrent activates can't each snapshot a
// stale registered set and sweep the other's freshly-opened .log file.
func (s *Shard) ActivateChangeLog(ctx context.Context, opID string) (*changelog.ChangeLog, error) {
	s.changeLogsActivateMu.Lock()
	defer s.changeLogsActivateMu.Unlock()

	dir := s.changelogDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("shard %q: create changelog dir: %w", s.ID(), err)
	}

	// An abandoned Start (slow shard load) must not clobber the retried attempt's live log or lost marker.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("shard %q: activate changelog for op %q: %w", s.ID(), opID, err)
	}

	// A resumed op reuses its id; its stale log would fail the O_EXCL Open forever.
	if stale := s.changeLogs.Load().Get(opID); stale != nil {
		changelog.Unregister(&s.changeLogs, opID)
		if err := stale.Deactivate(); err != nil {
			return nil, fmt.Errorf("shard %q: deactivate stale changelog for op %q: %w", s.ID(), opID, err)
		}
		s.index.logger.WithFields(logrus.Fields{
			"action":   "change_capture_log",
			"op_id":    opID,
			"shard":    s.ID(),
			"last_lsn": stale.LSN(),
		}).Info("replaced stale change-capture log")
	}

	keep := s.registeredOpIDs()
	keep[opID] = struct{}{} // don't sweep the file we're about to Open(O_EXCL)
	if err := s.sweepChangelogDirExcept(keep); err != nil {
		return nil, fmt.Errorf("shard %q: sweep orphans before activate: %w", s.ID(), err)
	}

	path, _ := changelogPaths(dir, opID)
	log, err := changelog.Open(path, s.index.logger)
	if err != nil {
		return nil, fmt.Errorf("shard %q: open changelog for op %q: %w", s.ID(), opID, err)
	}
	changelog.Register(&s.changeLogs, opID, log)
	// Only after Register: a failed Open must leave the op reading as lost, never as stopped.
	if err := s.clearChangeLogLost(opID); err != nil {
		changelog.Unregister(&s.changeLogs, opID)
		if derr := log.Deactivate(); derr != nil {
			err = errors.Join(err, derr)
		}
		return nil, fmt.Errorf("shard %q: activate changelog for op %q: %w", s.ID(), opID, err)
	}
	s.index.logger.WithFields(logrus.Fields{
		"action": "change_capture_log",
		"op_id":  opID,
		"shard":  s.ID(),
		"path":   path,
	}).Debug("change-capture log activated")
	return log, nil
}

// FinalizeChangeLog waits for the PREPAREs in flight at entry to commit or
// abort, then seals the log and returns the final LSN.
//
// Writes racing the seal need no write barrier: the consumer only calls
// Finalize after waiting for the op to reach INTEGRATING on every node (see
// processIntegratingOp in cluster/replication). Past that point every write
// is either routed to the target directly — so a dropped CCL append is
// harmless. So it does not matter whether a write's CCL append lands before
// or after the seal.
func (s *Shard) FinalizeChangeLog(ctx context.Context, opID string) (uint64, error) {
	log := s.changeLogs.Load().Get(opID)
	if log == nil {
		return 0, s.changeLogMissErr(opID)
	}
	start := time.Now()
	pending := s.replicationMap.keys()

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		draining := false
		for _, reqID := range pending {
			if _, stillPending := s.replicationMap.get(reqID); stillPending {
				draining = true
				break
			}
		}
		if !draining {
			finalLSN, err := log.Finalize()
			if err != nil {
				return 0, err
			}
			s.index.logger.WithFields(logrus.Fields{
				"action":     "change_capture_log",
				"op_id":      opID,
				"shard":      s.ID(),
				"final_lsn":  finalLSN,
				"drain_took": time.Since(start),
			}).Debug("change-capture log sealed")
			return finalLSN, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
		}
	}
}

// SnapshotChangeLogLSN returns the highest LSN currently in the log without
// sealing it — the log keeps accepting writes. Pairs with a capped tailer to
// drain a phase boundary mid-movement.
func (s *Shard) SnapshotChangeLogLSN(ctx context.Context, opID string) (uint64, error) {
	log := s.changeLogs.Load().Get(opID)
	if log == nil {
		return 0, s.changeLogMissErr(opID)
	}
	lsn := log.LSN()
	s.index.logger.WithFields(logrus.Fields{
		"action": "change_capture_log",
		"op_id":  opID,
		"shard":  s.ID(),
		"lsn":    lsn,
	}).Debug("change-capture log LSN snapshotted")
	return lsn, nil
}

// StopChangeCapture deactivates opID's log and removes its lost marker, so a
// later lookup reads as stopped. Idempotent.
func (s *Shard) StopChangeCapture(ctx context.Context, opID string) error {
	log := s.changeLogs.Load().Get(opID)
	if log == nil {
		return s.clearChangeLogLost(opID)
	}
	changelog.Unregister(&s.changeLogs, opID)
	if err := log.Deactivate(); err != nil {
		return err
	}
	if err := s.clearChangeLogLost(opID); err != nil {
		return err
	}
	lastLSN := log.LSN()
	s.index.logger.WithFields(logrus.Fields{
		"action":   "change_capture_log",
		"op_id":    opID,
		"shard":    s.ID(),
		"last_lsn": lastLSN,
	}).Debug("change-capture log deactivated")
	return nil
}

func (s *Shard) GetChangeLog(ctx context.Context, opID string) (*changelog.ChangeLog, bool) {
	set := s.changeLogs.Load()
	if set == nil {
		return nil, false
	}
	log := set.Get(opID)
	return log, log != nil
}

// AppendChangeLogPut tees every committed PUT into every active log. It MUST
// NOT fail the user write: exhausted-retry errors deactivate the log so the
// target's tailer observes ErrLogDeactivated and aborts the movement.
// objBinary is reused verbatim — the caller already marshalled it for the
// bucket write.
func (s *Shard) AppendChangeLogPut(idBytes []byte, updateTimeMillis int64, objBinary []byte) {
	set := s.changeLogs.Load()
	if set == nil {
		return
	}

	var uuidArr [16]byte
	copy(uuidArr[:], idBytes)

	set.ForEach(func(opID string, log *changelog.ChangeLog) {
		lsn, appendErr := s.appendWithRetry(func() (uint64, error) {
			return log.AppendPut(uuidArr, updateTimeMillis, objBinary)
		})
		s.logChangeLogAppend(opID, "put", uuidArr, updateTimeMillis, lsn, appendErr)
		s.dispatchAppendResult(opID, log, appendErr)
	})
}

func (s *Shard) AppendChangeLogDelete(idBytes []byte, updateTimeMillis int64) {
	set := s.changeLogs.Load()
	if set == nil {
		return
	}

	var uuidArr [16]byte
	copy(uuidArr[:], idBytes)

	set.ForEach(func(opID string, log *changelog.ChangeLog) {
		lsn, appendErr := s.appendWithRetry(func() (uint64, error) {
			return log.AppendDelete(uuidArr, updateTimeMillis)
		})
		s.logChangeLogAppend(opID, "delete", uuidArr, updateTimeMillis, lsn, appendErr)
		s.dispatchAppendResult(opID, log, appendErr)
	})
}

func (s *Shard) logChangeLogAppend(opID, kind string, uuidArr [16]byte, updateTimeMillis int64, lsn uint64, appendErr error) {
	if !s.index.debugLoggingEnabled() {
		return
	}
	if appendErr == nil {
		s.index.logger.WithFields(logrus.Fields{
			"action":         "change_capture_log",
			"op_id":          opID,
			"shard":          s.ID(),
			"kind":           kind,
			"uuid":           uuid.UUID(uuidArr),
			"update_time_ms": updateTimeMillis,
			"lsn":            lsn,
		}).Debug("change-capture log entry appended")
		return
	}
	if errors.Is(appendErr, changelog.ErrLogFinalized) || errors.Is(appendErr, changelog.ErrLogDeactivated) {
		s.index.logger.WithFields(logrus.Fields{
			"action":         "change_capture_log",
			"op_id":          opID,
			"shard":          s.ID(),
			"kind":           kind,
			"uuid":           uuid.UUID(uuidArr),
			"update_time_ms": updateTimeMillis,
		}).Debugf("change-capture log append dropped: %v", appendErr)
	}
}

// appendWithRetry short-circuits ErrLogFinalized/ErrLogDeactivated (retry
// can't help) and otherwise retries changelogRetryAttempts times.
func (s *Shard) appendWithRetry(attempt func() (uint64, error)) (uint64, error) {
	var (
		lsn uint64
		err error
	)
	exp := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(1*time.Millisecond),
		backoff.WithMultiplier(5),
	)
	retry := backoff.WithMaxRetries(exp, uint64(changelogRetryAttempts))
	if err := backoff.Retry(func() error {
		lsn, err = attempt()
		if err == nil {
			return nil
		}
		if errors.Is(err, changelog.ErrLogFinalized) || errors.Is(err, changelog.ErrLogDeactivated) {
			return backoff.Permanent(err)
		}
		return err
	}, retry); err != nil {
		return 0, fmt.Errorf("append with retry: %w", err)
	}
	return lsn, nil
}

func (s *Shard) dispatchAppendResult(opID string, log *changelog.ChangeLog, err error) {
	if err == nil {
		return
	}
	if errors.Is(err, changelog.ErrLogFinalized) || errors.Is(err, changelog.ErrLogDeactivated) {
		return
	}
	s.handleChangeLogFailure(opID, log, err)
}

// handleChangeLogFailure marks the log lost before unregistering it: the write
// it failed to capture is gone, so the movement must not read it as stopped.
func (s *Shard) handleChangeLogFailure(opID string, log *changelog.ChangeLog, cause error) {
	logger := s.index.logger.WithFields(logrus.Fields{"op_id": opID, "shard": s.ID()})
	logger.Errorf("change-capture log entered terminal failure, deactivating: %v", cause)
	// A resumed op may have replaced this log; its successor lives under the same path.
	if s.changeLogs.Load().Get(opID) != log {
		if err := log.Deactivate(); err != nil {
			logger.Errorf("change-capture log deactivate after failure: %v", err)
		}
		return
	}
	s.lostChangeLogs.Store(opID, struct{}{})
	if _, err := s.markChangeLogLost(opID, true); err != nil {
		logger.Errorf("change-capture log lost marker not persisted, a restart reads it as stopped: %v", err)
	}
	changelog.Unregister(&s.changeLogs, opID)
	if err := log.Deactivate(); err != nil {
		logger.Errorf("change-capture log deactivate after failure: %v", err)
	}
}

func (s *Shard) registeredOpIDs() map[string]struct{} {
	set := s.changeLogs.Load()
	if set == nil {
		return make(map[string]struct{})
	}
	out := make(map[string]struct{}, set.Len())
	set.ForEach(func(opID string, _ *changelog.ChangeLog) {
		out[opID] = struct{}{}
	})
	return out
}

// sweepChangelogDirExcept removes every .log file whose op-id is not in keep:
// ActivateChangeLog's safety net for orphans on a long-lived shard. Lost
// markers stay, their movements have yet to learn of the loss.
func (s *Shard) sweepChangelogDirExcept(keep map[string]struct{}) error {
	return s.forEachChangelogFile(func(opID, p string) error {
		if _, live := keep[opID]; live {
			return nil
		}
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove orphaned changelog %q: %w", p, err)
		}
		s.index.logger.WithField("file", p).Info("removed orphaned changelog")
		return nil
	})
}

// sweepChangelogDir runs at shard load. No log survives it, so each becomes a
// lost marker: a movement still draining it must not read it as stopped.
func (s *Shard) sweepChangelogDir() error {
	return s.forEachChangelogFile(func(opID, p string) error {
		marked, err := s.markChangeLogLost(opID, false)
		if err != nil {
			return err
		}
		if marked {
			s.index.logger.WithFields(logrus.Fields{
				"action": "change_capture_log",
				"op_id":  opID,
				"shard":  s.ID(),
				"file":   p,
			}).Warn("change-capture log discarded on shard load, marked lost")
		}
		return nil
	})
}

func (s *Shard) forEachChangelogFile(f func(opID, path string) error) error {
	dir := s.changelogDir()
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read changelog dir %q: %w", dir, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != changelogFileExtension {
			continue
		}
		if err := f(strings.TrimSuffix(name, changelogFileExtension), filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	return nil
}

// markChangeLogLost turns opID's log into its lost marker. The rename is atomic,
// so a crash leaves the log (swept again on load) or the marker, never neither.
// Without a log file, createIfMissing decides: a concurrent stop removed it.
func (s *Shard) markChangeLogLost(opID string, createIfMissing bool) (bool, error) {
	logPath, lostPath := changelogPaths(s.changelogDir(), opID)
	if err := os.Rename(logPath, lostPath); err != nil {
		if !os.IsNotExist(err) {
			return false, fmt.Errorf("mark changelog %q lost: %w", logPath, err)
		}
		if !createIfMissing {
			return false, nil
		}
		f, err := os.OpenFile(lostPath, os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return false, fmt.Errorf("create changelog lost marker %q: %w", lostPath, err)
		}
		if err := f.Close(); err != nil {
			return false, fmt.Errorf("close changelog lost marker %q: %w", lostPath, err)
		}
		return true, nil
	}
	if err := os.Truncate(lostPath, 0); err != nil {
		s.index.logger.WithField("file", lostPath).Warnf("changelog lost marker keeps its log's bytes: %v", err)
	}
	return true, nil
}

// clearChangeLogLost forgets opID's loss once the movement restarted or stopped its capture.
func (s *Shard) clearChangeLogLost(opID string) error {
	s.lostChangeLogs.Delete(opID)
	_, lostPath := changelogPaths(s.changelogDir(), opID)
	if err := os.Remove(lostPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove changelog lost marker %q: %w", lostPath, err)
	}
	return nil
}

// changeLogMissErr tells a log this shard discarded from one the movement stopped.
func (s *Shard) changeLogMissErr(opID string) error {
	if _, lost := s.lostChangeLogs.Load(opID); lost {
		return errChangeLogLost
	}
	_, lostPath := changelogPaths(s.changelogDir(), opID)
	lost, err := diskio.FileExists(lostPath)
	if err != nil {
		return fmt.Errorf("probe changelog lost marker %q: %w", lostPath, err)
	}
	if lost {
		return errChangeLogLost
	}
	return errNoSuchChangeLog
}

// markSourcedChangeLogsLost marks the logs of in-flight ops copying from a replica whose files came
// back without them, or those ops would read the missing logs as sealed and complete without their writes.
// A dir that was not recreated only counts while a SELF_RECOVERY op targets it: it is then the recovered copy.
func (i *Index) markSourcedChangeLogsLost(shardPath, shardName string, recreated bool) error {
	fsm := i.getReplicationFSMReader()
	sourced, ok := fsm.(replicationTypes.ReplicationFSMSourcedOpsReader)
	if !ok || i.getSchema == nil {
		return nil
	}
	collection, node := i.Config.ClassName.String(), i.getSchema.NodeName()
	opIDs := sourced.InFlightOpsSourcingShard(collection, shardName, node)
	if len(opIDs) == 0 {
		return nil
	}
	if !recreated && !fsm.HasActiveSelfRecoveryTargetingShard(collection, shardName, node) {
		return nil
	}
	dir := changelogDirOf(shardPath)
	var marked []string
	for _, id := range opIDs {
		opID := strconv.FormatUint(id, 10)
		logPath, lostPath := changelogPaths(dir, opID)
		present := false
		for _, p := range []string{logPath, lostPath} {
			found, err := diskio.FileExists(p)
			if err != nil {
				return fmt.Errorf("probe changelog %q: %w", p, err)
			}
			present = present || found
		}
		if present {
			continue
		}
		// Mkdir, not MkdirAll: a shard dir removed meanwhile must not be resurrected.
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("create changelog dir %q: %w", dir, err)
		}
		f, err := os.OpenFile(lostPath, os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return fmt.Errorf("create changelog lost marker %q: %w", lostPath, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close changelog lost marker %q: %w", lostPath, err)
		}
		marked = append(marked, opID)
	}
	if len(marked) == 0 {
		return nil
	}
	for _, d := range []string{dir, shardPath, filepath.Dir(shardPath)} {
		if err := diskio.Fsync(d); err != nil {
			return fmt.Errorf("fsync %q: %w", d, err)
		}
	}
	i.logger.WithFields(logrus.Fields{
		"action": "change_capture_log",
		"shard":  shardName,
		"op_ids": marked,
	}).Warn("replica files came back without the change-capture logs of in-flight ops copying from it, marked lost")
	return nil
}

// shardDirHoldsNoData: an empty shard dir, or one holding only lost markers, never held local data.
func shardDirHoldsNoData(entries []os.DirEntry) bool {
	return len(entries) == 0 || (len(entries) == 1 && entries[0].IsDir() && entries[0].Name() == changelogDirName)
}

func (s *Shard) changelogDir() string {
	return changelogDirOf(s.path())
}

func changelogDirOf(shardPath string) string {
	return filepath.Join(shardPath, changelogDirName)
}

func changelogPaths(dir, opID string) (logPath, lostPath string) {
	return filepath.Join(dir, opID+changelogFileExtension), filepath.Join(dir, opID+changelogLostExtension)
}
