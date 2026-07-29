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
	"os"
	"path/filepath"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/replication/changelog"
)

var errNoSuchChangeLog = errors.New("shard: " + changelog.ErrMsgNoActiveChangeCaptureLog + " for that op-id")

const (
	changelogDirName       = "changelog"
	changelogFileExtension = ".log"
	// Kept small because retries run under docIdLock + asyncReplicationRWMux.RLock.
	changelogRetryAttempts = 2
)

// ActivateChangeLog opens a fresh log for opID and registers it. It first
// sweeps any .log files whose op-id is not registered — the safety net for
// orphans left by prior failed movements on a long-lived shard.
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

	keep := s.registeredOpIDs()
	keep[opID] = struct{}{} // don't sweep the file we're about to Open(O_EXCL)
	if err := s.sweepChangelogDirExcept(keep); err != nil {
		return nil, fmt.Errorf("shard %q: sweep orphans before activate: %w", s.ID(), err)
	}

	path := filepath.Join(dir, opID+changelogFileExtension)
	log, err := changelog.Open(path, s.index.logger)
	if err != nil {
		return nil, fmt.Errorf("shard %q: open changelog for op %q: %w", s.ID(), opID, err)
	}
	changelog.Register(&s.changeLogs, opID, log)
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
		return 0, errNoSuchChangeLog
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
		return 0, errNoSuchChangeLog
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

func (s *Shard) StopChangeCapture(ctx context.Context, opID string) error {
	log := s.changeLogs.Load().Get(opID)
	if log == nil {
		return nil
	}
	changelog.Unregister(&s.changeLogs, opID)
	if err := log.Deactivate(); err != nil {
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

func (s *Shard) handleChangeLogFailure(opID string, log *changelog.ChangeLog, cause error) {
	s.index.logger.
		WithField("op_id", opID).
		WithField("shard", s.ID()).
		Error(fmt.Errorf("change-capture log entered terminal failure, deactivating: %w", cause))
	changelog.Unregister(&s.changeLogs, opID)
	if err := log.Deactivate(); err != nil {
		s.index.logger.
			WithField("op_id", opID).
			WithField("shard", s.ID()).
			Error(fmt.Errorf("change-capture log deactivate after failure: %w", err))
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

// sweepChangelogDirExcept removes every .log file whose op-id basename is
// not in keep. Called from NewShard (keep=nil, everything is orphaned on
// restart) and from ActivateChangeLog (keep = registered ops ∪ new opID).
func (s *Shard) sweepChangelogDirExcept(keep map[string]struct{}) error {
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
		opID := name[:len(name)-len(changelogFileExtension)]
		if _, live := keep[opID]; live {
			continue
		}
		p := filepath.Join(dir, name)
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove orphaned changelog %q: %w", p, err)
		}
		s.index.logger.WithField("file", p).Info("removed orphaned changelog")
	}
	return nil
}

func (s *Shard) sweepChangelogDir() error {
	return s.sweepChangelogDirExcept(nil)
}

func (s *Shard) changelogDir() string {
	return filepath.Join(s.path(), changelogDirName)
}
