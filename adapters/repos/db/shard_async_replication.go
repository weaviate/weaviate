//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/interval"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

const (
	defaultHashtreeHeight              = 16
	defaultFrequency                   = 30 * time.Second
	defaultFrequencyWhilePropagating   = 3 * time.Second
	defaultAliveNodesCheckingFrequency = 5 * time.Second
	defaultLoggingFrequency            = 60 * time.Second
	defaultInitShieldCPUEveryN         = 1_000
	defaultDiffBatchSize               = 1_000
	defaultDiffPerNodeTimeout          = 10 * time.Second
	defaultPrePropagationTimeout       = 300 * time.Second
	defaultPropagationTimeout          = 60 * time.Second
	defaultPropagationLimit            = 10_000
	defaultPropagationDelay            = 30 * time.Second
	defaultPropagationConcurrency      = 5
	defaultPropagationBatchSize        = 100

	minHashtreeHeight = 0
	maxHashtreeHeight = 20

	minInitShieldCPUEveryN = 0
	maxInitShieldCPUEveryN = math.MaxInt

	minDiffBatchSize = 1
	maxDiffBatchSize = 10_000

	minPropagationLimit = 1
	maxPropagationLimit = 1_000_000

	minPropgationConcurrency  = 1
	maxPropagationConcurrency = 20

	minPropagationBatchSize = 1
	maxPropagationBatchSize = 1_000
)

type asyncReplicationConfig struct {
	hashtreeHeight              int
	frequency                   time.Duration
	frequencyWhilePropagating   time.Duration
	aliveNodesCheckingFrequency time.Duration
	loggingFrequency            time.Duration
	initShieldCPUEveryN         int
	diffBatchSize               int
	diffPerNodeTimeout          time.Duration
	prePropagationTimeout       time.Duration
	propagationTimeout          time.Duration
	propagationLimit            int
	propagationDelay            time.Duration
	propagationConcurrency      int
	propagationBatchSize        int
	targetNodeOverrides         additional.AsyncReplicationTargetNodeOverrides
	maintenanceModeEnabled      func() bool
}

func (s *Shard) getAsyncReplicationConfig() (config asyncReplicationConfig, err error) {
	// preserve the target node overrides from the previous config
	config.targetNodeOverrides = s.asyncReplicationConfig.targetNodeOverrides

	config.hashtreeHeight, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_HASHTREE_HEIGHT"), defaultHashtreeHeight, minHashtreeHeight, maxHashtreeHeight)
	if err != nil {
		return asyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_HASHTREE_HEIGHT", err)
	}

	config.frequency, err = optParseDuration(os.Getenv("ASYNC_REPLICATION_FREQUENCY"), defaultFrequency)
	if err != nil {
		return asyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_FREQUENCY", err)
	}

	config.frequencyWhilePropagating, err = optParseDuration(os.Getenv("ASYNC_REPLICATION_FREQUENCY_WHILE_PROPAGATING"), defaultFrequencyWhilePropagating)
	if err != nil {
		return asyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_FREQUENCY_WHILE_PROPAGATING", err)
	}

	config.aliveNodesCheckingFrequency, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_ALIVE_NODES_CHECKING_FREQUENCY"), defaultAliveNodesCheckingFrequency)
	if err != nil {
		return asyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_ALIVE_NODES_CHECKING_FREQUENCY", err)
	}

	config.loggingFrequency, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_LOGGING_FREQUENCY"), defaultLoggingFrequency)
	if err != nil {
		return asyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_LOGGING_FREQUENCY", err)
	}

	config.initShieldCPUEveryN, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_INIT_SHIELD_CPU_EVERY_N"), defaultInitShieldCPUEveryN, minInitShieldCPUEveryN, maxInitShieldCPUEveryN)
	if err != nil {
		return asyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_INIT_SHIELD_CPU_EVERY_N", err)
	}

	config.diffBatchSize, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_DIFF_BATCH_SIZE"), defaultDiffBatchSize, minDiffBatchSize, maxDiffBatchSize)
	if err != nil {
		return asyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_DIFF_BATCH_SIZE", err)
	}

	config.diffPerNodeTimeout, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_DIFF_PER_NODE_TIMEOUT"), defaultDiffPerNodeTimeout)
	if err != nil {
		return asyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_DIFF_PER_NODE_TIMEOUT", err)
	}

	config.prePropagationTimeout, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_PRE_PROPAGATION_TIMEOUT"), defaultPrePropagationTimeout)
	if err != nil {
		return asyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_PRE_PROPAGATION_TIMEOUT", err)
	}

	config.propagationTimeout, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_PROPAGATION_TIMEOUT"), defaultPropagationTimeout)
	if err != nil {
		return asyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_PROPAGATION_TIMEOUT", err)
	}

	config.propagationLimit, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_PROPAGATION_LIMIT"), defaultPropagationLimit, minPropagationLimit, maxPropagationLimit)
	if err != nil {
		return asyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_PROPAGATION_LIMIT", err)
	}

	config.propagationDelay, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_PROPAGATION_DELAY"), defaultPropagationDelay)
	if err != nil {
		return asyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_PROPAGATION_DELAY", err)
	}

	config.propagationConcurrency, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_PROPAGATION_CONCURRENCY"), defaultPropagationConcurrency, minPropgationConcurrency, maxPropagationConcurrency)
	if err != nil {
		return asyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_PROPAGATION_CONCURRENCY", err)
	}

	config.propagationBatchSize, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_PROPAGATION_BATCH_SIZE"), defaultPropagationBatchSize, minPropagationBatchSize, maxPropagationBatchSize)
	if err != nil {
		return asyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_PROPAGATION_BATCH_SIZE", err)
	}

	config.maintenanceModeEnabled = s.index.Config.MaintenanceModeEnabled

	return config, err
}

func optParseInt(s string, defaultVal, minVal, maxVal int) (val int, err error) {
	if s == "" {
		val = defaultVal
	} else {
		val, err = strconv.Atoi(s)
		if err != nil {
			return 0, err
		}
	}

	if val < minVal || val > maxVal {
		return 0, fmt.Errorf("value %d out of range: min %d, max %d", val, minVal, maxVal)
	}

	return val, nil
}

func optParseDuration(s string, defaultDuration time.Duration) (time.Duration, error) {
	if s == "" {
		return defaultDuration, nil
	}
	return time.ParseDuration(s)
}

func (s *Shard) initAsyncReplication() (err error) {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)

	s.asyncReplicationRunning = atomic.Bool{}
	// channel is used to "wake up" the hashbeater when a change occurs that
	// requires propagation, e.g. a new target node override is added
	// it's buffered to ensure that multiple changes occurring in a short time
	// frame only cause one wake-up
	s.asyncReplicationShouldRun = make(chan struct{})

	ctx, cancelFunc := context.WithCancel(context.Background())
	s.asyncReplicationCancelFunc = cancelFunc

	config, err := s.getAsyncReplicationConfig()
	if err != nil {
		return err
	}
	s.asyncReplicationConfig = config

	start := time.Now()

	if err := os.MkdirAll(s.pathHashTree(), os.ModePerm); err != nil {
		return err
	}

	// load the most recent hashtree file
	dirEntries, err := os.ReadDir(s.pathHashTree())
	if err != nil {
		return err
	}

	for i := len(dirEntries) - 1; i >= 0; i-- {
		dirEntry := dirEntries[i]

		if dirEntry.IsDir() || filepath.Ext(dirEntry.Name()) != ".ht" {
			continue
		}

		hashtreeFilename := filepath.Join(s.pathHashTree(), dirEntry.Name())

		if s.hashtree != nil {
			err := os.Remove(hashtreeFilename)
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("deleting older hashtree file %q: %v", hashtreeFilename, err)
			continue
		}

		f, err := os.OpenFile(hashtreeFilename, os.O_RDONLY, os.ModePerm)
		if err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("reading hashtree file %q: %v", hashtreeFilename, err)
			continue
		}

		// attempt to load hashtree from file
		s.hashtree, err = hashtree.DeserializeHashTree(bufio.NewReader(f))
		if err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("reading hashtree file %q: %v", hashtreeFilename, err)
		}

		err = f.Close()
		if err != nil {
			return err
		}

		err = os.Remove(hashtreeFilename)
		if err != nil {
			return err
		}

		if err := diskio.Fsync(s.pathHashTree()); err != nil {
			return fmt.Errorf("fsync hashtree directory %q: %w", s.pathHashTree(), err)
		}

		if s.hashtree != nil && s.hashtree.Height() != config.hashtreeHeight {
			// existing hashtree is erased if a different height was specified
			s.hashtree = nil
		}
	}

	if s.hashtree != nil {
		s.hashtreeFullyInitialized = true
		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			WithField("took", fmt.Sprintf("%v", time.Since(start))).
			Info("hashtree successfully initialized")

		s.initHashBeater(ctx, config)
		return nil
	}

	s.hashtree, err = hashtree.NewHashTree(config.hashtreeHeight)
	if err != nil {
		return err
	}

	s.hashtreeFullyInitialized = false
	s.minimalHashtreeInitializationCh = make(chan struct{})

	enterrors.GoWrapper(func() {
		for i := 0; ; i++ {
			err := s.initHashtree(ctx, config, bucket)
			if err == nil {
				break
			}

			if ctx.Err() != nil {
				s.index.logger.
					WithField("action", "async_replication").
					WithField("class_name", s.class.Class).
					WithField("shard_name", s.name).
					Info("hashtree initialization stopped")
				return
			}

			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Errorf("hashtree initialization attempt %d failure: %v", i, err)

			// exponential backoff: min(2^i * 100ms, 5s)
			backoff := min(time.Duration(1<<i)*100*time.Millisecond, 5*time.Second)
			time.Sleep(backoff)

			s.asyncReplicationRWMux.Lock()
			s.hashtree.Reset()
			s.minimalHashtreeInitializationCh = make(chan struct{})
			s.asyncReplicationRWMux.Unlock()
		}
	}, s.index.logger)

	return nil
}

func (s *Shard) initHashtree(ctx context.Context, config asyncReplicationConfig, bucket *lsmkv.Bucket) (err error) {
	start := time.Now()

	s.metrics.IncAsyncReplicationHashTreeInitCount()
	s.metrics.IncAsyncReplicationHashTreeInitRunning()

	defer func() {
		s.metrics.DecAsyncReplicationHashTreeInitRunning()

		if err != nil {
			s.metrics.IncAsyncReplicationHashTreeInitFailure()
			return
		}

		s.metrics.ObserveAsyncReplicationHashTreeInitDuration(time.Since(start))
	}()

	releaseInitialization := func() {
		s.asyncReplicationRWMux.RLock()
		defer s.asyncReplicationRWMux.RUnlock()

		close(s.minimalHashtreeInitializationCh)
	}

	objCount := 0
	prevProgressLogging := time.Now()

	err = bucket.ApplyToObjectDigests(ctx, releaseInitialization, func(object *storobj.Object) error {
		if time.Since(prevProgressLogging) >= config.loggingFrequency {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				WithField("object_count", objCount).
				WithField("took", fmt.Sprintf("%v", time.Since(start))).
				Infof("hashtree initialization in progress...")
			prevProgressLogging = time.Now()
		}

		uuidBytes, err := parseBytesUUID(object.ID())
		if err != nil {
			return err
		}

		s.asyncReplicationRWMux.RLock()
		defer s.asyncReplicationRWMux.RUnlock()

		err = s.mayUpsertObjectHashTree(object, uuidBytes, objectInsertStatus{})
		if err != nil {
			return err
		}

		objCount++

		if config.initShieldCPUEveryN > 0 {
			if objCount%config.initShieldCPUEveryN == 0 {
				// yield the processor so other goroutines can run
				runtime.Gosched()
				time.Sleep(time.Millisecond)
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("iterating objects: %w", err)
	}

	s.asyncReplicationRWMux.Lock()
	defer s.asyncReplicationRWMux.Unlock()

	if s.hashtree == nil {
		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			Info("hashtree initialization stopped")
		return nil
	}

	s.hashtreeFullyInitialized = true

	s.index.logger.
		WithField("action", "async_replication").
		WithField("class_name", s.class.Class).
		WithField("shard_name", s.name).
		WithField("object_count", objCount).
		WithField("took", fmt.Sprintf("%v", time.Since(start))).
		Info("hashtree successfully initialized")

	s.initHashBeater(ctx, config)

	return nil
}

func (s *Shard) waitForMinimalHashTreeInitialization(ctx context.Context) error {
	if s.hashtree == nil || s.hashtreeFullyInitialized {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.minimalHashtreeInitializationCh:
		return nil
	}
}

func (s *Shard) mayStopAsyncReplication() {
	s.asyncReplicationRWMux.Lock()
	defer s.asyncReplicationRWMux.Unlock()

	if s.hashtree == nil {
		return
	}

	s.asyncReplicationCancelFunc()

	if s.hashtreeFullyInitialized {
		// the hashtree needs to be fully in sync with stored data before it can be persisted
		err := s.dumpHashTree()
		if err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Errorf("store hashtree failed: %v", err)
		}
	}

	s.hashtree = nil
	s.hashtreeFullyInitialized = false
}

func (s *Shard) SetAsyncReplicationEnabled(_ context.Context, enabled bool) error {
	s.asyncReplicationRWMux.Lock()
	defer s.asyncReplicationRWMux.Unlock()

	if enabled {
		if s.hashtree != nil {
			return nil
		}

		return s.initAsyncReplication()
	}

	if s.hashtree == nil {
		return nil
	}

	s.asyncReplicationCancelFunc()

	s.hashtree = nil
	s.asyncReplicationStatsByTargetNode = nil
	s.hashtreeFullyInitialized = false

	return nil
}

func (s *Shard) addTargetNodeOverride(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error {
	func() {
		s.asyncReplicationRWMux.Lock()
		// unlock before calling SetAsyncReplicationEnabled because it will lock again
		defer s.asyncReplicationRWMux.Unlock()

		for i, existing := range s.asyncReplicationConfig.targetNodeOverrides {
			if existing.Equal(&targetNodeOverride) {
				// if the collection/shard/source/target already exists, use the max
				// upper time bound between the existing/new override
				maxUpperTimeBound := existing.UpperTimeBound
				if targetNodeOverride.UpperTimeBound > maxUpperTimeBound {
					maxUpperTimeBound = targetNodeOverride.UpperTimeBound
					s.asyncReplicationConfig.targetNodeOverrides[i].UpperTimeBound = maxUpperTimeBound
				}
				return
			}
		}

		if s.asyncReplicationConfig.targetNodeOverrides == nil {
			s.asyncReplicationConfig.targetNodeOverrides = make(additional.AsyncReplicationTargetNodeOverrides, 0, 1)
		}
		s.asyncReplicationConfig.targetNodeOverrides = append(s.asyncReplicationConfig.targetNodeOverrides, targetNodeOverride)
	}()
	// we call update async replication config here to ensure that async replication starts
	// if it's not already running
	return s.SetAsyncReplicationEnabled(ctx, true)
}

func (s *Shard) removeTargetNodeOverride(ctx context.Context, targetNodeOverrideToRemove additional.AsyncReplicationTargetNodeOverride) error {
	targetNodeOverrideLen := 0
	func() {
		s.asyncReplicationRWMux.Lock()
		// unlock before calling SetAsyncReplicationEnabled because it will lock again
		defer s.asyncReplicationRWMux.Unlock()

		newTargetNodeOverrides := make(additional.AsyncReplicationTargetNodeOverrides, 0, len(s.asyncReplicationConfig.targetNodeOverrides))
		for _, existing := range s.asyncReplicationConfig.targetNodeOverrides {
			// only remove the existing override if the collection/shard/source/target match and the
			// existing upper time bound is <= to the override being removed (eg if the override to remove
			// is "before" the existing override, don't remove it)
			if existing.Equal(&targetNodeOverrideToRemove) && existing.UpperTimeBound <= targetNodeOverrideToRemove.UpperTimeBound {
				delete(s.asyncReplicationStatsByTargetNode, existing.TargetNode)
				continue
			}
			newTargetNodeOverrides = append(newTargetNodeOverrides, existing)
		}
		s.asyncReplicationConfig.targetNodeOverrides = newTargetNodeOverrides

		targetNodeOverrideLen = len(s.asyncReplicationConfig.targetNodeOverrides)
	}()
	// if there are no overrides left, return the async replication config to what it
	// was before overrides were added
	if targetNodeOverrideLen == 0 {
		return s.SetAsyncReplicationEnabled(ctx, s.index.Config.AsyncReplicationEnabled)
	}
	return nil
}

func (s *Shard) removeAllTargetNodeOverrides(ctx context.Context) error {
	func() {
		s.asyncReplicationRWMux.Lock()
		// unlock before calling SetAsyncReplicationEnabled because it will lock again
		defer s.asyncReplicationRWMux.Unlock()
		s.asyncReplicationConfig.targetNodeOverrides = make(additional.AsyncReplicationTargetNodeOverrides, 0)
	}()
	return s.SetAsyncReplicationEnabled(ctx, s.index.Config.AsyncReplicationEnabled)
}

func (s *Shard) getAsyncReplicationStats(ctx context.Context) []*models.AsyncReplicationStatus {
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()

	asyncReplicationStatsToReturn := make([]*models.AsyncReplicationStatus, 0, len(s.asyncReplicationStatsByTargetNode))
	for targetNodeName, asyncReplicationStats := range s.asyncReplicationStatsByTargetNode {
		asyncReplicationStatsToReturn = append(asyncReplicationStatsToReturn, &models.AsyncReplicationStatus{
			ObjectsPropagated:       uint64(asyncReplicationStats.localObjectsPropagationCount) - uint64(asyncReplicationStats.objectsNotResolved),
			StartDiffTimeUnixMillis: asyncReplicationStats.hashtreeDiffStartTime.UnixMilli(),
			TargetNode:              targetNodeName,
		})
	}

	return asyncReplicationStatsToReturn
}

func (s *Shard) dumpHashTree() error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(time.Now().UnixNano()))

	hashtreeFilename := filepath.Join(s.pathHashTree(), fmt.Sprintf("hashtree-%x.ht", string(b[:])))

	f, err := os.OpenFile(hashtreeFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		return fmt.Errorf("storing hashtree %q: %w", hashtreeFilename, err)
	}

	w := bufio.NewWriter(f)

	_, err = s.hashtree.Serialize(w)
	if err != nil {
		return fmt.Errorf("storing hashtree %q: %w", hashtreeFilename, err)
	}

	err = w.Flush()
	if err != nil {
		return fmt.Errorf("storing hashtree %q: %w", hashtreeFilename, err)
	}

	err = f.Sync()
	if err != nil {
		return fmt.Errorf("storing hashtree %q: %w", hashtreeFilename, err)
	}

	err = f.Close()
	if err != nil {
		return fmt.Errorf("closing hashtree %q: %w", hashtreeFilename, err)
	}

	if err := diskio.Fsync(s.pathHashTree()); err != nil {
		return fmt.Errorf("fsync hashtree directory %q: %w", s.pathHashTree(), err)
	}

	return nil
}

func (s *Shard) HashTreeLevel(ctx context.Context, level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error) {
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()

	if !s.hashtreeFullyInitialized {
		return nil, fmt.Errorf("hashtree not initialized on shard %q", s.ID())
	}

	// TODO (jeroiraz): reusable pool of digests slices
	digests = make([]hashtree.Digest, hashtree.LeavesCount(level+1))

	n, err := s.hashtree.Level(level, discriminant, digests)
	if err != nil {
		return nil, err
	}

	return digests[:n], nil
}

func (s *Shard) initHashBeater(ctx context.Context, config asyncReplicationConfig) {
	var lastHashbeatPropagatedObjects bool
	var lastHashbeatMux sync.Mutex

	enterrors.GoWrapper(func() {
		s.metrics.IncAsyncReplicationHashbeaterRunning()
		defer s.metrics.DecAsyncReplicationHashbeaterRunning()

		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			Info("hashbeater started...")

		defer func() {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Info("hashbeater stopped")
		}()

		var lastLog time.Time

		backoffTimer := interval.NewBackoffTimer(1*time.Second, 3*time.Second, 5*time.Second)

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.asyncReplicationShouldRun:
				// Reload target node overrides
				func() {
					s.asyncReplicationRWMux.Lock()
					defer s.asyncReplicationRWMux.Unlock()
					config.targetNodeOverrides = s.asyncReplicationConfig.targetNodeOverrides
				}()

				func() {
					s.asyncReplicationRunning.Store(true)
					defer s.asyncReplicationRunning.Store(false)

					if (!s.index.asyncReplicationEnabled() && len(config.targetNodeOverrides) == 0) ||
						(config.maintenanceModeEnabled != nil && config.maintenanceModeEnabled()) {
						// skip hashbeat iteration when async replication is disabled and no target node overrides are set
						// or maintenance mode is enabled for localhost
						if config.maintenanceModeEnabled != nil && config.maintenanceModeEnabled() {
							s.index.logger.
								WithField("action", "async_replication").
								WithField("class_name", s.class.Class).
								WithField("shard_name", s.name).
								Info("skipping async replication in maintenance mode")
						}
						backoffTimer.Reset()
						lastHashbeatMux.Lock()
						now := time.Now()
						s.asyncReplicationLastRun.Store(&now)
						lastHashbeatPropagatedObjects = false
						lastHashbeatMux.Unlock()
						return
					}

					stats, err := s.hashBeat(ctx, config)
					// update the shard stats for the target node
					// anonymous func only here so we can use defer unlock
					func() {
						s.asyncReplicationRWMux.Lock()
						defer s.asyncReplicationRWMux.Unlock()

						if s.asyncReplicationStatsByTargetNode == nil {
							s.asyncReplicationStatsByTargetNode = make(map[string]*hashBeatHostStats)
						}
						if (err == nil || errors.Is(err, replica.ErrNoDiffFound)) && stats != nil {
							for _, stat := range stats {
								if stat != nil {
									s.index.logger.WithFields(logrus.Fields{
										"shard_name":                      s.name,
										"target_node_name":                stat.targetNodeName,
										"hashtree_diff_took":              stat.hashtreeDiffTook,
										"object_digests_diff_took":        stat.objectDigestsDiffTook,
										"local_object_digests_count":      stat.localObjectDigestsCount,
										"remote_object_digests_count":     stat.remoteObjectDigestsCount,
										"local_objects_propagation_count": stat.localObjectsPropagationCount,
										"local_objects_propagation_took":  stat.localObjectsPropagationTook,
									}).Debug("updating async replication stats")
									s.asyncReplicationStatsByTargetNode[stat.targetNodeName] = stat
								}
							}
						}
					}()
					if err != nil {
						if ctx.Err() != nil {
							return
						}

						if errors.Is(err, replica.ErrNoDiffFound) {
							if time.Since(lastLog) >= config.loggingFrequency {
								lastLog = time.Now()

								s.index.logger.
									WithField("action", "async_replication").
									WithField("class_name", s.class.Class).
									WithField("shard_name", s.name).
									WithField("hosts", s.getLastComparedHosts()).
									Debug("hashbeat iteration successfully completed: no differences were found")
							}

							backoffTimer.Reset()
							lastHashbeatMux.Lock()
							now := time.Now()
							s.asyncReplicationLastRun.Store(&now)
							lastHashbeatPropagatedObjects = false
							lastHashbeatMux.Unlock()
							return
						}

						if time.Since(lastLog) >= config.loggingFrequency {
							lastLog = time.Now()

							s.index.logger.
								WithField("action", "async_replication").
								WithField("class_name", s.class.Class).
								WithField("shard_name", s.name).
								Warnf("hashbeat iteration failed: %v", err)
						}

						time.Sleep(backoffTimer.CurrentInterval())
						backoffTimer.IncreaseInterval()
						lastHashbeatMux.Lock()
						now := time.Now()
						s.asyncReplicationLastRun.Store(&now)
						lastHashbeatPropagatedObjects = false
						lastHashbeatMux.Unlock()
						return
					}

					statsHaveObjectsPropagated := false
					if time.Since(lastLog) >= config.loggingFrequency {
						lastLog = time.Now()

						for _, stat := range stats {
							s.index.logger.
								WithField("action", "async_replication").
								WithField("class_name", s.class.Class).
								WithField("shard_name", s.name).
								WithField("target_node_name", stat.targetNodeName).
								WithField("hashtree_diff_took", stat.hashtreeDiffTook).
								WithField("object_digests_diff_took", stat.objectDigestsDiffTook).
								WithField("local_object_digests_count", stat.localObjectDigestsCount).
								WithField("remote_object_digests_count", stat.remoteObjectDigestsCount).
								WithField("local_objects_propagation_count", stat.localObjectsPropagationCount).
								WithField("local_objects_propagation_took", stat.localObjectsPropagationTook).
								Debug("hashbeat iteration successfully completed")
							if stat.localObjectDigestsCount > 0 {
								statsHaveObjectsPropagated = true
							}
						}
					}

					backoffTimer.Reset()
					lastHashbeatMux.Lock()
					now := time.Now()
					s.asyncReplicationLastRun.Store(&now)
					lastHashbeatPropagatedObjects = statsHaveObjectsPropagated
					lastHashbeatMux.Unlock()
				}()
			}
		}
	}, s.index.logger)

	// goroutine to monitor changes in alive nodes and time since last hashbeat
	// and "wake up" the hashbeater when necessary
	// e.g. when a node goes down or comes back up, or when frequency time has elapsed
	// since last hashbeat
	// this ensures that changes in cluster topology are quickly detected and propagated
	// without having to wait for the next frequency tick
	// note that the hashbeater itself also has a frequency ticker to ensure that
	// propagation occurs at least every frequency interval even if no changes in
	// alive nodes occur
	s.asyncReplicationShouldRun <- struct{}{}
	enterrors.GoWrapper(func() {
		s.metrics.IncAsyncReplicationHashbeatTriggerRunning()
		defer s.metrics.DecAsyncReplicationHashbeatTriggerRunning()

		nt := time.NewTicker(config.aliveNodesCheckingFrequency)
		defer nt.Stop()

		ft := time.NewTicker(min(config.frequencyWhilePropagating, config.frequency))
		defer ft.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-nt.C:
				comparedHosts := s.getLastComparedHosts()
				aliveHosts := s.allAliveHostnames()

				slices.Sort(comparedHosts)
				slices.Sort(aliveHosts)

				if !slices.Equal(comparedHosts, aliveHosts) {
					select {
					case <-ctx.Done():
						return
					case s.asyncReplicationShouldRun <- struct{}{}:
					}

					s.setLastComparedNodes(aliveHosts)
				}
			case <-ft.C:
				var shouldHashbeat bool
				lastHashbeatMux.Lock()
				lastHashbeatTime := s.asyncReplicationLastRun.Load()
				var lastHashbeat time.Duration
				if lastHashbeatTime != nil {
					lastHashbeat = time.Since(*lastHashbeatTime)
					shouldHashbeat = (lastHashbeatPropagatedObjects && lastHashbeat >= config.frequencyWhilePropagating) ||
						lastHashbeat >= config.frequency
				} else {
					shouldHashbeat = true
				}

				lastHashbeatMux.Unlock()

				if shouldHashbeat {
					select {
					case <-ctx.Done():
						return
					case s.asyncReplicationShouldRun <- struct{}{}:
					}
				}
			}
		}
	}, s.index.logger)
}

func (s *Shard) setLastComparedNodes(hosts []string) {
	s.lastComparedHostsMux.Lock()
	defer s.lastComparedHostsMux.Unlock()

	s.lastComparedHosts = hosts
}

func (s *Shard) getLastComparedHosts() []string {
	s.lastComparedHostsMux.RLock()
	defer s.lastComparedHostsMux.RUnlock()

	return s.lastComparedHosts
}

func (s *Shard) allAliveHostnames() []string {
	return s.index.router.AllHostnames()
}

type hashBeatHostStats struct {
	targetNodeName               string
	hashtreeDiffStartTime        time.Time
	hashtreeDiffTook             time.Duration
	objectDigestsDiffTook        time.Duration
	localObjectDigestsCount      int
	remoteObjectDigestsCount     int
	localObjectsPropagationCount int
	localObjectsPropagationTook  time.Duration
	objectsNotResolved           int
}

func (s *Shard) hashBeat(ctx context.Context, config asyncReplicationConfig) (stats []*hashBeatHostStats, err error) {
	start := time.Now()

	s.metrics.IncAsyncReplicationIterationCount()

	defer func() {
		if err != nil && !errors.Is(err, replica.ErrNoDiffFound) {
			s.metrics.IncAsyncReplicationIterationFailureCount()
			return
		}

		s.metrics.ObserveAsyncReplicationIterationDuration(time.Since(start))
	}()

	var ht hashtree.AggregatedHashTree

	s.asyncReplicationRWMux.RLock()
	if s.hashtree == nil {
		s.asyncReplicationRWMux.RUnlock()
		// handling the case of a hashtree being explicitly set to nil
		return nil, fmt.Errorf("hashtree not initialized on shard %q", s.ID())
	}
	ht = s.hashtree
	s.asyncReplicationRWMux.RUnlock()

	hashtreeDiffStart := time.Now()

	shardDiffReader, err := s.index.replicator.CollectShardDifferences(ctx, s.name, ht, config.diffPerNodeTimeout, config.targetNodeOverrides)
	if err != nil {
		if errors.Is(err, replica.ErrNoDiffFound) && len(config.targetNodeOverrides) > 0 {
			stats := make([]*hashBeatHostStats, 0, len(config.targetNodeOverrides))
			for _, o := range config.targetNodeOverrides {
				stats = append(stats, &hashBeatHostStats{
					targetNodeName:        o.TargetNode,
					hashtreeDiffStartTime: hashtreeDiffStart,
				})
			}
			return stats, err
		}
		return nil, fmt.Errorf("collecting hashtree differences: %w", err)
	}

	hashtreeDiffTook := time.Since(hashtreeDiffStart)
	s.metrics.ObserveAsyncReplicationHashtreeDiffDuration(hashtreeDiffTook)

	rangeReader := shardDiffReader.RangeReader

	objectDigestsDiffStart := time.Now()

	localObjectDigestsCount := 0
	remoteObjectDigestsCount := 0

	localObjectsToPropagate := make([]strfmt.UUID, 0, config.propagationLimit)
	localUpdateTimeByUUID := make(map[strfmt.UUID]int64, config.propagationLimit)
	remoteStaleUpdateTimeByUUID := make(map[strfmt.UUID]int64, config.propagationLimit)

	objectDigestsDiffCtx, cancel := context.WithTimeout(ctx, config.prePropagationTimeout)
	defer cancel()

	for len(localObjectsToPropagate) < config.propagationLimit {
		initialLeaf, finalLeaf, err := rangeReader.Next()
		if err != nil {
			if errors.Is(err, hashtree.ErrNoMoreRanges) {
				break
			}
			return nil, fmt.Errorf("reading collected differences: %w", err)
		}

		localObjsCountWithinRange, remoteObjsCountWithinRange, objsToPropagateWithinRange, err := s.objectsToPropagateWithinRange(
			objectDigestsDiffCtx,
			config,
			shardDiffReader.TargetNodeAddress,
			shardDiffReader.TargetNodeName,
			initialLeaf,
			finalLeaf,
			config.propagationLimit-len(localObjectsToPropagate),
		)
		if err != nil {
			if objectDigestsDiffCtx.Err() != nil {
				// it may be the case that just pre propagation timeout was reached
				// and some objects could be propagated
				break
			}

			return nil, fmt.Errorf("collecting local objects to be propagated: %w", err)
		}

		localObjectDigestsCount += localObjsCountWithinRange
		remoteObjectDigestsCount += remoteObjsCountWithinRange

		for _, obj := range objsToPropagateWithinRange {
			localObjectsToPropagate = append(localObjectsToPropagate, obj.uuid)
			localUpdateTimeByUUID[obj.uuid] = obj.lastUpdateTime
			remoteStaleUpdateTimeByUUID[obj.uuid] = obj.remoteStaleUpdateTime
		}
	}

	objectDigestsDiffTook := time.Since(objectDigestsDiffStart)
	s.metrics.ObserveAsyncReplicationObjectDigestsDiffDuration(objectDigestsDiffTook)

	objectsPropagationStart := time.Now()

	objectsNotResolved := 0
	if len(localObjectsToPropagate) > 0 {
		propagationCtx, cancel := context.WithTimeout(ctx, config.propagationTimeout)
		defer cancel()

		resp, err := s.propagateObjects(propagationCtx, config, shardDiffReader.TargetNodeAddress, localObjectsToPropagate, remoteStaleUpdateTimeByUUID)
		if err != nil {
			return nil, fmt.Errorf("propagating local objects: %w", err)
		}

		for _, r := range resp {
			// NOTE: deleted objects are not propagated but locally deleted when conflict is detected

			deletionStrategy := s.index.DeletionStrategy()

			if !r.Deleted ||
				deletionStrategy == models.ReplicationConfigDeletionStrategyNoAutomatedResolution ||
				config.targetNodeOverrides.NoDeletionResolution(shardDiffReader.TargetNodeName) {
				objectsNotResolved++
				continue
			}

			if deletionStrategy == models.ReplicationConfigDeletionStrategyDeleteOnConflict ||
				(deletionStrategy == models.ReplicationConfigDeletionStrategyTimeBasedResolution &&
					r.UpdateTime > localUpdateTimeByUUID[strfmt.UUID(r.ID)]) {

				err := s.DeleteObject(propagationCtx, strfmt.UUID(r.ID), time.UnixMilli(r.UpdateTime))
				if err != nil {
					return nil, fmt.Errorf("deleting local objects: %w", err)
				}
			}
		}
	}

	return []*hashBeatHostStats{
		{
			targetNodeName:               shardDiffReader.TargetNodeName,
			hashtreeDiffStartTime:        hashtreeDiffStart,
			hashtreeDiffTook:             hashtreeDiffTook,
			objectDigestsDiffTook:        objectDigestsDiffTook,
			localObjectDigestsCount:      localObjectDigestsCount,
			remoteObjectDigestsCount:     remoteObjectDigestsCount,
			localObjectsPropagationCount: len(localObjectsToPropagate),
			localObjectsPropagationTook:  time.Since(objectsPropagationStart),
			objectsNotResolved:           objectsNotResolved,
		},
	}, nil
}

func uuidFromBytes(uuidBytes []byte) (id strfmt.UUID, err error) {
	uuidParsed, err := uuid.FromBytes(uuidBytes)
	if err != nil {
		return id, err
	}
	return strfmt.UUID(uuidParsed.String()), nil
}

func bytesFromUUID(id strfmt.UUID) (uuidBytes []byte, err error) {
	uuidParsed, err := uuid.Parse(id.String())
	if err != nil {
		return nil, err
	}
	return uuidParsed.MarshalBinary()
}

func incToNextLexValue(b []byte) bool {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return false
		}
		b[i] = 0x00
	}
	return true
}

type objectToPropagate struct {
	uuid                  strfmt.UUID
	lastUpdateTime        int64
	remoteStaleUpdateTime int64
}

func (s *Shard) objectsToPropagateWithinRange(ctx context.Context, config asyncReplicationConfig,
	targetNodeAddress, targetNodeName string, initialLeaf, finalLeaf uint64, limit int,
) (localObjectsCount int, remoteObjectsCount int, objectsToPropagate []objectToPropagate, err error) {
	objectsToPropagate = make([]objectToPropagate, 0, limit)

	hashtreeHeight := config.hashtreeHeight

	finalUUIDBytes := make([]byte, 16)
	binary.BigEndian.PutUint64(finalUUIDBytes, finalLeaf<<(64-hashtreeHeight)|((1<<(64-hashtreeHeight))-1))
	copy(finalUUIDBytes[8:], []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	finalUUID, err := uuidFromBytes(finalUUIDBytes)
	if err != nil {
		return localObjectsCount, remoteObjectsCount, objectsToPropagate, err
	}

	currLocalUUIDBytes := make([]byte, 16)
	binary.BigEndian.PutUint64(currLocalUUIDBytes, initialLeaf<<(64-hashtreeHeight))

	for limit > 0 && bytes.Compare(currLocalUUIDBytes, finalUUIDBytes) < 1 {
		if ctx.Err() != nil {
			return localObjectsCount, remoteObjectsCount, objectsToPropagate, ctx.Err()
		}

		currLocalUUID, err := uuidFromBytes(currLocalUUIDBytes)
		if err != nil {
			return localObjectsCount, remoteObjectsCount, objectsToPropagate, err
		}

		currBatchSize := min(limit, config.diffBatchSize)

		allLocalDigests, err := s.index.DigestObjectsInRange(ctx, s.name, currLocalUUID, finalUUID, currBatchSize)
		if err != nil {
			return localObjectsCount, remoteObjectsCount, objectsToPropagate, fmt.Errorf("fetching local object digests: %w", err)
		}

		if len(allLocalDigests) == 0 {
			// no more local objects need to be propagated in this iteration
			break
		}

		localObjectsCount += len(allLocalDigests)

		// iteration should stop when all local digests within the range has been read

		lastLocalUUID := strfmt.UUID(allLocalDigests[len(allLocalDigests)-1].ID)

		lastLocalUUIDBytes, err := bytesFromUUID(lastLocalUUID)
		if err != nil {
			return localObjectsCount, remoteObjectsCount, objectsToPropagate, err
		}

		localDigestsByUUID := make(map[string]types.RepairResponse, len(allLocalDigests))

		// filter out too recent local digests to avoid object propagation when all the nodes may be alive
		// or if an upper time bound is configured for shard replica movement
		maxUpdateTime := s.getHashBeatMaxUpdateTime(config, targetNodeName)

		for _, d := range allLocalDigests {
			if d.UpdateTime <= maxUpdateTime {
				localDigestsByUUID[d.ID] = d
			}
		}
		if len(localDigestsByUUID) == 0 {
			// local digests are all too recent, so we can stop now
			break
		}

		remoteStaleUpdateTime := make(map[string]int64, len(localDigestsByUUID))

		if len(localDigestsByUUID) > 0 {
			// fetch digests from remote host in order to avoid sending unnecessary objects
			for currRemoteUUIDBytes := currLocalUUIDBytes; bytes.Compare(currRemoteUUIDBytes, lastLocalUUIDBytes) < 1; {
				if ctx.Err() != nil {
					return localObjectsCount, remoteObjectsCount, objectsToPropagate, ctx.Err()
				}

				currRemoteUUID, err := uuidFromBytes(currRemoteUUIDBytes)
				if err != nil {
					return localObjectsCount, remoteObjectsCount, objectsToPropagate, err
				}

				// TODO could speed up by passing through the target node override upper time bound here
				remoteDigests, err := s.index.replicator.DigestObjectsInRange(ctx,
					s.name, targetNodeAddress, currRemoteUUID, lastLocalUUID, config.diffBatchSize)
				if err != nil {
					return localObjectsCount, remoteObjectsCount, objectsToPropagate, fmt.Errorf("fetching remote object digests: %w", err)
				}

				if len(remoteDigests) == 0 {
					// no more digests in remote host
					break
				}

				remoteObjectsCount += len(remoteDigests)

				for _, d := range remoteDigests {
					localDigest, ok := localDigestsByUUID[d.ID]
					if ok {
						if localDigest.UpdateTime <= d.UpdateTime {
							// older or up to date objects are not propagated
							delete(localDigestsByUUID, d.ID)

							if len(localDigestsByUUID) == 0 {
								// no more local objects need to be propagated in this iteration
								break
							}
						} else {
							// older object is subject to be overwriten
							remoteStaleUpdateTime[d.ID] = d.UpdateTime
						}
					}
				}

				if len(localDigestsByUUID) == 0 {
					// no more local objects need to be propagated in this iteration
					break
				}

				if len(remoteDigests) < config.diffBatchSize {
					break
				}

				lastRemoteUUID := strfmt.UUID(remoteDigests[len(remoteDigests)-1].ID)

				lastRemoteUUIDBytes, err := bytesFromUUID(lastRemoteUUID)
				if err != nil {
					return localObjectsCount, remoteObjectsCount, objectsToPropagate, err
				}

				overflow := incToNextLexValue(lastRemoteUUIDBytes)
				if overflow {
					// no more remote digests need to be fetched
					break
				}

				currRemoteUUIDBytes = lastRemoteUUIDBytes
			}
		}

		for _, obj := range localDigestsByUUID {
			objectsToPropagate = append(objectsToPropagate, objectToPropagate{
				uuid:                  strfmt.UUID(obj.ID),
				lastUpdateTime:        obj.UpdateTime,
				remoteStaleUpdateTime: remoteStaleUpdateTime[obj.ID],
			})
		}

		if len(allLocalDigests) < currBatchSize {
			// no more local objects need to be propagated
			break
		}

		// to avoid reading the last uuid in the next iteration
		overflow := incToNextLexValue(lastLocalUUIDBytes)
		if overflow {
			// no more local objects need to be propagated
			break
		}

		currLocalUUIDBytes = lastLocalUUIDBytes

		limit -= len(localDigestsByUUID)
	}

	// Note: propagations == 0 means local shard is laying behind remote shard,
	// the local shard may receive recent objects when remote shard propagates them

	return localObjectsCount, remoteObjectsCount, objectsToPropagate, nil
}

func (s *Shard) triggerAsyncReplication() {
	if s.asyncReplicationRunning.Load() {
		return
	}
	select {
	case s.asyncReplicationShouldRun <- struct{}{}:
	default:
		// dont block
	}
}

// getHashBeatMaxUpdateTime returns the maximum update time for the hash beat.
// If our local node and the target node have an upper time bound configured, use the
// configured upper time bound instead of the default one
func (s *Shard) getHashBeatMaxUpdateTime(config asyncReplicationConfig, targetNodeName string) int64 {
	localNodeName := s.index.replicator.LocalNodeName()
	for _, override := range config.targetNodeOverrides {
		if override.Equal(&additional.AsyncReplicationTargetNodeOverride{
			SourceNode:   localNodeName,
			TargetNode:   targetNodeName,
			CollectionID: s.class.Class,
			ShardID:      s.name,
		}) {
			return override.UpperTimeBound
		}
	}
	return time.Now().Add(-config.propagationDelay).UnixMilli()
}

func (s *Shard) propagateObjects(ctx context.Context, config asyncReplicationConfig, host string,
	objectsToPropagate []strfmt.UUID, remoteStaleUpdateTime map[strfmt.UUID]int64,
) (res []types.RepairResponse, err error) {
	s.metrics.IncAsyncReplicationPropagationCount()

	defer func(start time.Time) {
		if err != nil {
			s.metrics.IncAsyncReplicationPropagationFailureCount()
			return
		}

		s.metrics.AddAsyncReplicationPropagationObjectCount(len(objectsToPropagate))
		s.metrics.ObserveAsyncReplicationPropagationDuration(time.Since(start))
	}(time.Now())

	type workerResponse struct {
		resp []types.RepairResponse
		err  error
	}

	var wg sync.WaitGroup

	batchCh := make(chan []strfmt.UUID, len(objectsToPropagate)/config.propagationBatchSize+1)
	resultCh := make(chan workerResponse, len(objectsToPropagate)/config.propagationBatchSize+1)

	for range config.propagationConcurrency {
		enterrors.GoWrapper(func() {
			for uuidBatch := range batchCh {
				func() {
					defer wg.Done()
					localObjs, err := s.MultiObjectByID(ctx, wrapIDsInMulti(uuidBatch))
					if err != nil {
						resultCh <- workerResponse{
							err: fmt.Errorf("fetching local objects: %w", err),
						}
						return
					}

					batch := make([]*objects.VObject, 0, len(localObjs))

					for _, obj := range localObjs {
						if obj == nil {
							// local object was deleted meanwhile
							continue
						}

						var vectors map[string][]float32
						var multiVectors map[string][][]float32

						if obj.Vectors != nil {
							vectors = make(map[string][]float32, len(obj.Vectors))
							for targetVector, v := range obj.Vectors {
								vectors[targetVector] = v
							}
						}
						if obj.MultiVectors != nil {
							multiVectors = make(map[string][][]float32, len(obj.MultiVectors))
							for targetVector, v := range obj.MultiVectors {
								multiVectors[targetVector] = v
							}
						}

						obj := &objects.VObject{
							ID:                      obj.ID(),
							LastUpdateTimeUnixMilli: obj.LastUpdateTimeUnix(),
							LatestObject:            &obj.Object,
							Vector:                  obj.Vector,
							Vectors:                 vectors,
							MultiVectors:            multiVectors,
							StaleUpdateTime:         remoteStaleUpdateTime[obj.ID()],
						}

						batch = append(batch, obj)
					}

					if len(batch) > 0 {
						resp, err := s.index.replicator.Overwrite(ctx, host, s.class.Class, s.name, batch)

						resultCh <- workerResponse{
							resp: resp,
							err:  err,
						}
					}
				}()
			}
		}, s.index.logger)
	}

	for i := 0; i < len(objectsToPropagate); {
		actualBatchSize := config.propagationBatchSize
		if i+actualBatchSize > len(objectsToPropagate) {
			actualBatchSize = len(objectsToPropagate) - i
		}

		wg.Add(1)
		batchCh <- objectsToPropagate[i : i+actualBatchSize]

		i += actualBatchSize
	}

	enterrors.GoWrapper(func() {
		wg.Wait()
		close(batchCh)
		close(resultCh)
	}, s.index.logger)

	ec := errorcompounder.New()

	for r := range resultCh {
		if r.err != nil {
			ec.Add(err)
			continue
		}

		res = append(res, r.resp...)
	}

	if len(res) > 0 {
		return res, nil
	}

	return nil, ec.ToError()
}
