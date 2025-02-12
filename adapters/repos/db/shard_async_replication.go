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

package db

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/interval"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const maxHashtreeHeight = 20

const (
	defaultHashtreeHeight              = 16
	defaultFrequency                   = 30 * time.Second
	defaultFrequencyWhilePropagating   = 10 * time.Millisecond
	defaultAliveNodesCheckingFrequency = 1 * time.Second
	defaultLoggingFrequency            = 5 * time.Second
	defaultDiffPerNodeTimeout          = 10 * time.Second
	defaultPropagationTimeout          = 30 * time.Second
	defaultPropagationLimit            = 10_000
	defaultPropagationDelay            = 30 * time.Second
	defaultBatchSize                   = 100
)

type asyncReplicationConfig struct {
	hashtreeHeight              int
	frequency                   time.Duration
	frequencyWhilePropagating   time.Duration
	aliveNodesCheckingFrequency time.Duration
	loggingFrequency            time.Duration
	diffPerNodeTimeout          time.Duration
	propagationTimeout          time.Duration
	propagationLimit            int
	propagationDelay            time.Duration
	batchSize                   int
}

func (s *Shard) getAsyncReplicationConfig() (config asyncReplicationConfig, err error) {
	config.hashtreeHeight, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_HASHTREE_HEIGHT"), defaultHashtreeHeight)
	if err != nil {
		return
	}
	if config.hashtreeHeight < 0 || config.hashtreeHeight > maxHashtreeHeight {
		return asyncReplicationConfig{}, fmt.Errorf("hashtree height out of range: min height 0, max height %d", maxHashtreeHeight)
	}

	config.frequency, err = optParseDuration(os.Getenv("ASYNC_REPLICATION_FREQUENCY"), defaultFrequency)
	if err != nil {
		return
	}

	config.frequencyWhilePropagating, err = optParseDuration(os.Getenv("ASYNC_REPLICATION_FREQUENCY_WHILE_PROPAGATING"), defaultFrequencyWhilePropagating)
	if err != nil {
		return
	}

	config.aliveNodesCheckingFrequency, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_ALIVE_NODES_CHECKING_FREQUENCY"), defaultAliveNodesCheckingFrequency)
	if err != nil {
		return
	}

	config.loggingFrequency, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_LOGGING_FREQUENCY"), defaultLoggingFrequency)
	if err != nil {
		return
	}

	config.diffPerNodeTimeout, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_DIFF_PER_NODE_TIMEOUT"), defaultDiffPerNodeTimeout)
	if err != nil {
		return
	}

	config.propagationTimeout, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_PROPAGATION_TIMEOUT"), defaultPropagationTimeout)
	if err != nil {
		return
	}

	config.propagationLimit, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_PROPAGATION_LIMIT"), defaultPropagationLimit)
	if err != nil {
		return
	}

	config.propagationDelay, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_PROPAGATION_DELAY"), defaultPropagationDelay)
	if err != nil {
		return
	}

	config.batchSize, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_BATCH_SIZE"), defaultBatchSize)
	if err != nil {
		return
	}

	return
}

func optParseInt(s string, defaultInt int) (int, error) {
	if s == "" {
		return defaultInt, nil
	}
	return strconv.Atoi(s)
}

func optParseDuration(s string, defaultDuration time.Duration) (time.Duration, error) {
	if s == "" {
		return defaultDuration, nil
	}
	return time.ParseDuration(s)
}

func (s *Shard) initAsyncReplication() error {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)

	if bucket.GetSecondaryIndices() < 2 {
		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			Warn("secondary index for token ranges is not available")
		return nil
	}

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

		if s.hashtree.Height() != config.hashtreeHeight {
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

	// sync hashtree with current object states

	enterrors.GoWrapper(func() {
		objCount := 0
		prevProgressLogging := time.Now()

		err := bucket.ApplyToObjectDigests(ctx, func(object *storobj.Object) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if time.Since(prevProgressLogging) > time.Second {
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

			err = s.mayUpsertObjectHashTree(object, uuidBytes, objectInsertStatus{})
			if err != nil {
				return err
			}

			objCount++

			return nil
		})
		if err != nil {
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
				Errorf("iterating objects during hashtree initialization: %v", err)
			return
		}

		s.asyncReplicationRWMux.Lock()

		if s.hashtree == nil {
			s.asyncReplicationRWMux.Unlock()

			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Info("hashtree initialization stopped")
			return
		}

		s.hashtreeFullyInitialized = true

		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			WithField("object_count", objCount).
			WithField("took", fmt.Sprintf("%v", time.Since(start))).
			Info("hashtree successfully initialized")

		s.asyncReplicationRWMux.Unlock()

		s.initHashBeater(ctx, config)
	}, s.index.logger)

	return nil
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

func (s *Shard) updateAsyncReplicationConfig(_ context.Context, enabled bool) error {
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
	s.hashtreeFullyInitialized = false

	return nil
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
	propagationRequired := make(chan struct{})

	var lastHashbeat time.Time
	var lastHashbeatPropagatedObjects bool
	var lastHashbeatMux sync.Mutex

	enterrors.GoWrapper(func() {
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
			case <-propagationRequired:
				stats, err := s.hashBeat(ctx, config)
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
								Info("hashbeat iteration successfully completed: no differences were found")
						}

						backoffTimer.Reset()
						lastHashbeatMux.Lock()
						lastHashbeat = time.Now()
						lastHashbeatPropagatedObjects = false
						lastHashbeatMux.Unlock()
						continue
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
					lastHashbeat = time.Now()
					lastHashbeatPropagatedObjects = false
					lastHashbeatMux.Unlock()
					continue
				}

				if time.Since(lastLog) >= config.loggingFrequency {
					lastLog = time.Now()

					s.index.logger.
						WithField("action", "async_replication").
						WithField("class_name", s.class.Class).
						WithField("shard_name", s.name).
						WithField("host", stats.host).
						WithField("diff_calculation_took", stats.diffCalculationTook.String()).
						WithField("local_objects", stats.localObjects).
						WithField("remote_objects", stats.remoteObjects).
						WithField("objects_propagated", stats.objectsPropagated).
						WithField("object_progation_took", stats.objectProgationTook.String()).
						Info("hashbeat iteration successfully completed")
				}

				backoffTimer.Reset()
				lastHashbeatMux.Lock()
				lastHashbeat = time.Now()
				lastHashbeatPropagatedObjects = stats.objectsPropagated > 0
				lastHashbeatMux.Unlock()
			}
		}
	}, s.index.logger)

	enterrors.GoWrapper(func() {
		ft := time.NewTicker(10 * time.Millisecond)
		defer ft.Stop()

		nt := time.NewTicker(config.aliveNodesCheckingFrequency)
		defer nt.Stop()

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
					propagationRequired <- struct{}{}
					s.setLastComparedNodes(aliveHosts)
				}
			case <-ft.C:
				var shouldHashbeat bool
				lastHashbeatMux.Lock()
				shouldHashbeat = (lastHashbeatPropagatedObjects && time.Since(lastHashbeat) >= config.frequencyWhilePropagating) ||
					time.Since(lastHashbeat) >= config.frequency
				lastHashbeatMux.Unlock()

				if shouldHashbeat {
					propagationRequired <- struct{}{}
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
	return s.index.replicator.AllHostnames()
}

type hashBeatHostStats struct {
	host                string
	diffCalculationTook time.Duration
	localObjects        int
	remoteObjects       int
	objectsPropagated   int
	objectProgationTook time.Duration
}

func (s *Shard) hashBeat(ctx context.Context, config asyncReplicationConfig) (stats *hashBeatHostStats, err error) {
	var ht hashtree.AggregatedHashTree

	s.asyncReplicationRWMux.RLock()
	if s.hashtree == nil {
		s.asyncReplicationRWMux.RUnlock()
		// handling the case of a hashtree being explicitly set to nil
		return nil, fmt.Errorf("hashtree not initialized on shard %q", s.ID())
	}
	ht = s.hashtree
	s.asyncReplicationRWMux.RUnlock()

	diffCalculationStart := time.Now()

	shardDiffReader, err := s.index.replicator.CollectShardDifferences(ctx, s.name, ht, config.diffPerNodeTimeout)
	if err != nil {
		return nil, fmt.Errorf("collecting differences: %w", err)
	}

	diffCalculationTook := time.Since(diffCalculationStart)

	rangeReader := shardDiffReader.RangeReader

	objectProgationStart := time.Now()

	localObjects := 0
	remoteObjects := 0
	objectsPropagated := 0

	ctx, cancel := context.WithTimeout(ctx, config.propagationTimeout)
	defer cancel()

	for objectsPropagated < config.propagationLimit {
		initialLeaf, finalLeaf, err := rangeReader.Next()
		if err != nil {
			if errors.Is(err, hashtree.ErrNoMoreRanges) {
				break
			}
			return nil, fmt.Errorf("reading collected differences: %w", err)
		}

		localObjs, remoteObjs, propagations, err := s.stepsTowardsShardConsistency(
			ctx,
			config,
			s.name,
			shardDiffReader.Host,
			initialLeaf,
			finalLeaf,
			config.propagationLimit-objectsPropagated,
		)
		if err != nil {
			return nil, fmt.Errorf("achieving shard consistency : %w", err)
		}

		localObjects += localObjs
		remoteObjects += remoteObjs
		objectsPropagated += propagations
	}

	return &hashBeatHostStats{
		host:                shardDiffReader.Host,
		diffCalculationTook: diffCalculationTook,
		localObjects:        localObjects,
		remoteObjects:       remoteObjects,
		objectsPropagated:   objectsPropagated,
		objectProgationTook: time.Since(objectProgationStart),
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

func (s *Shard) stepsTowardsShardConsistency(ctx context.Context, config asyncReplicationConfig,
	shardName string, host string, initialLeaf, finalLeaf uint64, limit int,
) (localObjects, remoteObjects, propagations int, err error) {
	hashtreeHeight := config.hashtreeHeight

	finalUUIDBytes := make([]byte, 16)
	binary.BigEndian.PutUint64(finalUUIDBytes, finalLeaf<<(64-hashtreeHeight)|((1<<(64-hashtreeHeight))-1))
	copy(finalUUIDBytes[8:], []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	finalUUID, err := uuidFromBytes(finalUUIDBytes)
	if err != nil {
		return localObjects, remoteObjects, propagations, err
	}

	currLocalUUIDBytes := make([]byte, 16)
	binary.BigEndian.PutUint64(currLocalUUIDBytes, initialLeaf<<(64-hashtreeHeight))

	shouldContinueFetchingLocalData := true

	for shouldContinueFetchingLocalData && bytes.Compare(currLocalUUIDBytes, finalUUIDBytes) < 1 {
		if ctx.Err() != nil {
			return localObjects, remoteObjects, propagations, ctx.Err()
		}

		currLocalUUID, err := uuidFromBytes(currLocalUUIDBytes)
		if err != nil {
			return localObjects, remoteObjects, propagations, err
		}

		allLocalDigests, err := s.index.DigestObjectsInRange(ctx, shardName, currLocalUUID, finalUUID, config.batchSize)
		if err != nil {
			return localObjects, remoteObjects, propagations, fmt.Errorf("fetching local object digests: %w", err)
		}

		// filter out too recent local digests to avoid object propagation when all the nodes may be alive
		localDigests := make([]replica.RepairResponse, 0, len(allLocalDigests))

		maxUpdateTime := time.Now().Add(-config.propagationDelay).UnixMilli()

		for _, d := range allLocalDigests {
			if d.UpdateTime <= maxUpdateTime {
				localDigests = append(localDigests, d)
			}
		}

		if len(localDigests) == 0 {
			// no more local objects need to be propagated in this iteration
			break
		}

		// iteration should stop when all local digests within the range has been read
		shouldContinueFetchingLocalData = len(localDigests) == config.batchSize

		lastLocalUUID := strfmt.UUID(localDigests[len(localDigests)-1].ID)

		lastLocalUUIDBytes, err := bytesFromUUID(lastLocalUUID)
		if err != nil {
			return localObjects, remoteObjects, propagations, err
		}

		localDigestsByUUID := make(map[string]replica.RepairResponse, len(localDigests))

		for _, d := range localDigests {
			localDigestsByUUID[d.ID] = d
		}

		localObjects += len(localDigestsByUUID)

		remoteStaleUpdateTime := make(map[string]int64, len(localDigestsByUUID))

		// fetch digests from remote host in order to avoid sending unnecessary objects
		for currRemoteUUIDBytes := currLocalUUIDBytes; bytes.Compare(currRemoteUUIDBytes, lastLocalUUIDBytes) < 1; {
			if ctx.Err() != nil {
				return localObjects, remoteObjects, propagations, ctx.Err()
			}

			currRemoteUUID, err := uuidFromBytes(currRemoteUUIDBytes)
			if err != nil {
				return localObjects, remoteObjects, propagations, err
			}

			remoteDigests, err := s.index.replicator.DigestObjectsInRange(ctx,
				shardName, host, currRemoteUUID, lastLocalUUID, config.batchSize)
			if err != nil {
				return localObjects, remoteObjects, propagations, fmt.Errorf("fetching remote object digests: %w", err)
			}

			if len(remoteDigests) == 0 {
				// no more objects in remote host
				break
			}

			remoteObjects += len(remoteDigests)

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

			lastRemoteUUID := strfmt.UUID(remoteDigests[len(remoteDigests)-1].ID)

			currRemoteUUIDBytes, err = bytesFromUUID(lastRemoteUUID)
			if err != nil {
				return localObjects, remoteObjects, propagations, err
			}

			if len(remoteDigests) < config.batchSize {
				break
			}
		}

		// to avoid reading the last uuid in the next iteration
		overflow := incToNextLexValue(lastLocalUUIDBytes)
		if overflow {
			// no more local objects need to be propagated
			break
		}

		currLocalUUIDBytes = lastLocalUUIDBytes

		if len(localDigestsByUUID) == 0 {
			// no more local objects need to be propagated in this iteration
			continue
		}

		uuids := make([]strfmt.UUID, 0, len(localDigestsByUUID))
		for uuid := range localDigestsByUUID {
			uuids = append(uuids, strfmt.UUID(uuid))
		}

		localObjs, err := s.MultiObjectByID(ctx, wrapIDsInMulti(uuids))
		if err != nil {
			return localObjects, remoteObjects, propagations, fmt.Errorf("fetching local objects: %w", err)
		}

		mergeObjs := make([]*objects.VObject, 0, len(localObjs))

		for _, obj := range localObjs {
			var vectors models.Vectors

			if obj.Vectors != nil {
				vectors = make(models.Vectors, len(obj.Vectors))
				for i, v := range obj.Vectors {
					vectors[i] = v
				}
			}

			obj := &objects.VObject{
				ID:                      obj.ID(),
				LastUpdateTimeUnixMilli: obj.LastUpdateTimeUnix(),
				LatestObject:            &obj.Object,
				Vector:                  obj.Vector,
				Vectors:                 vectors,
				StaleUpdateTime:         remoteStaleUpdateTime[obj.ID().String()],
			}

			mergeObjs = append(mergeObjs, obj)
		}

		if len(mergeObjs) == 0 {
			// no more local objects need to be propagated in this iteration
			continue
		}

		resp, err := s.index.replicator.Overwrite(ctx, host, s.class.Class, shardName, mergeObjs)
		if err != nil {
			return localObjects, remoteObjects, propagations, fmt.Errorf("propagating local objects: %w", err)
		}

		for _, r := range resp {
			// NOTE: deleted objects are not propagated but locally deleted when conflict is detected

			deletionStrategy := s.index.DeletionStrategy()

			if !r.Deleted ||
				deletionStrategy == models.ReplicationConfigDeletionStrategyNoAutomatedResolution {
				continue
			}

			if deletionStrategy == models.ReplicationConfigDeletionStrategyDeleteOnConflict ||
				(deletionStrategy == models.ReplicationConfigDeletionStrategyTimeBasedResolution &&
					r.UpdateTime > localDigestsByUUID[r.ID].UpdateTime) {

				err := s.DeleteObject(ctx, strfmt.UUID(r.ID), time.UnixMilli(r.UpdateTime))
				if err != nil {
					return localObjects, remoteObjects, propagations, fmt.Errorf("deleting local objects: %w", err)
				}
			}
		}

		propagations += len(mergeObjs)

		if propagations >= limit {
			break
		}
	}

	// Note: propagations == 0 means local shard is laying behind remote shard,
	// the local shard may receive recent objects when remote shard propagates them

	return localObjects, remoteObjects, propagations, nil
}
