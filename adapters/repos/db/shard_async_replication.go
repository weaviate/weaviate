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
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
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
	defaultFrequency                   = 5 * time.Second
	defaultFrequencyWhilePropagating   = 10 * time.Millisecond
	defaultAliveNodesCheckingFrequency = 1 * time.Second
	defaultLoggingFrequency            = 3 * time.Second
	defaultDiffPerNodeTimeout          = 10 * time.Second
	defaultPropagationTimeout          = 30 * time.Second
	defaultPropagationLimit            = 100_000
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
	batchSize                   int
}

func (s *Shard) asyncReplicationConfig() (config asyncReplicationConfig, err error) {
	config.hashtreeHeight, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_HASHTREE_HEIGHT"), defaultHashtreeHeight)
	if err != nil {
		return
	}
	if config.hashtreeHeight < 1 || config.hashtreeHeight > maxHashtreeHeight {
		return asyncReplicationConfig{}, fmt.Errorf("hashtree height out of range: min height 1, max height %d", maxHashtreeHeight)
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

	config, err := s.asyncReplicationConfig()
	if err != nil {
		return err
	}

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
		s.hashtree, err = hashtree.DeserializeCompactHashTree(bufio.NewReader(f))
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

	s.hashtree, err = s.buildCompactHashTree(config.hashtreeHeight)
	if err != nil {
		return err
	}

	// sync hashtree with current object states

	enterrors.GoWrapper(func() {
		objCount := 0
		prevProgressLogging := time.Now()

		// data inserted before v1.26 does not contain the required secondary index
		// to support async replication thus such data is not inserted into the hashtree
		err := bucket.IterateObjectDigests(ctx, 2, func(object *storobj.Object) error {
			if time.Since(prevProgressLogging) > time.Second {
				s.index.logger.
					WithField("action", "async_replication").
					WithField("class_name", s.class.Class).
					WithField("shard_name", s.name).
					WithField("object_count", objCount).
					WithField("took", fmt.Sprintf("%v", time.Since(start))).
					Infof("hashtree initialization is progress...")
				prevProgressLogging = time.Now()
			}

			uuid, err := uuid.MustParse(object.ID().String()).MarshalBinary()
			if err != nil {
				return err
			}

			if ctx.Err() != nil {
				return ctx.Err()
			}

			err = s.mayUpsertObjectHashTree(object, uuid, objectInsertStatus{})
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

func (s *Shard) UpdateAsyncReplication(_ context.Context, enabled bool) error {
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

func (s *Shard) buildCompactHashTree(height int) (hashtree.AggregatedHashTree, error) {
	return hashtree.NewCompactHashTree(math.MaxUint64, height)
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

		backoffTimer := interval.NewBackoffTimer()

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

	aliveHosts := s.allAliveHostnames()
	s.setLastComparedNodes(aliveHosts)

	shardDiffReader, err := s.index.replicator.CollectShardDifferences(ctx, s.name, aliveHosts, ht, config.diffPerNodeTimeout)
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
		initialToken, finalToken, err := rangeReader.Next()
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
			initialToken,
			finalToken,
			config.propagationLimit-objectsPropagated,
		)
		if err != nil {
			return nil, fmt.Errorf("propagating local objects: %w", err)
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

func (s *Shard) stepsTowardsShardConsistency(ctx context.Context, config asyncReplicationConfig,
	shardName string, host string, initialToken, finalToken uint64, limit int,
) (localObjects, remoteObjects, propagations int, err error) {
	for localLastReadToken := initialToken; localLastReadToken < finalToken; {
		if ctx.Err() != nil {
			return localObjects, remoteObjects, propagations, ctx.Err()
		}

		localDigests, newLocalLastReadToken, err := s.index.DigestObjectsInTokenRange(ctx, shardName, localLastReadToken, finalToken, config.batchSize)
		if err != nil && !errors.Is(err, storobj.ErrLimitReached) {
			return localObjects, remoteObjects, propagations, fmt.Errorf("fetching local object digests: %w", err)
		}

		if len(localDigests) == 0 {
			// no more local objects need to be propagated in this iteration
			localLastReadToken = newLocalLastReadToken
			continue
		}

		localDigestsByUUID := make(map[string]replica.RepairResponse, len(localDigests))

		for _, d := range localDigests {
			localDigestsByUUID[d.ID] = d
		}

		localObjects += len(localDigestsByUUID)

		remoteLastTokenRead := localLastReadToken

		remoteStaleUpdateTime := make(map[string]int64, len(localDigestsByUUID))

		// fetch digests from remote host in order to avoid sending unnecessary objects
		for remoteLastTokenRead < newLocalLastReadToken {
			remoteDigests, newRemoteLastTokenRead, err := s.index.replicator.DigestObjectsInTokenRange(ctx,
				shardName, host, remoteLastTokenRead, newLocalLastReadToken, config.batchSize)
			if err != nil && !strings.Contains(err.Error(), storobj.ErrLimitReached.Error()) {
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

			remoteLastTokenRead = newRemoteLastTokenRead

			if len(localDigestsByUUID) == 0 {
				// no more local objects need to be propagated in this iteration
				break
			}
		}

		if len(localDigestsByUUID) == 0 {
			// no more local objects need to be propagated in this iteration
			localLastReadToken = newLocalLastReadToken
			continue
		}

		uuids := make([]strfmt.UUID, 0, len(localDigestsByUUID))
		for uuid := range localDigestsByUUID {
			uuids = append(uuids, strfmt.UUID(uuid))
		}

		replicaObjs, err := s.index.FetchObjects(ctx, shardName, uuids)
		if err != nil {
			return localObjects, remoteObjects, propagations, fmt.Errorf("fetching local objects: %w", err)
		}

		mergeObjs := make([]*objects.VObject, 0, len(replicaObjs))

		for _, replicaObj := range replicaObjs {
			if replicaObj.Deleted {
				continue
			}

			var vectors models.Vectors

			if replicaObj.Object.Vectors != nil {
				vectors = make(models.Vectors, len(replicaObj.Object.Vectors))
				for i, v := range replicaObj.Object.Vectors {
					vectors[i] = v
				}
			}

			obj := &objects.VObject{
				ID:                      replicaObj.ID,
				LastUpdateTimeUnixMilli: replicaObj.LastUpdateTimeUnixMilli,
				LatestObject:            &replicaObj.Object.Object,
				Vector:                  replicaObj.Object.Vector,
				Vectors:                 vectors,
				StaleUpdateTime:         remoteStaleUpdateTime[replicaObj.ID.String()],
			}

			mergeObjs = append(mergeObjs, obj)
		}

		if len(mergeObjs) == 0 {
			// no more local objects need to be propagated in this iteration
			localLastReadToken = newLocalLastReadToken
			continue
		}

		resp, err := s.index.replicator.Overwrite(ctx, host, s.class.Class, shardName, mergeObjs)
		if err != nil {
			return localObjects, remoteObjects, propagations, fmt.Errorf("propagating local objects: %w", err)
		}

		for _, r := range resp {
			// NOTE: deleted objects are not propagated but locally deleted when conflict is detected

			if !r.Deleted ||
				s.index.Config.DeletionStrategy == models.ReplicationConfigDeletionStrategyNoAutomatedResolution {
				continue
			}

			if s.index.Config.DeletionStrategy == models.ReplicationConfigDeletionStrategyDeleteOnConflict ||
				(s.index.Config.DeletionStrategy == models.ReplicationConfigDeletionStrategyTimeBasedResolution &&
					r.UpdateTime > localDigestsByUUID[r.ID].UpdateTime) {

				err := s.DeleteObject(ctx, strfmt.UUID(r.ID), time.UnixMilli(r.UpdateTime))
				if err != nil {
					return localObjects, remoteObjects, propagations, fmt.Errorf("deleting local objects: %w", err)
				}
			}
		}

		propagations += len(mergeObjs)
		localLastReadToken = newLocalLastReadToken

		if propagations >= limit {
			break
		}
	}

	// Note: propagations == 0 means local shard is laying behind remote shard,
	// the local shard may receive recent objects when remote shard propagates them

	return localObjects, remoteObjects, propagations, nil
}
