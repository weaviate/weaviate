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
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/interval"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const propagationLimitPerHashbeatIteration = 100_000

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

	s.asyncReplicationCtx, s.asyncReplicationCancelFunc = context.WithCancel(context.Background())

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
	}

	if s.hashtree != nil {
		s.hashtreeFullyInitialized = true
		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			Info("hashtree successfully initialized")

		s.initHashBeater()
		return nil
	}

	s.hashtree, err = s.buildCompactHashTree()
	if err != nil {
		return err
	}

	// sync hashtree with current object states

	enterrors.GoWrapper(func() {
		objCount := 0
		prevProgressLogging := time.Now()

		// data inserted before v1.26 does not contain the required secondary index
		// to support async replication thus such data is not inserted into the hashtree
		err := bucket.IterateObjectsWith(s.asyncReplicationCtx, 2, func(object *storobj.Object) error {
			if time.Since(prevProgressLogging) > time.Second {
				s.index.logger.
					WithField("action", "async_replication").
					WithField("class_name", s.class.Class).
					WithField("shard_name", s.name).
					WithField("object_count", objCount).
					Infof("hashtree initialization is progress...")
				prevProgressLogging = time.Now()
			}

			uuid, err := uuid.MustParse(object.ID().String()).MarshalBinary()
			if err != nil {
				return err
			}

			// TODO: we have to stop initialization if async replication is stopped
			err = s.mayUpsertObjectHashTree(object, uuid, objectInsertStatus{})
			if err != nil {
				return err
			}

			objCount++

			return nil
		})
		if err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Errorf("iterating objects during hashtree initialization: %v", err)
			return
		}

		s.asyncReplicationRWMux.Lock()

		if s.asyncReplicationCtx.Err() != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Info("hashtree initialization stopped")

			s.asyncReplicationRWMux.Unlock()
			return
		}

		s.hashtreeFullyInitialized = true
		s.asyncReplicationRWMux.Unlock()

		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			Info("hashtree successfully initialized")

		s.initHashBeater()
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
		s.dumpHashTree()
	}

	s.hashtree = nil
	s.hashtreeFullyInitialized = false
}

func (s *Shard) UpdateAsyncReplication(ctx context.Context, enabled bool) error {
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

func (s *Shard) buildCompactHashTree() (hashtree.AggregatedHashTree, error) {
	return hashtree.NewCompactHashTree(math.MaxUint64, 16)
}

func (s *Shard) dumpHashTree() error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(time.Now().UnixNano()))

	hashtreeFilename := filepath.Join(s.pathHashTree(), fmt.Sprintf("hashtree-%x.ht", string(b[:])))

	f, err := os.OpenFile(hashtreeFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		return fmt.Errorf("storing hashtree in %q: %w", hashtreeFilename, err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	_, err = s.hashtree.Serialize(w)
	if err != nil {
		return fmt.Errorf("storing hashtree in %q: %w", hashtreeFilename, err)
	}

	err = w.Flush()
	if err != nil {
		return fmt.Errorf("storing hashtree in %q: %w", hashtreeFilename, err)
	}

	err = f.Sync()
	if err != nil {
		return fmt.Errorf("storing hashtree in %q: %w", hashtreeFilename, err)
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

func (s *Shard) initHashBeater() {
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

		t := time.NewTicker(1 * time.Second)
		defer t.Stop()

		backoffs := []time.Duration{
			1 * time.Second,
			3 * time.Second,
			5 * time.Second,
		}

		backoffTimer := interval.NewBackoffTimer(backoffs...)

		for it := 0; ; it++ {
			select {
			case <-s.asyncReplicationCtx.Done():
				return
			case <-t.C:
				err := s.waitUntilObjectPropagationRequired(s.asyncReplicationCtx)
				if s.asyncReplicationCtx.Err() != nil {
					return
				}
				if err != nil {
					s.index.logger.
						WithField("action", "async_replication").
						WithField("class_name", s.class.Class).
						WithField("shard_name", s.name).
						WithField("hashbeat_iteration", it).
						Warn(err)
					return
				}

				stats, err := s.hashBeat()
				if err != nil {
					if s.asyncReplicationCtx.Err() != nil {
						return
					}

					s.index.logger.
						WithField("action", "async_replication").
						WithField("class_name", s.class.Class).
						WithField("shard_name", s.name).
						WithField("hashbeat_iteration", it).
						Warnf("hashbeat iteration failed: %v", err)

					time.Sleep(backoffTimer.CurrentInterval())
					backoffTimer.IncreaseInterval()

					s.objectPropagationRequired()

					continue
				}

				localObjects := 0
				remoteObjects := 0
				objectsPropagated := 0
				var objectProgationTook time.Duration
				var propagationErr error

				for _, stat := range stats.hostStats {
					localObjects += stat.localObjects
					remoteObjects += stat.remoteObjects
					objectsPropagated += stat.objectsPropagated
					objectProgationTook += stat.objectProgationTook

					if stat.err != nil && propagationErr == nil {
						propagationErr = fmt.Errorf("%w: host %s", stat.err, stat.host)
					}
				}

				logEntry := s.index.logger.
					WithField("action", "async_replication").
					WithField("class_name", s.class.Class).
					WithField("shard_name", s.name).
					WithField("hashbeat_iteration", it).
					WithField("hosts", s.getLastComparedHosts()).
					WithField("diff_calculation_took", stats.diffCalculationTook.String()).
					WithField("local_objects", localObjects).
					WithField("remote_objects", remoteObjects).
					WithField("objects_propagated", objectsPropagated).
					WithField("object_progation_took", objectProgationTook.String())

				if propagationErr == nil {
					if objectsPropagated > 0 || it%1000 == 0 {
						logEntry.Info("hashbeat iteration successfully completed")
					}

					backoffTimer.Reset()

					if objectsPropagated > 0 {
						s.objectPropagationRequired()
					}
				} else {
					logEntry.Warnf("hashbeat iteration failed: %v", propagationErr)

					time.Sleep(backoffTimer.CurrentInterval())
					backoffTimer.IncreaseInterval()

					s.objectPropagationRequired()
				}
			}
		}
	}, s.index.logger)

	enterrors.GoWrapper(func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()

		// just in case host comparison is not enough
		// this way we ensure hashbeat will be always triggered
		jict := time.NewTicker(10 * time.Second)
		defer jict.Stop()

		for {
			select {
			case <-s.asyncReplicationCtx.Done():
				s.objectPropagationNeededCond.Signal()
				return
			case <-t.C:
				comparedHosts := s.getLastComparedHosts()
				aliveHosts := s.allAliveHostnames()

				slices.Sort(comparedHosts)
				slices.Sort(aliveHosts)

				if !slices.Equal(comparedHosts, aliveHosts) {
					s.objectPropagationRequired()
				}
			case <-jict.C:
				s.objectPropagationRequired()
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

func (s *Shard) objectPropagationRequired() {
	s.objectPropagationNeededCond.L.Lock()
	s.objectPropagationNeeded = true
	s.objectPropagationNeededCond.Signal()
	s.objectPropagationNeededCond.L.Unlock()
}

func (s *Shard) waitUntilObjectPropagationRequired(ctx context.Context) error {
	s.objectPropagationNeededCond.L.Lock()
	for !s.objectPropagationNeeded {
		if ctx.Err() != nil {
			s.objectPropagationNeededCond.L.Unlock()
			return ctx.Err()
		}
		s.objectPropagationNeededCond.Wait()
	}
	s.objectPropagationNeeded = false
	s.objectPropagationNeededCond.L.Unlock()
	return nil
}

type hashBeatStats struct {
	diffCalculationTook time.Duration
	hostStats           []hashBeatHostStats
}

type hashBeatHostStats struct {
	host                string
	localObjects        int
	remoteObjects       int
	objectsPropagated   int
	objectProgationTook time.Duration
	err                 error
}

func (s *Shard) hashBeat() (stats hashBeatStats, err error) {
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()

	if s.hashtree == nil {
		// handling the case of a hashtree being explicitly set to nil
		return
	}

	diffCalculationStart := time.Now()

	replyCh, _, err := s.index.replicator.CollectShardDifferences(s.asyncReplicationCtx, s.name, s.hashtree)
	if err != nil {
		return stats, fmt.Errorf("collecting differences: %w", err)
	}

	stats.diffCalculationTook = time.Since(diffCalculationStart)

	// an error will be returned when it was not possible to collect differences with any host
	var diffCollectionDone bool
	var diffCollectionErr error

	for r := range replyCh {
		if r.Err != nil {
			if !errors.Is(r.Err, hashtree.ErrNoMoreRanges) && !diffCollectionDone {
				diffCollectionErr = fmt.Errorf("collecting differences: %w", r.Err)
			}
			continue
		}

		shardDiffReader := r.Value
		rangeReader := shardDiffReader.RangeReader

		objectProgationStart := time.Now()

		localObjects := 0
		remoteObjects := 0
		objectsPropagated := 0

		var propagationErr error

		for {
			if s.asyncReplicationCtx.Err() != nil {
				return stats, s.asyncReplicationCtx.Err()
			}

			initialToken, finalToken, err := rangeReader.Next()
			if errors.Is(err, hashtree.ErrNoMoreRanges) {
				break
			}
			if err != nil {
				propagationErr = fmt.Errorf("reading collected differences: %w", err)
				break
			}

			localObjs, remoteObjs, propagations, err := s.stepsTowardsShardConsistency(
				s.asyncReplicationCtx,
				s.name,
				shardDiffReader.Host,
				initialToken,
				finalToken,
				propagationLimitPerHashbeatIteration-objectsPropagated,
			)
			if err != nil {
				propagationErr = fmt.Errorf("propagating local objects: %v", err)
				break
			}

			localObjects += localObjs
			remoteObjects += remoteObjs
			objectsPropagated += propagations

			if objectsPropagated >= propagationLimitPerHashbeatIteration {
				break
			}
		}

		stat := hashBeatHostStats{
			host:                shardDiffReader.Host,
			localObjects:        localObjects,
			remoteObjects:       remoteObjects,
			objectsPropagated:   objectsPropagated,
			objectProgationTook: time.Since(objectProgationStart),
			err:                 propagationErr,
		}

		stats.hostStats = append(stats.hostStats, stat)

		diffCollectionDone = true
		diffCollectionErr = nil
	}

	s.setLastComparedNodes(s.allAliveHostnames())

	return stats, diffCollectionErr
}

func (s *Shard) stepsTowardsShardConsistency(ctx context.Context,
	shardName string, host string, initialToken, finalToken uint64, limit int,
) (localObjects, remoteObjects, propagations int, err error) {
	const maxBatchSize = 100

	for localLastReadToken := initialToken; localLastReadToken < finalToken; {
		localDigests, newLocalLastReadToken, err := s.index.DigestObjectsInTokenRange(ctx, shardName, localLastReadToken, finalToken, maxBatchSize)
		if err != nil && !errors.Is(err, storobj.ErrLimitReached) {
			return localObjects, remoteObjects, propagations, fmt.Errorf("fetching local object digests: %w", err)
		}

		localDigestsByUUID := make(map[string]replica.RepairResponse, len(localDigests))

		for _, d := range localDigests {
			// deleted objects are not propagated
			if !d.Deleted {
				localDigestsByUUID[d.ID] = d
			}
		}

		if len(localDigestsByUUID) == 0 {
			// no more local objects need to be propagated in this iteration
			localLastReadToken = newLocalLastReadToken
			continue
		}

		localObjects += len(localDigestsByUUID)

		remoteLastTokenRead := localLastReadToken

		remoteStaleUpdateTime := make(map[string]int64, len(localDigestsByUUID))

		// fetch digests from remote host in order to avoid sending unnecessary objects
		for remoteLastTokenRead < newLocalLastReadToken {
			remoteDigests, newRemoteLastTokenRead, err := s.index.replicator.DigestObjectsInTokenRange(ctx,
				shardName, host, remoteLastTokenRead, newLocalLastReadToken, maxBatchSize)
			if err != nil && !strings.Contains(err.Error(), storobj.ErrLimitReached.Error()) {
				return localObjects, remoteObjects, propagations, fmt.Errorf("fetching remote object digests: %w", err)
			}

			if len(remoteDigests) == 0 {
				// no more objects in remote host
				break
			}

			for _, d := range remoteDigests {
				if len(localDigestsByUUID) == 0 {
					// no more local objects need to be propagated in this iteration
					break
				}

				if d.Deleted {
					// object was deleted in remote host
					delete(localDigestsByUUID, d.ID)
					continue
				}

				remoteObjects++

				localDigest, ok := localDigestsByUUID[d.ID]
				if ok {
					if localDigest.UpdateTime <= d.UpdateTime {
						// older or up to date objects are not propagated
						delete(localDigestsByUUID, d.ID)
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
				ID:              replicaObj.ID,
				LatestObject:    &replicaObj.Object.Object,
				Vector:          replicaObj.Object.Vector,
				Vectors:         vectors,
				StaleUpdateTime: remoteStaleUpdateTime[replicaObj.ID.String()],
			}

			mergeObjs = append(mergeObjs, obj)
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
