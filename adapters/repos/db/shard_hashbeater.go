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
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/interval"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const propagationLimitPerHashbeatIteration = 100_000

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
			case <-s.hashBeaterCtx.Done():
				return
			case <-t.C:
				err := s.waitUntilObjectPropagationRequired(s.hashBeaterCtx)
				if s.hashBeaterCtx.Err() != nil {
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
				if s.hashBeaterCtx.Err() != nil {
					return
				}
				if err != nil {
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
					logEntry.Info("hashbeat iteration successfully completed")

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
			case <-s.hashBeaterCtx.Done():
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
	s.hashtreeRWMux.RLock()
	defer s.hashtreeRWMux.RUnlock()

	if s.hashtree == nil {
		// handling the case of a hashtree being explicitly set to nil
		return
	}

	diffCalculationStart := time.Now()

	replyCh, _, err := s.index.replicator.CollectShardDifferences(s.hashBeaterCtx, s.name, s.hashtree)
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
			if s.hashBeaterCtx.Err() != nil {
				return stats, s.hashBeaterCtx.Err()
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
				s.hashBeaterCtx,
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

func (s *Shard) stopHashBeater() {
	s.hashBeaterCancelFunc()
}
