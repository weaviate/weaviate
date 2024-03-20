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
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

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

		t := time.NewTicker(50 * time.Millisecond)

		backoffs := []time.Duration{
			1 * time.Second,
			5 * time.Second,
			10 * time.Second,
			30 * time.Second,
			1 * time.Minute,
		}

		backoffTimer := interval.NewBackoffTimer(backoffs...)

		firstFailure := true

		defer t.Stop()

		for {
			select {
			case <-s.hashBeaterCtx.Done():
				return
			case <-t.C:
				s.objectPropagationNeededCond.L.Lock()
				for !s.objectPropagationNeeded {
					s.objectPropagationNeededCond.Wait()
				}
				s.objectPropagationNeeded = false
				s.objectPropagationNeededCond.L.Unlock()

				stats, err := s.hashBeat()
				if s.hashBeaterCtx.Err() != nil {
					return
				}
				if err != nil {
					s.index.logger.
						WithField("action", "async_replication").
						WithField("class_name", s.class.Class).
						WithField("shard_name", s.name).
						Warnf("iteration failed: %v", err)

					if firstFailure {
						backoffTimer.Reset()
						firstFailure = false
					}

					time.Sleep(backoffTimer.CurrentInterval())
					backoffTimer.IncreaseInterval()

					s.objectPropagationRequired()

					continue
				}

				firstFailure = false

				hosts := make([]string, len(stats.hostStats))
				localObjects := 0
				remoteObjects := 0
				objectsPropagated := 0
				var objectProgationTook time.Duration
				var propagationErr error

				for i, stat := range stats.hostStats {
					hosts[i] = stat.host
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
					WithField("hosts", hosts).
					WithField("diffCalculationTook", stats.diffCalculationTook.String()).
					WithField("localObjects", localObjects).
					WithField("remoteObjects", remoteObjects).
					WithField("objectsPropagated", objectsPropagated).
					WithField("objectProgationTook", objectProgationTook.String())

				if propagationErr == nil {
					if backoffTimer.IntervalElapsed() {
						logEntry.Info("iteration successfully completed")
					}

					if objectsPropagated == 0 {
						backoffTimer.IncreaseInterval()
					} else {
						backoffTimer.Reset()
						s.objectPropagationRequired()
					}
				} else {
					logEntry.Warnf("propagation error: %v", propagationErr)

					time.Sleep(backoffTimer.CurrentInterval())
					backoffTimer.IncreaseInterval()

					s.objectPropagationRequired()
				}
			}
		}
	}, s.index.logger)

	enterrors.GoWrapper(func() {
		t := time.NewTicker(100 * time.Millisecond)

		for {
			select {
			case <-s.hashBeaterCtx.Done():
				return
			case <-t.C:
				comparedHosts := s.getLastComparedHosts()
				aliveHosts := s.allAliveHostnames()

				slices.Sort(comparedHosts)
				slices.Sort(aliveHosts)

				if !slices.Equal(comparedHosts, aliveHosts) {
					s.objectPropagationRequired()
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

func (s *Shard) objectPropagationRequired() {
	s.objectPropagationNeededCond.L.Lock()
	s.objectPropagationNeeded = true
	s.objectPropagationNeededCond.Signal()
	s.objectPropagationNeededCond.L.Unlock()
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

	diffCalculationStart := time.Now()

	replyCh, hosts, err := s.index.replicator.CollectShardDifferences(s.hashBeaterCtx, s.name, s.hashtree)
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
			)
			if err != nil {
				propagationErr = fmt.Errorf("propagating local objects: %v", err)
				break
			}

			localObjects += localObjs
			remoteObjects += remoteObjs
			objectsPropagated += propagations
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

	s.setLastComparedNodes(hosts)

	return stats, diffCollectionErr
}

func (s *Shard) stepsTowardsShardConsistency(ctx context.Context,
	shardName string, host string, initialToken, finalToken uint64,
) (localObjects, remoteObjects, propagations int, err error) {
	const limit = 100

	for localLastReadToken := initialToken; localLastReadToken < finalToken; {
		localDigests, newLocalLastReadToken, err := s.index.digestObjectsInTokenRange(ctx, shardName, localLastReadToken, finalToken, limit)
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
				shardName, host, remoteLastTokenRead, newLocalLastReadToken, limit)
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

		replicaObjs, err := s.index.fetchObjects(ctx, shardName, uuids)
		if err != nil {
			return localObjects, remoteObjects, propagations, fmt.Errorf("fetching local objects: %w", err)
		}

		mergeObjs := make([]*objects.VObject, len(replicaObjs))

		for i, replicaObj := range replicaObjs {
			obj := &objects.VObject{
				LatestObject:    &replicaObj.Object.Object,
				Vector:          replicaObj.Object.Vector,
				StaleUpdateTime: remoteStaleUpdateTime[replicaObj.ID.String()],
			}
			mergeObjs[i] = obj
		}

		_, err = s.index.replicator.Overwrite(ctx, host, s.class.Class, shardName, mergeObjs)
		if err != nil {
			return localObjects, remoteObjects, propagations, fmt.Errorf("propagating local objects: %w", err)
		}

		propagations += len(mergeObjs)
		localLastReadToken = newLocalLastReadToken
	}

	// Note: propagations == 0 means local shard is laying behind remote shard,
	// the local shard may receive recent objects when remote shard propagates them

	return localObjects, remoteObjects, propagations, nil
}

func (s *Shard) stopHashBeater() {
	s.hashBeaterCancelFunc()
}
