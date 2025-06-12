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

package replica

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

var (
	// ErrConflictFindDeleted object exists on one replica but is deleted on the other.
	//
	// It depends on the order of operations
	// Created -> Deleted    => It is safe in this case to propagate deletion to all replicas
	// Created -> Deleted -> Created => propagating deletion will result in data lost

	ErrConflictExistOrDeleted = errors.New("conflict: object has been deleted on another replica")

	// ErrConflictObjectChanged object changed since last time and cannot be repaired
	ErrConflictObjectChanged = errors.New("source object changed during repair")
)

// repairer tries to detect inconsistencies and repair objects when reading them from replicas
type repairer struct {
	class               string
	getDeletionStrategy func() string
	client              FinderClient // needed to commit and abort operation
	logger              logrus.FieldLogger
}

// repairOne repairs a single object (used by Finder::GetOne)
func (r *repairer) repairOne(ctx context.Context,
	shard string,
	id strfmt.UUID,
	votes []ObjTuple,
	contentIdx int,
) (_ *storobj.Object, err error) {
	var (
		deleted          bool
		deletionTime     int64
		lastUTime        int64
		winnerIdx        int
		cl               = r.client
		deletionStrategy = r.getDeletionStrategy()
	)

	for i, x := range votes {
		if x.O.Deleted {
			deleted = true

			if x.UTime > deletionTime {
				deletionTime = x.UTime
			}
		}
		if x.UTime > lastUTime {
			lastUTime = x.UTime
			winnerIdx = i
		}
	}

	if deleted && deletionStrategy == models.ReplicationConfigDeletionStrategyDeleteOnConflict {
		gr := enterrors.NewErrorGroupWrapper(r.logger)
		for _, vote := range votes {
			if vote.O.Deleted && vote.UTime == deletionTime {
				continue
			}

			vote := vote

			gr.Go(func() error {
				ups := []*objects.VObject{{
					ID:                      id,
					Deleted:                 true,
					LastUpdateTimeUnixMilli: deletionTime,
					StaleUpdateTime:         vote.UTime,
				}}
				resp, err := cl.Overwrite(ctx, vote.Sender, r.class, shard, ups)
				if err != nil {
					return fmt.Errorf("node %q could not repair deleted object: %w", vote.Sender, err)
				}
				if len(resp) > 0 && resp[0].Err != "" {
					return fmt.Errorf("overwrite deleted object %w %s: %s", ErrConflictObjectChanged, vote.Sender, resp[0].Err)
				}
				return nil
			})
		}

		return nil, gr.Wait()
	}

	if deleted && deletionStrategy != models.ReplicationConfigDeletionStrategyTimeBasedResolution {
		return nil, ErrConflictExistOrDeleted
	}

	// fetch most recent object
	updates := votes[contentIdx].O
	winner := votes[winnerIdx]

	if updates.UpdateTime() != lastUTime {
		updates, err = cl.FullRead(ctx, winner.Sender, r.class, shard, id,
			search.SelectProperties{}, additional.Properties{}, 9)
		if err != nil {
			return nil, fmt.Errorf("get most recent object from %s: %w", winner.Sender, err)
		}
		if updates.UpdateTime() != lastUTime {
			return nil, fmt.Errorf("fetch new state from %s: %w, %w", winner.Sender, ErrConflictObjectChanged, err)
		}
	}

	gr := enterrors.NewErrorGroupWrapper(r.logger)
	for _, vote := range votes { // repair
		if vote.UTime == lastUTime {
			continue
		}

		vote := vote

		gr.Go(func() error {
			var latestObject *models.Object
			var vector []float32
			var vectors map[string][]float32
			var multiVectors map[string][][]float32

			if !updates.Deleted {
				latestObject = &updates.Object.Object
				vector = updates.Object.Vector
				if updates.Object.Vectors != nil {
					vectors = make(map[string][]float32, len(updates.Object.Vectors))
					for targetVector, v := range updates.Object.Vectors {
						vectors[targetVector] = v
					}
				}
				if updates.Object.MultiVectors != nil {
					multiVectors = make(map[string][][]float32, len(updates.Object.MultiVectors))
					for targetVector, v := range updates.Object.MultiVectors {
						multiVectors[targetVector] = v
					}
				}
			}

			ups := []*objects.VObject{{
				ID:                      updates.ID,
				Deleted:                 updates.Deleted,
				LastUpdateTimeUnixMilli: updates.UpdateTime(),
				LatestObject:            latestObject,
				Vector:                  vector,
				Vectors:                 vectors,
				MultiVectors:            multiVectors,
				StaleUpdateTime:         vote.UTime,
			}}
			resp, err := cl.Overwrite(ctx, vote.Sender, r.class, shard, ups)
			if err != nil {
				return fmt.Errorf("node %q could not repair object: %w", vote.Sender, err)
			}
			if len(resp) > 0 && resp[0].Err != "" {
				return fmt.Errorf("overwrite %w %s: %s", ErrConflictObjectChanged, vote.Sender, resp[0].Err)
			}
			return nil
		})
	}

	return updates.Object, gr.Wait()
}

// iTuple tuple of indices used to identify a unique object
type iTuple struct {
	S       int   // sender's index
	O       int   // object's index
	T       int64 // last update time
	Deleted bool
}

// repairExist repairs a single object when checking for existence
func (r *repairer) repairExist(ctx context.Context,
	shard string,
	id strfmt.UUID,
	votes []BoolTuple,
) (_ bool, err error) {
	var (
		deleted          bool
		deletionTime     int64
		lastUTime        int64
		winnerIdx        int
		cl               = r.client
		deletionStrategy = r.getDeletionStrategy()
	)

	for i, x := range votes {
		if x.O.Deleted {
			deleted = true

			if x.UTime > deletionTime {
				deletionTime = x.UTime
			}
		}
		if x.UTime > lastUTime {
			lastUTime = x.UTime
			winnerIdx = i
		}
	}

	if deleted && deletionStrategy == models.ReplicationConfigDeletionStrategyDeleteOnConflict {
		gr := enterrors.NewErrorGroupWrapper(r.logger)

		for _, vote := range votes {
			if vote.O.Deleted && vote.UTime == deletionTime {
				continue
			}

			vote := vote

			gr.Go(func() error {
				ups := []*objects.VObject{{
					ID:                      id,
					Deleted:                 true,
					LastUpdateTimeUnixMilli: deletionTime,
					StaleUpdateTime:         vote.UTime,
				}}
				resp, err := cl.Overwrite(ctx, vote.Sender, r.class, shard, ups)
				if err != nil {
					return fmt.Errorf("node %q could not repair deleted object: %w", vote.Sender, err)
				}
				if len(resp) > 0 && resp[0].Err != "" {
					return fmt.Errorf("overwrite deleted object %w %s: %s", ErrConflictObjectChanged, vote.Sender, resp[0].Err)
				}
				return nil
			})
		}

		return false, gr.Wait()
	}

	if deleted && deletionStrategy != models.ReplicationConfigDeletionStrategyTimeBasedResolution {
		return false, ErrConflictExistOrDeleted
	}

	// fetch most recent object
	winner := votes[winnerIdx]
	resp, err := cl.FullRead(ctx, winner.Sender, r.class, shard, id, search.SelectProperties{}, additional.Properties{}, 9)
	if err != nil {
		return false, fmt.Errorf("get most recent object from %s: %w", winner.Sender, err)
	}
	if resp.UpdateTime() != lastUTime {
		return false, fmt.Errorf("fetch new state from %s: %w, %w", winner.Sender, ErrConflictObjectChanged, err)
	}

	gr, ctx := enterrors.NewErrorGroupWithContextWrapper(r.logger, ctx)

	for _, vote := range votes { // repair
		if vote.UTime == lastUTime {
			continue
		}

		vote := vote

		gr.Go(func() error {
			var latestObject *models.Object
			var vector []float32
			var vectors map[string][]float32
			var multiVectors map[string][][]float32

			if !resp.Deleted {
				latestObject = &resp.Object.Object
				vector = resp.Object.Vector
				if resp.Object.Vectors != nil {
					vectors = make(map[string][]float32, len(resp.Object.Vectors))
					for targetVector, v := range resp.Object.Vectors {
						vectors[targetVector] = v
					}
				}
				if resp.Object.MultiVectors != nil {
					multiVectors = make(map[string][][]float32, len(resp.Object.MultiVectors))
					for targetVector, v := range resp.Object.MultiVectors {
						multiVectors[targetVector] = v
					}
				}
			}

			ups := []*objects.VObject{{
				ID:                      resp.ID,
				Deleted:                 resp.Deleted,
				LastUpdateTimeUnixMilli: resp.UpdateTime(),
				LatestObject:            latestObject,
				Vector:                  vector,
				Vectors:                 vectors,
				MultiVectors:            multiVectors,
				StaleUpdateTime:         vote.UTime,
			}}

			resp, err := cl.Overwrite(ctx, vote.Sender, r.class, shard, ups)
			if err != nil {
				return fmt.Errorf("node %q could not repair object: %w", vote.Sender, err)
			}
			if len(resp) > 0 && resp[0].Err != "" {
				return fmt.Errorf("overwrite %w %s: %s", ErrConflictObjectChanged, vote.Sender, resp[0].Err)
			}

			return nil
		})
	}

	return !resp.Deleted, gr.Wait()
}

// repairAll repairs objects when reading them ((use in combination with Finder::GetAll)
func (r *repairer) repairBatchPart(ctx context.Context,
	shard string,
	ids []strfmt.UUID,
	votes []Vote,
	contentIdx int,
) ([]*storobj.Object, error) {
	var (
		result            = make([]*storobj.Object, len(ids)) // final result
		lastTimes         = make([]iTuple, len(ids))          // most recent times
		lastDeletionTimes = make([]int64, len(ids))           // most recent deletion times
		ms                = make([]iTuple, 0, len(ids))       // mismatches
		cl                = r.client
		nVotes            = len(votes)
		// The input objects cannot be used for repair because
		// their attributes might have been filtered out
		reFetchSet       = make(map[int]struct{})
		deletionStrategy = r.getDeletionStrategy()
	)

	// find most recent objects
	for i, x := range votes[contentIdx].FullData {
		lastTimes[i] = iTuple{S: contentIdx, O: i, T: x.UpdateTime(), Deleted: x.Deleted}
		if x.Deleted {
			lastDeletionTimes[i] = x.UpdateTime()
		}
		votes[contentIdx].Count[i] = nVotes // reuse Count[] to check consistency
	}

	for i, vote := range votes {
		if i != contentIdx {
			for j, x := range vote.DigestData {
				if curTime := lastTimes[j].T; x.UpdateTime > curTime {
					// input object is not up to date
					lastTimes[j] = iTuple{S: i, O: j, T: x.UpdateTime}
					reFetchSet[j] = struct{}{} // we need to fetch this object again
				}

				lastTimes[j].Deleted = lastTimes[j].Deleted || x.Deleted

				if x.Deleted && x.UpdateTime > lastDeletionTimes[j] {
					lastDeletionTimes[j] = x.UpdateTime
				}

				votes[i].Count[j] = nVotes
			}
		}
	}

	// find missing content (diff)
	for i, p := range votes[contentIdx].FullData {
		if lastTimes[i].Deleted && lastDeletionTimes[i] == lastTimes[i].T {
			continue
		}

		if _, ok := reFetchSet[i]; ok {
			ms = append(ms, lastTimes[i])
		} else {
			result[i] = p.Object
		}
	}

	if len(ms) > 0 { // fetch most recent objects
		// partition by hostname
		sort.SliceStable(ms, func(i, j int) bool { return ms[i].S < ms[j].S })
		partitions := make([]int, 0, len(votes))
		pre := ms[0].S
		for i, y := range ms {
			if y.S != pre {
				partitions = append(partitions, i)
				pre = y.S
			}
		}
		partitions = append(partitions, len(ms))

		// concurrent fetches
		gr, ctx := enterrors.NewErrorGroupWithContextWrapper(r.logger, ctx)
		start := 0
		for _, end := range partitions { // fetch diffs
			rid := ms[start].S
			receiver := votes[rid].Sender
			query := make([]strfmt.UUID, end-start)
			for j := 0; start < end; start++ {
				query[j] = ids[ms[start].O]
				j++
			}
			start := start
			gr.Go(func() error {
				resp, err := cl.FullReads(ctx, receiver, r.class, shard, query)
				for i, n := 0, len(query); i < n; i++ {
					idx := ms[start-n+i].O
					if err != nil || lastTimes[idx].T != resp[i].UpdateTime() {
						votes[rid].Count[idx]--
					} else {
						result[idx] = resp[i].Object
					}
				}
				return nil
			})

		}
		if err := gr.Wait(); err != nil {
			return nil, err
		}
	}

	// concurrent repairs
	gr, ctx := enterrors.NewErrorGroupWithContextWrapper(r.logger, ctx)

	for rid, vote := range votes {
		query := make([]*objects.VObject, 0, len(ids)/2)
		m := make(map[string]int, len(ids)/2) //

		for j, x := range lastTimes {
			if !x.Deleted && result[j] == nil {
				// latest object could not be fetched
				continue
			}

			if x.Deleted && deletionStrategy == models.ReplicationConfigDeletionStrategyDeleteOnConflict {
				alreadyDeleted := false

				if rid == contentIdx {
					alreadyDeleted = vote.BatchReply.FullData[j].Deleted
				} else {
					alreadyDeleted = vote.BatchReply.DigestData[j].Deleted
				}

				if alreadyDeleted && lastDeletionTimes[j] == vote.UpdateTimeAt(j) {
					continue
				}

				obj := objects.VObject{
					ID:                      ids[j],
					Deleted:                 true,
					LastUpdateTimeUnixMilli: lastDeletionTimes[j],
					StaleUpdateTime:         vote.UpdateTimeAt(j),
				}
				query = append(query, &obj)
				m[string(ids[j])] = j

				continue
			}

			if x.Deleted && deletionStrategy != models.ReplicationConfigDeletionStrategyTimeBasedResolution {
				// note: conflict is not resolved
				continue
			}

			cTime := vote.UpdateTimeAt(j)

			if x.T != cTime && vote.Count[j] == nVotes {
				var latestObject *models.Object
				var vector []float32
				var vectors map[string][]float32
				var multiVectors map[string][][]float32

				deleted := x.Deleted && lastDeletionTimes[j] == x.T

				if !deleted {
					latestObject = &result[j].Object
					vector = result[j].Vector
					if result[j].Vectors != nil {
						vectors = make(map[string][]float32, len(result[j].Vectors))
						for targetVector, v := range result[j].Vectors {
							vectors[targetVector] = v
						}
					}
					if result[j].MultiVectors != nil {
						multiVectors = make(map[string][][]float32, len(result[j].MultiVectors))
						for targetVector, v := range result[j].MultiVectors {
							multiVectors[targetVector] = v
						}
					}
				}

				obj := objects.VObject{
					ID:                      ids[j],
					Deleted:                 deleted,
					LastUpdateTimeUnixMilli: x.T,
					LatestObject:            latestObject,
					Vector:                  vector,
					Vectors:                 vectors,
					MultiVectors:            multiVectors,
					StaleUpdateTime:         cTime,
				}
				query = append(query, &obj)
				m[string(ids[j])] = j
			}
		}

		if len(query) == 0 {
			continue
		}

		receiver := vote.Sender
		rid := rid

		gr.Go(func() error {
			rs, err := cl.Overwrite(ctx, receiver, r.class, shard, query)
			if err != nil {
				for _, idx := range m {
					votes[rid].Count[idx]--
				}
				return nil
			}
			for _, r := range rs {
				if r.Err != "" {
					if idx, ok := m[r.ID]; ok {
						votes[rid].Count[idx]--
					}
				}
			}
			return nil
		})
	}

	return result, gr.Wait()
}
