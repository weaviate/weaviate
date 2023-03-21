//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"golang.org/x/sync/errgroup"
)

var (
	// errConflictFindDeleted object exists on one replica but is deleted on the other.
	//
	// It depends on the order of operations
	// Created -> Deleted    => It is safe in this case to propagate deletion to all replicas
	// Created -> Deleted -> Created => propagating deletion will result in data lost
	errConflictExistOrDeleted = errors.New("conflict: object has been deleted on another replica")

	// errConflictObjectChanged object changed since last time and cannot be repaired
	errConflictObjectChanged = errors.New("source object changed during repair")
)

// repairer tries to detect inconsistencies and repair objects when reading them from replicas
type repairer struct {
	class  string
	client finderClient // needed to commit and abort operation
}

// repairOne repairs a single object (used by Finder::GetOne)
func (r *repairer) repairOne(ctx context.Context,
	shard string,
	id strfmt.UUID,
	votes []objTuple, st rState,
	contentIdx int,
) (_ *storobj.Object, err error) {
	var (
		lastUTime int64
		winnerIdx int
		cl        = r.client
	)
	for i, x := range votes {
		if x.o.Deleted {
			return nil, errConflictExistOrDeleted
		}
		if x.UTime > lastUTime {
			lastUTime = x.UTime
			winnerIdx = i
		}
	}
	// fetch most recent object
	updates := votes[contentIdx].o
	winner := votes[winnerIdx]
	if updates.UpdateTime() != lastUTime {
		updates, err = cl.FullRead(ctx, winner.sender, r.class, shard, id,
			search.SelectProperties{}, additional.Properties{})
		if err != nil {
			return nil, fmt.Errorf("get most recent object from %s: %w", winner.sender, err)
		}
		if updates.UpdateTime() != lastUTime {
			return nil, fmt.Errorf("fetch new state from %s: %w, %v", winner.sender, errConflictObjectChanged, err)
		}
	}

	var gr errgroup.Group
	for _, vote := range votes { // repair
		if vote.UTime == lastUTime {
			continue
		}
		vote := vote
		gr.Go(func() error {
			ups := []*objects.VObject{{
				LatestObject:    &updates.Object.Object,
				StaleUpdateTime: vote.UTime,
			}}
			resp, err := cl.Overwrite(ctx, vote.sender, r.class, shard, ups)
			if err != nil {
				return fmt.Errorf("node %q could not repair object: %w", vote.sender, err)
			}
			if len(resp) > 0 && resp[0].Err != "" {
				return fmt.Errorf("overwrite %w %s: %s", errConflictObjectChanged, vote.sender, resp[0].Err)
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
	votes []boolTuple,
	st rState,
) (_ bool, err error) {
	var (
		lastUTime int64
		winnerIdx int
		cl        = r.client
	)
	for i, x := range votes {
		if x.o.Deleted {
			return false, errConflictExistOrDeleted
		}
		if x.UTime > lastUTime {
			lastUTime = x.UTime
			winnerIdx = i
		}
	}
	// fetch most recent object
	winner := votes[winnerIdx]
	resp, err := cl.FullRead(ctx, winner.sender, r.class, shard, id, search.SelectProperties{}, additional.Properties{})
	if err != nil {
		return false, fmt.Errorf("get most recent object from %s: %w", winner.sender, err)
	}
	if resp.UpdateTime() != lastUTime {
		return false, fmt.Errorf("fetch new state from %s: %w, %v", winner.sender, errConflictObjectChanged, err)
	}
	gr, ctx := errgroup.WithContext(ctx)
	for _, vote := range votes { // repair
		if vote.UTime == lastUTime {
			continue
		}
		vote := vote
		gr.Go(func() error {
			ups := []*objects.VObject{{
				LatestObject:    &resp.Object.Object,
				StaleUpdateTime: vote.UTime,
			}}
			resp, err := cl.Overwrite(ctx, vote.sender, r.class, shard, ups)
			if err != nil {
				return fmt.Errorf("node %q could not repair object: %w", vote.sender, err)
			}
			if len(resp) > 0 && resp[0].Err != "" {
				return fmt.Errorf("overwrite %w %s: %s", errConflictObjectChanged, vote.sender, resp[0].Err)
			}
			return nil
		})
	}
	return !resp.Deleted, gr.Wait()
}

// repairAll repairs objects when reading them ((use in combination with Finder::GetAll)
func (r *repairer) repairAll(ctx context.Context,
	shard string,
	ids []strfmt.UUID,
	votes []vote,
	st rState,
	contentIdx int,
) ([]*storobj.Object, error) {
	var (
		result     = make([]*storobj.Object, len(ids)) // final result
		lastTimes  = make([]iTuple, len(ids))          // most recent times
		ms         = make([]iTuple, 0, len(ids))       // mismatches
		nDeletions = 0
		cl         = r.client
	)
	// find most recent objects
	for i, x := range votes[contentIdx].FullData {
		lastTimes[i] = iTuple{S: contentIdx, O: i, T: x.UpdateTime(), Deleted: x.Deleted}
	}
	for i, vote := range votes {
		if i != contentIdx {
			for j, x := range vote.DigestData {
				deleted := lastTimes[j].Deleted || x.Deleted
				if x.UpdateTime > lastTimes[j].T {
					lastTimes[j] = iTuple{S: i, O: j, T: x.UpdateTime}
				}
				lastTimes[j].Deleted = deleted
			}
		}
	}
	// find missing content (diff)
	for i, p := range votes[contentIdx].FullData {
		if lastTimes[i].Deleted { // conflict
			nDeletions++
			result[i] = nil
		} else if contentIdx != lastTimes[i].S {
			ms = append(ms, lastTimes[i])
		} else {
			result[i] = p.Object
		}
	}
	if len(ms) > 0 { // fetch most recent objects
		// partition by hostname
		sort.SliceStable(ms, func(i, j int) bool { return ms[i].S < ms[j].S })
		partitions := make([]int, 0, len(votes)-2)
		pre := ms[0].S
		for i, y := range ms {
			if y.S != pre {
				partitions = append(partitions, i)
				pre = y.S
			}
		}
		partitions = append(partitions, len(ms))

		// concurrent fetches
		gr, ctx := errgroup.WithContext(ctx)
		start := 0
		for _, end := range partitions { // fetch diffs
			receiver := votes[ms[start].S].Sender
			query := make([]strfmt.UUID, end-start)
			for j := 0; start < end; start++ {
				query[j] = ids[ms[start].O]
				j++
			}
			start := start
			gr.Go(func() error {
				resp, err := cl.FullReads(ctx, receiver, r.class, shard, query)
				if err != nil {
					return err
				}
				for i, n := 0, len(query); i < n; i++ {
					idx := ms[start-n+i].O
					if lastTimes[idx].T != resp[i].UpdateTime() {
						return fmt.Errorf("object %s changed on %s", ids[idx], receiver)
					}
					result[idx] = resp[i].Object
				}
				return nil
			})
			if err := gr.Wait(); err != nil {
				return nil, err
			}
		}
	}

	// concurrent repairs
	gr, ctx := errgroup.WithContext(ctx)
	for _, vote := range votes {
		query := make([]*objects.VObject, 0, len(ids)/2)
		for j, x := range lastTimes {
			if cTime := vote.UpdateTimeAt(j); x.T != cTime && !x.Deleted {
				query = append(query, &objects.VObject{LatestObject: &result[j].Object, StaleUpdateTime: cTime})
			}
		}
		if len(query) == 0 {
			continue
		}
		receiver := vote.Sender
		gr.Go(func() error {
			rs, err := cl.Overwrite(ctx, receiver, r.class, shard, query)
			if err != nil {
				return fmt.Errorf("node %q could not repair objects: %w", receiver, err)
			}
			for _, r := range rs {
				if r.Err != "" {
					return fmt.Errorf("object changed in the meantime on node %s: %s", receiver, r.Err)
				}
			}
			return nil
		})
	}
	err := gr.Wait()
	if nDeletions > 0 {
		return result, errConflictExistOrDeleted
	}

	return result, err
}
