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
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

var (
	// ErrConsistencyLevel consistency level cannot be achieved
	ErrConsistencyLevel = errors.New("cannot achieve consistency level")
	// errConflictFindDeleted object exists on one replica but is deleted on the other.
	//
	// It depends on the order of operations
	// Created -> Deleted    => It is safe in this case to propagate deletion to all replicas
	// Created -> Deleted -> Created => propagating deletion will result in data lost
	errConflictExistOrDeleted = errors.New("conflict: object has been deleted on another replica")
)

type (
	// senderReply represent the data received from a sender
	senderReply[T any] struct {
		sender     string // hostname of the sender
		Version    int64  // sender's current version of the object
		Data       T      // the data sent by the sender
		UpdateTime int64  // sender's current update time
		DigestRead bool
	}
	findOneReply senderReply[objects.Replica]
	existReply   senderReply[bool]
)

// Finder finds replicated objects
type Finder struct {
	RClient            // needed to commit and abort operation
	resolver *resolver // host names of replicas
	class    string
	// TODO LOGGER
	// Don't leak host nodes to end user but log it
}

// NewFinder constructs a new finder instance
func NewFinder(className string,
	stateGetter shardingState, nodeResolver nodeResolver,
	client RClient,
) *Finder {
	return &Finder{
		class: className,
		resolver: &resolver{
			schema:       stateGetter,
			nodeResolver: nodeResolver,
			class:        className,
		},
		RClient: client,
	}
}

// GetOne gets object which satisfies the giving consistency
func (f *Finder) GetOne(ctx context.Context,
	l ConsistencyLevel, shard string,
	id strfmt.UUID, props search.SelectProperties, adds additional.Properties,
) (*storobj.Object, error) {
	c := newReadCoordinator[findOneReply](f, shard)
	op := func(ctx context.Context, host string, fullRead bool) (findOneReply, error) {
		if fullRead {
			r, err := f.FetchObject(ctx, host, f.class, shard, id, props, adds)
			return findOneReply{host, 0, r, r.UpdateTime(), false}, err
		} else {
			xs, err := f.DigestObjects(ctx, host, f.class, shard, []strfmt.UUID{id})
			var x RepairResponse
			if len(xs) == 1 {
				x = xs[0]
			}
			if err == nil && len(xs) != 1 {
				err = fmt.Errorf("digest read request: empty result")
			}
			r := objects.Replica{ID: id, Deleted: x.Deleted}
			return findOneReply{host, x.Version, r, x.UpdateTime, true}, err
		}
	}
	replyCh, state, err := c.Fetch2(ctx, l, op)
	if err != nil {
		return nil, err
	}
	result := <-f.readOne(ctx, shard, id, replyCh, state)
	return result.data, result.err
}

// Exists checks if an object exists which satisfies the giving consistency
// TODO: implement using new approach
func (f *Finder) Exists(ctx context.Context, l ConsistencyLevel, shard string, id strfmt.UUID) (bool, error) {
	c := newReadCoordinator[existReply](f, shard)
	op := func(ctx context.Context, host string) (existReply, error) {
		obj, err := f.RClient.Exists(ctx, host, f.class, shard, id)
		return existReply{host, -1, obj, 0, false}, err
	}
	replyCh, state, err := c.Fetch(ctx, l, op)
	if err != nil {
		return false, err
	}
	return readOneExists(replyCh, state)
}

// NodeObject gets object from a specific node.
// it is used mainly for debugging purposes
func (f *Finder) NodeObject(ctx context.Context,
	nodeName, shard string, id strfmt.UUID,
	props search.SelectProperties, adds additional.Properties,
) (*storobj.Object, error) {
	host, ok := f.resolver.NodeHostname(nodeName)
	if !ok || host == "" {
		return nil, fmt.Errorf("cannot resolve node name: %s", nodeName)
	}
	r, err := f.RClient.FetchObject(ctx, host, f.class, shard, id, props, adds)
	return r.Object, err
}

func (f *Finder) readOne(ctx context.Context,
	shard string,
	id strfmt.UUID,
	ch <-chan simpleResult[findOneReply],
	st rState,
) <-chan result[*storobj.Object] {
	// counters tracks the number of votes for each participant
	resultCh := make(chan result[*storobj.Object], 1)
	go func() {
		defer close(resultCh)
		var (
			votes      = make([]objTuple, 0, len(st.Hosts))
			maxCount   = 0
			contentIdx = -1
		)

		for r := range ch { // len(ch) == st.Level
			resp := r.Response
			if r.Err != nil { // a least one node is not responding
				resultCh <- result[*storobj.Object]{nil, fmt.Errorf("source %s: %w", resp.sender, r.Err)}
				return
			}
			if !resp.DigestRead {
				contentIdx = len(votes)
			}
			votes = append(votes, objTuple{resp.sender, resp.UpdateTime, resp.Data, 0, nil})
			for i := range votes { // count number of votes
				if votes[i].UTime == resp.UpdateTime {
					votes[i].ack++
				}
				if maxCount < votes[i].ack {
					maxCount = votes[i].ack
				}
				if maxCount >= st.Level && contentIdx >= 0 {
					resultCh <- result[*storobj.Object]{votes[contentIdx].o.Object, nil}
					return
				}
			}
		}
		// Just in case this however should not be possible
		if n := len(votes); n == 0 || contentIdx < 0 {
			resultCh <- result[*storobj.Object]{nil, fmt.Errorf("internal error: #responses %d index %d", n, contentIdx)}
			return
		}

		obj, err := f.repairOne(ctx, shard, id, votes, st, contentIdx)
		if err == nil {
			resultCh <- result[*storobj.Object]{obj, nil}
			return
		}
		// TODO: log message
		var sb strings.Builder
		for i, c := range votes {
			if i != 0 {
				sb.WriteByte(' ')
			}
			fmt.Fprintf(&sb, "%s:%d", c.sender, c.UTime)
		}
		resultCh <- result[*storobj.Object]{nil, fmt.Errorf("%w %q: %q %v", ErrConsistencyLevel, st.CLevel, sb.String(), err)}
	}()
	return resultCh
}

// repair one object on several nodes using last write wins strategy
func (f *Finder) repairOne(ctx context.Context, shard string, id strfmt.UUID, votes []objTuple, st rState, contentIdx int) (_ *storobj.Object, err error) {
	var (
		lastUTime int64
		winnerIdx int
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
		updates, err = f.RClient.FetchObject(ctx, winner.sender, f.class, shard, id,
			search.SelectProperties{}, additional.Properties{})
		if err != nil {
			return nil, fmt.Errorf("get most recent object from %s: %w", votes[winnerIdx].sender, err)
		}
	}

	for _, c := range votes { // repair
		if c.UTime != lastUTime {
			updates := []*objects.VObject{{
				LatestObject:    &updates.Object.Object,
				StaleUpdateTime: c.UTime,
				Version:         0, // todo set when implemented
			}}
			resp, err := f.RClient.OverwriteObjects(ctx, c.sender, f.class, shard, updates)
			if err != nil {
				return nil, fmt.Errorf("node %q could not repair object: %w", c.sender, err)
			}
			if len(resp) > 0 && resp[0].Err != "" && resp[0].UpdateTime != lastUTime {
				return nil, fmt.Errorf("object changed in the meantime on node %s: %s", c.sender, resp[0].Err)
			}
		}
	}
	return updates.Object, nil
}

// batchReply represents the data returned by sender
// The returned data may result from a direct or digest read request
type batchReply struct {
	// Sender hostname of the sender
	Sender string
	// DigestRead is this reply from digest read?
	DigestRead bool
	// DirectData returned from a direct read request
	DirectData []objects.Replica
	// DigestData returned from a digest read request
	DigestData []RepairResponse
}

// GetAll gets all objects which satisfy the giving consistency
func (f *Finder) GetAll(ctx context.Context, l ConsistencyLevel, shard string,
	ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	c := newReadCoordinator[batchReply](f, shard)
	n := len(ids)
	op := func(ctx context.Context, host string, fullRead bool) (batchReply, error) {
		if fullRead {
			xs, err := f.RClient.FetchObjects(ctx, host, f.class, shard, ids)
			if m := len(xs); err == nil && n != m {
				err = fmt.Errorf("direct read expected %d got %d items", n, m)
			}
			return batchReply{Sender: host, DigestRead: false, DirectData: xs}, err
		} else {
			xs, err := f.DigestObjects(ctx, host, f.class, shard, ids)
			if m := len(xs); err == nil && n != m {
				err = fmt.Errorf("direct read expected %d got %d items", n, m)
			}
			return batchReply{Sender: host, DigestRead: true, DigestData: xs}, err
		}
	}
	replyCh, state, err := c.Fetch2(ctx, l, op)
	if err != nil {
		return nil, err
	}
	result := <-f.readAll(ctx, shard, ids, replyCh, state)
	if err = result.err; err != nil {
		err = fmt.Errorf("%w %q: %v", ErrConsistencyLevel, state.CLevel, err)
	}

	return result.data, err
}

type vote struct {
	batchReply
	Count []int
	Err   error
}

func (r batchReply) UpdateTimeAt(idx int) int64 {
	if len(r.DigestData) != 0 {
		return r.DigestData[idx].UpdateTime
	}
	return r.DirectData[idx].UpdateTime()
}

type _Results result[[]*storobj.Object]

func (f *Finder) readAll(ctx context.Context, shard string, ids []strfmt.UUID, ch <-chan simpleResult[batchReply], st rState) <-chan _Results {
	resultCh := make(chan _Results, 1)

	go func() {
		defer close(resultCh)
		var (
			N = len(ids) // number of requested objects
			// votes counts number of votes per object for each node
			votes      = make([]vote, 0, len(st.Hosts))
			contentIdx = -1 // index of direct read reply
		)

		for r := range ch { // len(ch) == st.Level
			resp := r.Response
			if r.Err != nil { // a least one node is not responding
				resultCh <- _Results{nil, fmt.Errorf("source %s: %w", r.Response.Sender, r.Err)}
				return
			}
			if !resp.DigestRead {
				contentIdx = len(votes)
			}

			votes = append(votes, vote{resp, make([]int, N), nil})
			M := 0
			for i := 0; i < N; i++ {
				max := 0
				lastTime := resp.UpdateTimeAt(i)

				for j := range votes { // count votes
					if votes[j].UpdateTimeAt(i) == lastTime {
						votes[j].Count[i]++
					}
					if max < votes[j].Count[i] {
						max = votes[j].Count[i]
					}
				}
				if max >= st.Level {
					M++
				}
			}

			if M == N {
				resultCh <- _Results{fromReplicas(votes[contentIdx].DirectData), nil}
				return
			}
		}
		res, err := f.repairAll(ctx, shard, ids, votes, st, contentIdx)
		resultCh <- _Results{res, err}
	}()

	return resultCh
}

func (f *Finder) repairAll(ctx context.Context,
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
	)
	// find most recent objects
	for i, x := range votes[contentIdx].DirectData {
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
	for i, p := range votes[contentIdx].DirectData {
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
		// TODO parallel fetch
		start := 0
		for _, end := range partitions { // fetch diffs
			receiver := votes[ms[start].S].Sender
			query := make([]strfmt.UUID, end-start)
			for j := 0; start < end; start++ {
				query[j] = ids[ms[start].O]
				j++
			}
			resp, err := f.RClient.FetchObjects(ctx, receiver, f.class, shard, query)
			if err != nil {
				return nil, err
			}
			n := len(query)
			if m := len(resp); n != m {
				return nil, fmt.Errorf("try to fetch %d objects from %s but got %d", m, receiver, n)
			}
			for i := 0; i < n; i++ {
				idx := ms[start-n+i].O
				if lastTimes[idx].T != resp[i].UpdateTime() {
					return nil, fmt.Errorf("object %s changed on %s", ids[idx], receiver)
				}
				result[idx] = resp[i].Object
			}
		}
	}

	// repair
	// TODO parallel overwrite
	for _, vote := range votes {
		receiver := vote.Sender
		query := make([]*objects.VObject, 0, len(ids)/2)
		for j, x := range lastTimes {
			if cTime := vote.UpdateTimeAt(j); x.T != cTime && !x.Deleted {
				query = append(query, &objects.VObject{LatestObject: &result[j].Object, StaleUpdateTime: cTime})
			}
		}
		if len(query) == 0 {
			continue
		}
		rs, err := f.RClient.OverwriteObjects(ctx, receiver, f.class, shard, query)
		if err != nil {
			return nil, fmt.Errorf("node %q could not repair objects: %w", receiver, err)
		}
		for _, r := range rs {
			if r.Err != "" {
				return nil, fmt.Errorf("object changed in the meantime on node %s: %s", receiver, r.Err)
			}
		}
	}
	if nDeletions > 0 {
		return result, errConflictExistOrDeleted
	}

	return result, nil
}

// iTuple tuple of indices used to identify a unique object
type iTuple struct {
	S       int   // sender's index
	O       int   // object's index
	T       int64 // last update time
	Deleted bool
}
