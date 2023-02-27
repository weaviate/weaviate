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
	"golang.org/x/sync/errgroup"
)

var (
	// msgCLevel consistency level cannot be achieved
	msgCLevel = "cannot achieve consistency level"
	// errConflictFindDeleted object exists on one replica but is deleted on the other.
	//
	// It depends on the order of operations
	// Created -> Deleted    => It is safe in this case to propagate deletion to all replicas
	// Created -> Deleted -> Created => propagating deletion will result in data lost
	errConflictExistOrDeleted = errors.New("conflict: object has been deleted on another replica")

	// errConflictObjectChanged object changed since last time and cannot be repaired
	errConflictObjectChanged = errors.New("source object changed during repair")
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
	existReply   struct {
		Sender string
		RepairResponse
	}
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
	replyCh, state, err := c.Pull(ctx, l, op)
	if err != nil {
		return nil, fmt.Errorf("%s %q: %w", msgCLevel, l, err)
	}
	result := <-f.readOne(ctx, shard, id, replyCh, state)
	if err = result.err; err != nil {
		err = fmt.Errorf("%s %q: %w", msgCLevel, l, err)
	}
	return result.data, err
}

// batchReply represents the data returned by sender
// The returned data may result from a direct or digest read request
type batchReply struct {
	// Sender hostname of the sender
	Sender string
	// IsDigest is this reply from a digest read?
	IsDigest bool
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
			return batchReply{Sender: host, IsDigest: false, DirectData: xs}, err
		} else {
			xs, err := f.DigestObjects(ctx, host, f.class, shard, ids)
			if m := len(xs); err == nil && n != m {
				err = fmt.Errorf("direct read expected %d got %d items", n, m)
			}
			return batchReply{Sender: host, IsDigest: true, DigestData: xs}, err
		}
	}
	replyCh, state, err := c.Pull(ctx, l, op)
	if err != nil {
		return nil, fmt.Errorf("%s %q: %w", msgCLevel, l, err)
	}
	result := <-f.readAll(ctx, shard, ids, replyCh, state)
	if err = result.err; err != nil {
		err = fmt.Errorf("%s %q: %w", msgCLevel, l, err)
	}

	return result.data, err
}

// Exists checks if an object exists which satisfies the giving consistency
func (f *Finder) Exists(ctx context.Context, l ConsistencyLevel, shard string, id strfmt.UUID) (bool, error) {
	c := newReadCoordinator[existReply](f, shard)
	op := func(ctx context.Context, host string, _ bool) (existReply, error) {
		xs, err := f.DigestObjects(ctx, host, f.class, shard, []strfmt.UUID{id})
		var x RepairResponse
		if len(xs) == 1 {
			x = xs[0]
		}
		if err == nil && len(xs) != 1 {
			err = fmt.Errorf("malformed digest read response: expected one result, got %d", len(xs))
		}
		return existReply{host, x}, err
	}
	replyCh, state, err := c.Pull(ctx, l, op)
	if err != nil {
		return false, fmt.Errorf("%s %q: %w", msgCLevel, l, err)
	}
	result := <-f.readExistence(ctx, shard, id, replyCh, state)
	if err = result.err; err != nil {
		err = fmt.Errorf("%s %q: %w", msgCLevel, l, err)
	}
	return result.data, err
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
		resultCh <- result[*storobj.Object]{nil, fmt.Errorf("%q: %w", sb.String(), err)}
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
			resp, err := f.RClient.OverwriteObjects(ctx, vote.sender, f.class, shard, ups)
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
			if !resp.IsDigest {
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
		if err != nil {
			res = nil
		}
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
				resp, err := f.RClient.FetchObjects(ctx, receiver, f.class, shard, query)
				if err != nil {
					return err
				}
				n := len(query)
				if m := len(resp); n != m {
					return fmt.Errorf("try to fetch %d objects from %s but got %d", m, receiver, n)
				}
				for i := 0; i < n; i++ {
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
			rs, err := f.RClient.OverwriteObjects(ctx, receiver, f.class, shard, query)
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

// iTuple tuple of indices used to identify a unique object
type iTuple struct {
	S       int   // sender's index
	O       int   // object's index
	T       int64 // last update time
	Deleted bool
}

func (f *Finder) readExistence(ctx context.Context,
	shard string,
	id strfmt.UUID,
	ch <-chan simpleResult[existReply],
	st rState,
) <-chan result[bool] {
	resultCh := make(chan result[bool], 1)
	go func() {
		defer close(resultCh)
		var (
			votes    = make([]boolTuple, 0, len(st.Hosts)) // number of votes per replica
			maxCount = 0
		)

		for r := range ch { // len(ch) == st.Level
			resp := r.Response
			if r.Err != nil { // a least one node is not responding
				resultCh <- result[bool]{false, fmt.Errorf("source %s: %w", resp.Sender, r.Err)}
				return
			}

			votes = append(votes, boolTuple{resp.Sender, resp.UpdateTime, resp.RepairResponse, 0, nil})
			for i := range votes { // count number of votes
				if votes[i].UTime == resp.UpdateTime {
					votes[i].ack++
				}
				if maxCount < votes[i].ack {
					maxCount = votes[i].ack
				}
				if maxCount >= st.Level {
					exists := !votes[i].o.Deleted && votes[i].o.UpdateTime != 0
					resultCh <- result[bool]{exists, nil}
					return
				}
			}
		}

		obj, err := f.repairExist(ctx, shard, id, votes, st)
		if err == nil {
			resultCh <- result[bool]{obj, nil}
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
		resultCh <- result[bool]{false, fmt.Errorf("%q: %w", sb.String(), err)}
	}()
	return resultCh
}

func (f *Finder) repairExist(ctx context.Context, shard string, id strfmt.UUID, votes []boolTuple, st rState) (_ bool, err error) {
	var (
		lastUTime int64
		winnerIdx int
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
	resp, err := f.RClient.FetchObject(ctx, winner.sender, f.class, shard, id, search.SelectProperties{}, additional.Properties{})
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
			resp, err := f.RClient.OverwriteObjects(ctx, vote.sender, f.class, shard, ups)
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
