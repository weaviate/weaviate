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
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/cluster/router/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/storobj"
)

// pullSteam is used by the finder to pull objects from replicas
type finderStream struct {
	repairer
	log logrus.FieldLogger
}

type (
	// tuple is a container for the data received from a replica
	tuple[T any] struct {
		Sender string
		UTime  int64
		O      T
		ack    int
		err    error
	}

	ObjTuple  tuple[Replica]
	ObjResult = _Result[*storobj.Object]
)

// readOne reads one replicated object
func (f *finderStream) readOne(ctx context.Context,
	shard string,
	id strfmt.UUID,
	ch <-chan _Result[findOneReply],
	level int,
) <-chan ObjResult {
	// counters tracks the number of votes for each participant
	resultCh := make(chan ObjResult, 1)
	g := func() {
		defer close(resultCh)
		var (
			votes      = make([]ObjTuple, 0, level)
			contentIdx = -1
		)

		for r := range ch { // len(ch) == level
			resp := r.Value
			if r.Err != nil { // a least one node is not responding
				f.log.WithField("op", "get").WithField("replica", resp.sender).
					WithField("class", f.class).WithField("shard", shard).
					WithField("uuid", id).Error(r.Err)
				resultCh <- ObjResult{nil, ErrRead}
				return
			}
			if !resp.DigestRead {
				contentIdx = len(votes)
			}
			votes = append(votes, ObjTuple{resp.sender, resp.UpdateTime, resp.Data, 0, nil})

			for i := range votes {
				if votes[i].UTime != resp.UpdateTime {
					// incomming response does not match current Vote
					continue
				}

				votes[i].ack++

				if votes[i].ack < level {
					// current Vote does not have enough acks
					continue
				}

				if votes[i].O.Deleted {
					resultCh <- ObjResult{nil, nil}
					return
				}
				if i == contentIdx {
					// prefetched payload matches agreed Vote
					resultCh <- ObjResult{votes[contentIdx].O.Object, nil}
					return
				}
			}
		}

		obj, err := f.repairOne(ctx, shard, id, votes, contentIdx)
		if err == nil {
			resultCh <- ObjResult{obj, nil}
			return
		}

		resultCh <- ObjResult{nil, errors.Wrap(err, ErrRepair.Error())}
		var sb strings.Builder
		for i, c := range votes {
			if i != 0 {
				sb.WriteByte(' ')
			}
			fmt.Fprintf(&sb, "%s:%d", c.Sender, c.UTime)
		}
		f.log.WithField("op", "repair_one").WithField("class", f.class).
			WithField("shard", shard).WithField("uuid", id).
			WithField("msg", sb.String()).Error(err)
	}
	enterrors.GoWrapper(g, f.logger)
	return resultCh
}

type (
	batchResult _Result[[]*storobj.Object]

	// Vote represents objects received from a specific replica and the number of votes per object.
	Vote struct {
		BatchReply       // reply from a replica
		Count      []int // number of votes per object
		Err        error
	}
)

type BoolTuple tuple[types.RepairResponse]

// readExistence checks if replicated object exists
func (f *finderStream) readExistence(ctx context.Context,
	shard string,
	id strfmt.UUID,
	ch <-chan _Result[existReply],
	level int,
) <-chan _Result[bool] {
	resultCh := make(chan _Result[bool], 1)
	g := func() {
		defer close(resultCh)
		votes := make([]BoolTuple, 0, level) // number of votes per replica

		for r := range ch { // len(ch) == st.Level
			resp := r.Value
			if r.Err != nil { // at least one node is not responding
				f.log.WithField("op", "exists").WithField("replica", resp.Sender).
					WithField("class", f.class).WithField("shard", shard).
					WithField("uuid", id).Error(r.Err)
				resultCh <- _Result[bool]{false, ErrRead}
				return
			}

			votes = append(votes, BoolTuple{resp.Sender, resp.UpdateTime, resp.RepairResponse, 0, nil})

			for i := range votes { // count number of votes
				if votes[i].UTime != resp.UpdateTime {
					// incomming response does not match current Vote
					continue
				}

				votes[i].ack++

				if votes[i].ack < level {
					// current Vote does not have enough acks
					continue
				}

				exists := !votes[i].O.Deleted && votes[i].O.UpdateTime != 0
				resultCh <- _Result[bool]{exists, nil}
				return
			}
		}

		obj, err := f.repairExist(ctx, shard, id, votes)
		if err == nil {
			resultCh <- _Result[bool]{obj, nil}
			return
		}
		resultCh <- _Result[bool]{false, errors.Wrap(err, ErrRepair.Error())}

		var sb strings.Builder
		for i, c := range votes {
			if i != 0 {
				sb.WriteByte(' ')
			}
			fmt.Fprintf(&sb, "%s:%d", c.Sender, c.UTime)
		}
		f.log.WithField("op", "repair_exist").WithField("class", f.class).
			WithField("shard", shard).WithField("uuid", id).
			WithField("msg", sb.String()).Error(err)
	}
	enterrors.GoWrapper(g, f.logger)
	return resultCh
}

// readBatchPart reads in replicated objects specified by their ids
// It checks each object x for consistency and sets x.IsConsistent
func (f *finderStream) readBatchPart(ctx context.Context,
	batch ShardPart,
	ids []strfmt.UUID,
	ch <-chan _Result[BatchReply],
	level int,
) <-chan batchResult {
	resultCh := make(chan batchResult, 1)

	g := func() {
		defer close(resultCh)
		var (
			N = len(ids) // number of requested objects
			// votes counts number of votes per object for each node
			votes      = make([]Vote, 0, level)
			contentIdx = -1 // index of full read reply
		)

		for r := range ch { // len(ch) == level
			resp := r.Value
			if r.Err != nil { // at least one node is not responding
				f.log.WithField("op", "read_batch.get").WithField("replica", r.Value.Sender).
					WithField("class", f.class).WithField("shard", batch.Shard).Error(r.Err)
				resultCh <- batchResult{nil, ErrRead}
				return
			}
			if !resp.IsDigest {
				contentIdx = len(votes)
			}

			votes = append(votes, Vote{resp, make([]int, N), nil})
			M := 0
			for i := 0; i < N; i++ {
				max := 0
				maxAt := -1
				lastTime := resp.UpdateTimeAt(i)

				for j := range votes { // count votes
					if votes[j].UpdateTimeAt(i) == lastTime {
						votes[j].Count[i]++
					}
					if max < votes[j].Count[i] {
						max = votes[j].Count[i]
						maxAt = j
					}
				}
				if max >= level && maxAt == contentIdx {
					M++
				}
			}

			if M == N { // all objects are consistent
				for _, idx := range batch.Index {
					batch.Data[idx].IsConsistent = true
				}
				resultCh <- batchResult{fromReplicas(votes[contentIdx].FullData), nil}
				return
			}
		}
		res, err := f.repairBatchPart(ctx, batch.Shard, ids, votes, contentIdx)
		if err != nil {
			resultCh <- batchResult{nil, ErrRepair}
			f.log.WithField("op", "repair_batch").WithField("class", f.class).
				WithField("shard", batch.Shard).WithField("uuids", ids).Error(err)
			return
		}
		// count total number of votes
		maxCount := len(votes) * len(votes)
		sum := votes[0].Count
		for _, vote := range votes[1:] {
			for i, n := range vote.Count {
				sum[i] += n
			}
		}
		// set consistency flag
		for i, n := range sum {
			if n == maxCount { // if consistent
				x := res[i]

				if x == nil {
					// object was fetched but deleted during repair phase
					batch.Data[batch.Index[i]].IsConsistent = false
					continue
				}

				prev := batch.Data[batch.Index[i]]
				x.BelongsToShard = prev.BelongsToShard
				x.BelongsToNode = prev.BelongsToNode
				batch.Data[batch.Index[i]] = x
				x.IsConsistent = true
			}
		}

		resultCh <- batchResult{res, nil}
	}
	enterrors.GoWrapper(g, f.logger)

	return resultCh
}

// BatchReply is a container of the batch received from a replica
// The returned data may result from a full or digest read request
type BatchReply struct {
	// Sender hostname of the Sender
	Sender string
	// IsDigest is this reply from a digest read?
	IsDigest bool
	// FullData returned from a full read request
	FullData []Replica
	// DigestData returned from a digest read request
	DigestData []types.RepairResponse
}

// UpdateTimeAt gets update time from reply
func (r BatchReply) UpdateTimeAt(idx int) int64 {
	if len(r.DigestData) != 0 {
		return r.DigestData[idx].UpdateTime
	}
	return r.FullData[idx].UpdateTime()
}
