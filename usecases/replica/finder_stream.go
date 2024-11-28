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
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

// pullSteam is used by the finder to pull objects from replicas
type finderStream struct {
	repairer
	log logrus.FieldLogger
}

type (
	// tuple is a container for the data received from a replica
	tuple[T any] struct {
		sender string
		UTime  int64
		o      T
		ack    int
		err    error
	}

	objTuple  tuple[objects.Replica]
	objResult = _Result[*storobj.Object]
)

// readOne reads one replicated object
func (f *finderStream) readOne(ctx context.Context,
	shard string,
	id strfmt.UUID,
	ch <-chan _Result[findOneReply],
	st rState,
) <-chan objResult {
	// counters tracks the number of votes for each participant
	resultCh := make(chan objResult, 1)
	g := func() {
		defer close(resultCh)
		var (
			votes      = make([]objTuple, 0, st.Level)
			contentIdx = -1
		)

		for r := range ch { // len(ch) == st.Level
			resp := r.Value
			if r.Err != nil { // a least one node is not responding
				f.log.WithField("op", "get").WithField("replica", resp.sender).
					WithField("class", f.class).WithField("shard", shard).
					WithField("uuid", id).Error(r.Err)
				resultCh <- objResult{nil, errRead}
				return
			}
			if !resp.DigestRead {
				contentIdx = len(votes)
			}
			votes = append(votes, objTuple{resp.sender, resp.UpdateTime, resp.Data, 0, nil})

			for i := range votes {
				if votes[i].UTime != resp.UpdateTime {
					// incomming response does not match current vote
					continue
				}

				votes[i].ack++

				if votes[i].ack < st.Level {
					// current vote does not have enough acks
					continue
				}

				if votes[i].o.Deleted {
					resultCh <- objResult{nil, nil}
					return
				}
				if i == contentIdx {
					// prefetched payload matches agreed vote
					resultCh <- objResult{votes[contentIdx].o.Object, nil}
					return
				}
			}
		}

		obj, err := f.repairOne(ctx, shard, id, votes, st, contentIdx)
		if err == nil {
			resultCh <- objResult{obj, nil}
			return
		}

		resultCh <- objResult{nil, errors.Wrap(err, errRepair.Error())}
		var sb strings.Builder
		for i, c := range votes {
			if i != 0 {
				sb.WriteByte(' ')
			}
			fmt.Fprintf(&sb, "%s:%d", c.sender, c.UTime)
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

	// vote represents objects received from a specific replica and the number of votes per object.
	vote struct {
		batchReply       // reply from a replica
		Count      []int // number of votes per object
		Err        error
	}
)

type boolTuple tuple[RepairResponse]

// readExistence checks if replicated object exists
func (f *finderStream) readExistence(ctx context.Context,
	shard string,
	id strfmt.UUID,
	ch <-chan _Result[existReply],
	st rState,
) <-chan _Result[bool] {
	resultCh := make(chan _Result[bool], 1)
	g := func() {
		defer close(resultCh)
		votes := make([]boolTuple, 0, st.Level) // number of votes per replica

		for r := range ch { // len(ch) == st.Level
			resp := r.Value
			if r.Err != nil { // at least one node is not responding
				f.log.WithField("op", "exists").WithField("replica", resp.Sender).
					WithField("class", f.class).WithField("shard", shard).
					WithField("uuid", id).Error(r.Err)
				resultCh <- _Result[bool]{false, errRead}
				return
			}

			votes = append(votes, boolTuple{resp.Sender, resp.UpdateTime, resp.RepairResponse, 0, nil})

			for i := range votes { // count number of votes
				if votes[i].UTime != resp.UpdateTime {
					// incomming response does not match current vote
					continue
				}

				votes[i].ack++

				if votes[i].ack < st.Level {
					// current vote does not have enough acks
					continue
				}

				exists := !votes[i].o.Deleted && votes[i].o.UpdateTime != 0
				resultCh <- _Result[bool]{exists, nil}
				return
			}
		}

		obj, err := f.repairExist(ctx, shard, id, votes, st)
		if err == nil {
			resultCh <- _Result[bool]{obj, nil}
			return
		}
		resultCh <- _Result[bool]{false, errors.Wrap(err, errRepair.Error())}

		var sb strings.Builder
		for i, c := range votes {
			if i != 0 {
				sb.WriteByte(' ')
			}
			fmt.Fprintf(&sb, "%s:%d", c.sender, c.UTime)
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
	batch shardPart,
	ids []strfmt.UUID,
	ch <-chan _Result[batchReply], st rState,
) <-chan batchResult {
	resultCh := make(chan batchResult, 1)

	g := func() {
		defer close(resultCh)
		var (
			N = len(ids) // number of requested objects
			// votes counts number of votes per object for each node
			votes      = make([]vote, 0, st.Level)
			contentIdx = -1 // index of full read reply
		)

		for r := range ch { // len(ch) == st.Level
			resp := r.Value
			if r.Err != nil { // at least one node is not responding
				f.log.WithField("op", "read_batch.get").WithField("replica", r.Value.Sender).
					WithField("class", f.class).WithField("shard", batch.Shard).Error(r.Err)
				resultCh <- batchResult{nil, errRead}
				return
			}
			if !resp.IsDigest {
				contentIdx = len(votes)
			}

			votes = append(votes, vote{resp, make([]int, N), nil})
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
				if max >= st.Level && maxAt == contentIdx {
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
		res, err := f.repairBatchPart(ctx, batch.Shard, ids, votes, st, contentIdx)
		if err != nil {
			resultCh <- batchResult{nil, errRepair}
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

// batchReply is a container of the batch received from a replica
// The returned data may result from a full or digest read request
type batchReply struct {
	// Sender hostname of the sender
	Sender string
	// IsDigest is this reply from a digest read?
	IsDigest bool
	// FullData returned from a full read request
	FullData []objects.Replica
	// DigestData returned from a digest read request
	DigestData []RepairResponse
}

// UpdateTimeAt gets update time from reply
func (r batchReply) UpdateTimeAt(idx int) int64 {
	if len(r.DigestData) != 0 {
		return r.DigestData[idx].UpdateTime
	}
	return r.FullData[idx].UpdateTime()
}
