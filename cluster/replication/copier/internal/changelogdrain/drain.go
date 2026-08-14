//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package changelogdrain pulls ChangeLogStreamEntry messages from a gRPC
// server-streaming client, batches them, and flushes via a caller-supplied
// apply func.
package changelogdrain

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/adapters/repos/db"
)

// BatchSize caps per-flush memory: each entry carries a storobj payload, which can be
// small ~ 1KB or large ~4MB+. If throughput is seen to be low (too small value) or memory is high
// (too large a value for the objects) then this may need to be dynamically set.
const BatchSize = 128

// Receiver is the minimum the drain loop needs from a gRPC server-streaming
// client.
type Receiver interface {
	Recv() (*protocol.ChangeLogStreamEntry, error)
}

type ApplyFunc func(ctx context.Context, batch []db.ChangeLogReplayEntry) error

// Drain returns on io.EOF (final flush is performed first), recv/apply error,
// or ctx cancel. lastAppliedLSN reflects the last successfully-flushed entry;
// entries buffered in the in-flight batch do not advance it.
func Drain(
	ctx context.Context,
	stream Receiver,
	apply ApplyFunc,
) (lastAppliedLSN uint64, err error) {
	batch := make([]db.ChangeLogReplayEntry, 0, BatchSize)
	var lastBatchedLSN uint64

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := apply(ctx, batch); err != nil {
			return err
		}
		lastAppliedLSN = lastBatchedLSN
		batch = batch[:0]
		return nil
	}

	for {
		msg, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			if flushErr := flush(); flushErr != nil {
				return lastAppliedLSN, flushErr
			}
			return lastAppliedLSN, nil
		}
		if recvErr != nil {
			return lastAppliedLSN, fmt.Errorf("recv change-log entry: %w", recvErr)
		}

		id, err := uuid.FromBytes(msg.Uuid)
		if err != nil {
			return lastAppliedLSN, fmt.Errorf("decode uuid for lsn %d: %w", msg.Lsn, err)
		}

		batch = append(batch, db.ChangeLogReplayEntry{
			ID:                      strfmt.UUID(id.String()),
			LastUpdateTimeUnixMilli: msg.UpdateTimeMillis,
			IsDelete:                msg.IsDelete,
			Payload:                 msg.Payload,
		})
		lastBatchedLSN = msg.Lsn

		if len(batch) >= BatchSize {
			if err := flush(); err != nil {
				return lastAppliedLSN, err
			}
		}
	}
}
