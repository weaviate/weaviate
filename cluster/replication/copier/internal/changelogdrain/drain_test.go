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

package changelogdrain_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/replication/copier/internal/changelogdrain"
)

// fakeReceiver yields queued (msg, err) pairs, then io.EOF forever.
type fakeReceiver struct {
	frames []recvFrame
	idx    int
}

type recvFrame struct {
	msg *protocol.ChangeLogStreamEntry
	err error
}

func (f *fakeReceiver) Recv() (*protocol.ChangeLogStreamEntry, error) {
	if f.idx >= len(f.frames) {
		return nil, io.EOF
	}
	fr := f.frames[f.idx]
	f.idx++
	return fr.msg, fr.err
}

func mkEntry(t *testing.T, lsn uint64, isDelete bool) *protocol.ChangeLogStreamEntry {
	t.Helper()
	var uuidBytes [16]byte
	for i := range uuidBytes {
		uuidBytes[i] = byte(lsn>>uint(i*8)) ^ byte(i)
	}
	_, err := uuid.FromBytes(uuidBytes[:])
	require.NoError(t, err)

	payload := []byte("payload")
	if isDelete {
		payload = nil
	}
	return &protocol.ChangeLogStreamEntry{
		Lsn:              lsn,
		IsDelete:         isDelete,
		UpdateTimeMillis: int64(1_000_000 + lsn),
		Uuid:             uuidBytes[:],
		Payload:          payload,
	}
}

func TestDrain_HappyPath(t *testing.T) {
	const total = changelogdrain.BatchSize*2 + 17

	frames := make([]recvFrame, 0, total)
	for lsn := uint64(1); lsn <= total; lsn++ {
		frames = append(frames, recvFrame{msg: mkEntry(t, lsn, false)})
	}
	stream := &fakeReceiver{frames: frames}

	var applied []db.ChangeLogReplayEntry
	var applyCalls int
	apply := func(_ context.Context, batch []db.ChangeLogReplayEntry) error {
		applyCalls++
		applied = append(applied, batch...)
		return nil
	}

	lastLSN, err := changelogdrain.Drain(context.Background(), stream, apply)
	require.NoError(t, err)
	require.Equal(t, uint64(total), lastLSN)
	require.Equal(t, total, len(applied))
	require.Equal(t, 3, applyCalls) // 128 + 128 + 17
	for i, entry := range applied {
		require.Equal(t, int64(1_000_000+int64(i+1)), entry.LastUpdateTimeUnixMilli,
			"entry %d out of order", i)
	}
}

func TestDrain_DeletesAndPuts(t *testing.T) {
	frames := []recvFrame{
		{msg: mkEntry(t, 1, false)},
		{msg: mkEntry(t, 2, true)},
		{msg: mkEntry(t, 3, false)},
		{msg: mkEntry(t, 4, true)},
	}
	stream := &fakeReceiver{frames: frames}

	var applied []db.ChangeLogReplayEntry
	apply := func(_ context.Context, batch []db.ChangeLogReplayEntry) error {
		applied = append(applied, batch...)
		return nil
	}

	lastLSN, err := changelogdrain.Drain(context.Background(), stream, apply)
	require.NoError(t, err)
	require.Equal(t, uint64(4), lastLSN)
	require.Equal(t, 4, len(applied))
	require.False(t, applied[0].IsDelete)
	require.True(t, applied[1].IsDelete)
	require.False(t, applied[2].IsDelete)
	require.True(t, applied[3].IsDelete)
}

func TestDrain_RecvErrorAbortsImmediately(t *testing.T) {
	frames := []recvFrame{
		{msg: mkEntry(t, 1, false)},
		{msg: mkEntry(t, 2, false)},
		{err: errors.New("stream torn")},
		{msg: mkEntry(t, 3, false)}, // must never be consumed
	}
	stream := &fakeReceiver{frames: frames}

	var applyCalls int
	apply := func(_ context.Context, batch []db.ChangeLogReplayEntry) error {
		applyCalls++
		return nil
	}

	_, err := changelogdrain.Drain(context.Background(), stream, apply)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stream torn")
	// Frames 1 and 2 sat in the in-flight batch (sub-BatchSize), so the recv
	// error aborts before any flush.
	require.Equal(t, 0, applyCalls)
}

func TestDrain_ApplyErrorPropagates(t *testing.T) {
	frames := make([]recvFrame, 0, changelogdrain.BatchSize+10)
	for lsn := uint64(1); lsn <= changelogdrain.BatchSize+10; lsn++ {
		frames = append(frames, recvFrame{msg: mkEntry(t, lsn, false)})
	}
	stream := &fakeReceiver{frames: frames}

	applyErr := errors.New("apply kaboom")
	var applyCalls int
	apply := func(_ context.Context, _ []db.ChangeLogReplayEntry) error {
		applyCalls++
		return applyErr
	}

	lastLSN, err := changelogdrain.Drain(context.Background(), stream, apply)
	require.ErrorIs(t, err, applyErr)
	// Failed batch must not advance lastAppliedLSN.
	require.Equal(t, uint64(0), lastLSN)
	require.Equal(t, 1, applyCalls)
}

func TestDrain_EmptyStream(t *testing.T) {
	stream := &fakeReceiver{frames: nil}

	var applyCalls int
	apply := func(_ context.Context, _ []db.ChangeLogReplayEntry) error {
		applyCalls++
		return nil
	}

	lastLSN, err := changelogdrain.Drain(context.Background(), stream, apply)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastLSN)
	require.Equal(t, 0, applyCalls)
}
