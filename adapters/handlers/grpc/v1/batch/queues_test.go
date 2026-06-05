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

package batch

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func Test_statsUpdateBatchSize(t *testing.T) {
	t.Run("calculate new batch size when processing time is ideal", func(t *testing.T) {
		stats := newStats() // default batch size is 200
		stats.updateBatchSize(time.Second)
		require.Equal(t, 200, stats.getBatchSize())
		// when it takes 1s to process, the batch size should remain the same
	})

	t.Run("increase batch size when processing time is lesser in value than ideal", func(t *testing.T) {
		stats := newStats() // default batch size is 200
		stats.updateBatchSize(500 * time.Millisecond)
		require.Greater(t, stats.getBatchSize(), 200)
		// when it takes less than 1s to process, the batch size should increase
	})

	t.Run("decrease batch size when processing time is greater in value than ideal", func(t *testing.T) {
		stats := newStats() // default batch size is 200
		stats.updateBatchSize(2 * time.Second)
		require.Less(t, stats.getBatchSize(), 200)
		// when it takes more than 1s to process, the batch size should decrease
	})

	t.Run("batch size should not go below 100", func(t *testing.T) {
		stats := newStats() // default batch size is 200
		for i := 0; i < 100; i++ {
			stats.updateBatchSize(10 * time.Second)
		}
		require.Equal(t, 100, stats.getBatchSize())
		// when it takes more than 1s to process, the batch size should decrease, but not below 100
	})

	t.Run("batch size should not go above 1000", func(t *testing.T) {
		stats := newStats() // default batch size is 200
		for i := 0; i < 100; i++ {
			stats.updateBatchSize(10 * time.Millisecond)
		}
		require.Equal(t, 1000, stats.getBatchSize())
		// when it takes less than 1s to process, the batch size should increase, but not above 1000
	})
}

func TestReportingQueueSendBlocksUntilDelivery(t *testing.T) {
	streamId := "test-stream-blocks-until-delivery"
	q := NewReportingQueues()
	q.Make(streamId)

	successes := []*pb.BatchStreamReply_Results_Success{
		{Detail: &pb.BatchStreamReply_Results_Success_Uuid{Uuid: "uuid-1"}},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- q.send(t.Context(), streamId, successes, nil, &workerStats{processingTime: time.Second})
	}()

	select {
	case err := <-errCh:
		t.Fatalf("send returned prematurely: %v", err)
	case <-time.After(2 * time.Second):
	}

	rq, ok := q.Get(streamId)
	require.True(t, ok)
	report := <-rq
	require.Len(t, report.Successes, 1)
	require.Equal(t, "uuid-1", report.Successes[0].GetUuid())

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("send did not return after queue drained")
	}
}

func TestReportingQueueCloseUnblocksBlockedSend(t *testing.T) {
	streamId := "test-stream-close-unblocks-send"
	q := NewReportingQueues()
	q.Make(streamId)

	successes := []*pb.BatchStreamReply_Results_Success{
		{Detail: &pb.BatchStreamReply_Results_Success_Uuid{Uuid: "uuid-1"}},
	}

	sendCh := make(chan error, 1)
	go func() {
		sendCh <- q.send(t.Context(), streamId, successes, nil, &workerStats{processingTime: time.Second})
	}()

	time.Sleep(10 * time.Millisecond) // let the send goroutine park in the select

	closeCh := make(chan struct{})
	go func() {
		q.close(streamId)
		close(closeCh)
	}()

	select {
	case <-closeCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("close did not complete promptly with a blocked send")
	}

	select {
	case err := <-sendCh:
		require.ErrorIs(t, err, errReportingQueueClosed)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("send did not return after close")
	}
}

func TestReportingQueueSendCancelsOnStreamCtx(t *testing.T) {
	streamId := "test-stream-cancels-on-streamctx"
	q := NewReportingQueues()
	q.Make(streamId)

	streamCtx, cancel := context.WithCancel(t.Context())

	successes := []*pb.BatchStreamReply_Results_Success{
		{Detail: &pb.BatchStreamReply_Results_Success_Uuid{Uuid: "uuid-1"}},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- q.send(streamCtx, streamId, successes, nil, &workerStats{processingTime: time.Second})
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("send did not return after streamCtx cancel")
	}
}
