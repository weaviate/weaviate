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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

var StreamId string = "329c306b-c912-4ec7-9b1d-55e5e0ca8dea"

func TestWorkerLoop(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	logger := logrus.New()

	t.Run("should process separate objs & refs requests from the queue and send data without error", func(t *testing.T) {
		mockBatcher := mocks.NewMockbatcher(t)

		reportingQueues := NewReportingQueues()
		reportingQueues.Make(StreamId)
		processingQueue := NewProcessingQueue()

		mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).Return(&pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: nil,
		}, nil).Times(1)
		mockBatcher.EXPECT().BatchReferences(mock.Anything, mock.Anything).Return(&pb.BatchReferencesReply{
			Took:   float32(1),
			Errors: nil,
		}, nil).Times(1)
		var wg sync.WaitGroup
		StartBatchWorkers(&wg, 1, processingQueue, reportingQueues, mockBatcher, logger)

		collection := "TestCollection"
		UUID0 := uuid.New().String()
		obj := &pb.BatchObject{Collection: collection, Uuid: UUID0}
		ref1 := &pb.BatchReference{
			FromUuid:       UUID0,
			ToUuid:         uuid.New().String(),
			Name:           "ref",
			FromCollection: "Class",
		}

		// Send data
		wg.Add(2)
		go func() {
			processingQueue <- &processRequest{
				objects:                       []*pb.BatchObject{obj},
				references:                    nil,
				streamId:                      StreamId,
				consistencyLevel:              nil,
				streamCtx:                     ctx,
				usesVectorisationByCollection: map[string]bool{collection: false},
				onComplete:                    func() { wg.Done() },
				onStart:                       func() {},
			}
			processingQueue <- &processRequest{
				objects:          nil,
				references:       []*pb.BatchReference{ref1},
				streamId:         StreamId,
				consistencyLevel: nil,
				streamCtx:        ctx,
				onComplete:       func() { wg.Done() },
				onStart:          func() {},
			}
		}()

		rq, ok := reportingQueues.Get(StreamId)
		require.True(t, ok, "Expected reporting queue to exist and to contain message")

		// Read first report from worker
		report := <-rq

		require.NotNil(t, report.Successes, "Expected successes to be returned")
		require.Equal(t, 0, len(report.Errors), "Expected no errors to be returned")
		require.NotNil(t, report.Stats, "Expected stats to be returned")
		require.Len(t, report.Successes, 1, "Expected one result to be returned")

		require.Equal(t, UUID0, report.Successes[0].GetUuid(), "Expected first result's UUID to match")

		// Read second report from worker
		report = <-rq

		require.NotNil(t, report.Successes, "Expected successes to be returned")
		require.Equal(t, 0, len(report.Errors), "Expected no errors to be returned")
		require.NotNil(t, report.Stats, "Expected stats to be returned")
		require.Len(t, report.Successes, 1, "Expected one result to be returned")

		require.Equal(t, toBeacon(ref1), report.Successes[0].GetBeacon(), "Expected second result's beacon to match")

		require.Empty(t, rq, "Expected reporting queue to be empty after reading all messages")
		close(processingQueue) // Allow the draining logic to exit naturally
		wg.Wait()
		require.Empty(t, processingQueue, "Expected processing queue to be empty after processing")
	})

	t.Run("should process combined objs & refs request from the queue and send data returning errors", func(t *testing.T) {
		mockBatcher := mocks.NewMockbatcher(t)

		reportingQueues := NewReportingQueues()
		reportingQueues.Make(StreamId)
		processingQueue := NewProcessingQueue()

		errorsObj := []*pb.BatchObjectsReply_BatchError{
			{
				Error: replicaerrors.ErrReplicas.Error(),
				Index: 0,
			},
			{
				Error: "objs error",
				Index: 1,
			},
		}
		errorsRefs := []*pb.BatchReferencesReply_BatchError{
			{
				Error: "refs error",
				Index: 0,
			},
		}
		// Return one retriable error and one regular error for objects
		mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).Return(&pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: errorsObj,
		}, nil).Times(1)
		// Verify that the retriable error is sent again and no error is returned this time
		mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).Return(&pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: nil,
		}, nil).Times(1)
		// Return one regular error for references
		mockBatcher.EXPECT().BatchReferences(mock.Anything, mock.Anything).Return(&pb.BatchReferencesReply{
			Took:   float32(1),
			Errors: errorsRefs,
		}, nil).Times(1)
		var wg sync.WaitGroup
		StartBatchWorkers(&wg, 1, processingQueue, reportingQueues, mockBatcher, logger)

		// Send data
		collection := "TestCollection"
		UUID0 := uuid.New().String()
		UUID1 := uuid.New().String()
		UUID2 := uuid.New().String()
		obj1 := &pb.BatchObject{Collection: collection, Uuid: UUID0}
		obj2 := &pb.BatchObject{Collection: collection, Uuid: UUID1}
		obj3 := &pb.BatchObject{Collection: collection, Uuid: UUID2}
		ref1 := &pb.BatchReference{
			FromUuid:       UUID0,
			ToUuid:         UUID1,
			Name:           "ref",
			FromCollection: "Class",
		}
		ref2 := &pb.BatchReference{
			FromUuid:       UUID1,
			ToUuid:         UUID2,
			Name:           "ref",
			FromCollection: "Class",
		}
		// must use goroutine to avoid deadlock due to one worker sending error over read stream
		// while next send to processing queue is blocked by there only being one worker
		wg.Add(1)
		go func() {
			processingQueue <- &processRequest{
				objects:                       []*pb.BatchObject{obj1, obj2, obj3},
				references:                    []*pb.BatchReference{ref1, ref2},
				streamId:                      StreamId,
				consistencyLevel:              nil,
				streamCtx:                     ctx,
				usesVectorisationByCollection: map[string]bool{collection: false},
				onComplete:                    func() { wg.Done() },
				onStart:                       func() {},
			}
		}()

		rq, ok := reportingQueues.Get(StreamId)
		require.True(t, ok, "Expected reporting queue to exist and to contain message")

		// Read first report from worker
		report := <-rq
		require.NotNil(t, report.Successes, "Expected successes to be returned")
		require.NotNil(t, report.Errors, "Expected errors to be returned")
		require.NotNil(t, report.Stats, "Expected stats to be returned")
		require.Len(t, report.Successes, 3, "Expected three successes to be returned")
		require.Len(t, report.Errors, 2, "Expected two errors to be returned")

		require.Equal(t, UUID0, report.Successes[0].GetUuid(), "Expected first success' UUID to match")
		require.Equal(t, UUID2, report.Successes[1].GetUuid(), "Expected second success' UUID to match")
		require.Equal(t, toBeacon(ref2), report.Successes[2].GetBeacon(), "Expected third success' beacon to match")

		require.Equal(t, "objs error", report.Errors[0].GetError(), "Expected first error to be first non-retriable object error")
		require.Equal(t, UUID1, report.Errors[0].GetUuid(), "Expected first error's UUID to match")
		require.Equal(t, "refs error", report.Errors[1].GetError(), "Expected second error to be first non-retriable reference error")
		require.Equal(t, toBeacon(ref1), report.Errors[1].GetBeacon(), "Expected second error's beacon to match")

		require.Empty(t, rq, "Expected reporting queue to be empty after reading all messages")
		close(processingQueue) // Allow the draining logic to exit naturally
		wg.Wait()
		require.Empty(t, processingQueue, "Expected processing queue to be empty after processing")
	})

	t.Run("should fanout if request uses vectorisation", func(t *testing.T) {
		mockBatcher := mocks.NewMockbatcher(t)

		reportingQueues := NewReportingQueues()
		reportingQueues.Make(StreamId)
		processingQueue := NewProcessingQueue()

		numObjs := 101
		mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
			return &pb.BatchObjectsReply{
				Took:   float32(1),
				Errors: nil,
			}, nil
		}).Times(10)

		var wg sync.WaitGroup
		StartBatchWorkers(&wg, 1, processingQueue, reportingQueues, mockBatcher, logger)

		collection := "TestCollection"
		objs := []*pb.BatchObject{}
		for i := 0; i < numObjs; i++ {
			objs = append(objs, &pb.BatchObject{Collection: collection, Uuid: uuid.New().String()})
		}

		// Send data
		wg.Add(1)
		go func() {
			processingQueue <- &processRequest{
				objects:                       objs,
				references:                    nil,
				streamId:                      StreamId,
				consistencyLevel:              nil,
				streamCtx:                     ctx,
				usesVectorisationByCollection: map[string]bool{collection: true},
				onComplete:                    func() { wg.Done() },
				onStart:                       func() {},
			}
		}()

		rq, ok := reportingQueues.Get(StreamId)
		require.True(t, ok, "Expected reporting queue to exist and to contain message")

		// Read first report from worker
		report := <-rq
		require.NotNil(t, report.Successes, "Expected successes to be returned")
		require.Equal(t, 0, len(report.Errors), "Expected no errors to be returned")
		require.NotNil(t, report.Stats, "Expected stats to be returned")
		require.Len(t, report.Successes, numObjs, "Expected 101 results to be returned")

		require.Empty(t, rq, "Expected reporting queue to be empty after reading all messages")
		close(processingQueue) // Allow the draining logic to exit naturally
		wg.Wait()
		require.Empty(t, processingQueue, "Expected processing queue to be empty after processing")
	})

	t.Run("worker exits cleanly when streamCtx cancels mid-process and remains available", func(t *testing.T) {
		mockBatcher := mocks.NewMockbatcher(t)

		reportingQueues := NewReportingQueues()
		reportingQueues.Make(StreamId)
		processingQueue := NewProcessingQueue()

		// Closed inside the mock so the test can cancel only after the worker is mid-call,
		// not mid-setup (sendObjects short-circuits if ctx is already cancelled on entry).
		batchObjectsAStarted := make(chan struct{})

		mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
			close(batchObjectsAStarted)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(5 * time.Second):
				return &pb.BatchObjectsReply{Took: 1}, nil
			}
		}).Once()
		mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
			return &pb.BatchObjectsReply{Took: 1}, nil
		}).Once()

		var wg sync.WaitGroup
		StartBatchWorkers(&wg, 1, processingQueue, reportingQueues, mockBatcher, logger)

		collection := "TestCollection"
		uuidA := uuid.New().String()
		uuidB := uuid.New().String()
		objA := &pb.BatchObject{Collection: collection, Uuid: uuidA}
		objB := &pb.BatchObject{Collection: collection, Uuid: uuidB}

		streamCtxA, cancelA := context.WithCancel(t.Context())
		completedA := make(chan struct{})

		streamCtxB := t.Context()
		completedB := make(chan struct{})

		wg.Add(2)

		go func() {
			processingQueue <- &processRequest{
				objects:                       []*pb.BatchObject{objA},
				streamId:                      StreamId,
				streamCtx:                     streamCtxA,
				usesVectorisationByCollection: map[string]bool{collection: false},
				onStart:                       func() {},
				onComplete:                    func() { close(completedA); wg.Done() },
			}
		}()

		<-batchObjectsAStarted
		cancelA()

		select {
		case <-completedA:
		case <-time.After(PER_PROCESS_TIMEOUT + time.Second):
			t.Fatal("worker did not complete after streamCtx cancel")
		}

		go func() {
			processingQueue <- &processRequest{
				objects:                       []*pb.BatchObject{objB},
				streamId:                      StreamId,
				streamCtx:                     streamCtxB,
				usesVectorisationByCollection: map[string]bool{collection: false},
				onStart:                       func() {},
				onComplete:                    func() { close(completedB); wg.Done() },
			}
		}()

		rq, ok := reportingQueues.Get(StreamId)
		require.True(t, ok)
		report := <-rq
		require.Len(t, report.Successes, 1)
		require.Equal(t, uuidB, report.Successes[0].GetUuid())

		<-completedB

		close(processingQueue)
		wg.Wait()
		require.Empty(t, processingQueue)
	})

	t.Run("should fanout if request uses vectorisation returning errors correctly", func(t *testing.T) {
		mockBatcher := mocks.NewMockbatcher(t)

		reportingQueues := NewReportingQueues()
		reportingQueues.Make(StreamId)
		processingQueue := NewProcessingQueue()

		mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
			require.Len(t, req.Objects, 10, "Expected each batched request to contain 10 objects")
			return &pb.BatchObjectsReply{
				Took: float32(1),
				Errors: []*pb.BatchObjectsReply_BatchError{
					{
						Error: "objs error",
						Index: 1,
					},
				},
			}, nil
		}).Times(10)

		var wg sync.WaitGroup
		StartBatchWorkers(&wg, 1, processingQueue, reportingQueues, mockBatcher, logger)

		collection := "TestCollection"
		objs := []*pb.BatchObject{}
		for i := 0; i < 100; i++ {
			objs = append(objs, &pb.BatchObject{Collection: collection, Uuid: uuid.New().String()})
		}

		// Send data
		wg.Add(1)
		go func() {
			processingQueue <- &processRequest{
				objects:                       objs,
				references:                    nil,
				streamId:                      StreamId,
				consistencyLevel:              nil,
				streamCtx:                     ctx,
				usesVectorisationByCollection: map[string]bool{collection: true},
				onComplete:                    func() { wg.Done() },
				onStart:                       func() {},
			}
		}()

		rq, ok := reportingQueues.Get(StreamId)
		require.True(t, ok, "Expected reporting queue to exist and to contain message")

		// Read first report from worker
		report := <-rq
		require.NotNil(t, report.Successes, "Expected successes to be returned")
		require.Len(t, report.Errors, 10, "Expected 10 errors to be returned")
		// the sub-batches complete in any order, so the errors are a set: one per
		// sub-batch, each for that sub-batch's second object
		expectedErrored := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			expectedErrored = append(expectedErrored, objs[i*10+1].GetUuid())
		}
		require.ElementsMatch(t, expectedErrored, errorUuids(report))
		require.NotNil(t, report.Stats, "Expected stats to be returned")
		require.Len(t, report.Successes, 90, "Expected 90 successes to be returned")
		require.Empty(t, rq, "Expected reporting queue to be empty after reading all messages")
		close(processingQueue) // Allow the draining logic to exit naturally
		wg.Wait()
		require.Empty(t, processingQueue, "Expected processing queue to be empty after processing")
	})
}

// processOneRequest runs one batch through a real worker. No report means the
// worker died in the recovery wrapper, which is itself a failure.
func processOneRequest(t *testing.T, batcher batcher, objs []*pb.BatchObject, refs []*pb.BatchReference, usesVectorisationByCollection map[string]bool) *report {
	t.Helper()

	reportingQueues := NewReportingQueues()
	reportingQueues.Make(StreamId)
	processingQueue := NewProcessingQueue()

	var wg sync.WaitGroup
	StartBatchWorkers(&wg, 1, processingQueue, reportingQueues, batcher, logrus.New())

	wg.Add(1)
	go func() {
		processingQueue <- &processRequest{
			objects:                       objs,
			references:                    refs,
			streamId:                      StreamId,
			streamCtx:                     t.Context(),
			usesVectorisationByCollection: usesVectorisationByCollection,
			onComplete:                    func() { wg.Done() },
			onStart:                       func() {},
		}
	}()

	rq, ok := reportingQueues.Get(StreamId)
	require.True(t, ok)

	var rep *report
	select {
	case rep = <-rq:
	case <-time.After(10 * time.Second):
		t.Fatal("no report was produced for the batch")
	}

	close(processingQueue)
	wg.Wait()
	return rep
}

func successUuids(rep *report) []string {
	out := make([]string, 0, len(rep.Successes))
	for _, s := range rep.Successes {
		out = append(out, s.GetUuid())
	}
	return out
}

func errorUuids(rep *report) []string {
	out := make([]string, 0, len(rep.Errors))
	for _, e := range rep.Errors {
		out = append(out, e.GetUuid())
	}
	return out
}

func successBeacons(rep *report) []string {
	out := make([]string, 0, len(rep.Successes))
	for _, s := range rep.Successes {
		out = append(out, s.GetBeacon())
	}
	return out
}

func errorBeacons(rep *report) []string {
	out := make([]string, 0, len(rep.Errors))
	for _, e := range rep.Errors {
		out = append(out, e.GetBeacon())
	}
	return out
}

func uuidsOf(objs []*pb.BatchObject, idxs ...int) []string {
	out := make([]string, 0, len(idxs))
	for _, i := range idxs {
		out = append(out, objs[i].GetUuid())
	}
	return out
}

func newObjs(collection string, howMany int) []*pb.BatchObject {
	objs := make([]*pb.BatchObject, 0, howMany)
	for range howMany {
		objs = append(objs, &pb.BatchObject{Collection: collection, Uuid: uuid.New().String()})
	}
	return objs
}

// Only the failed sub-batch's objects come back as errors, and none of them is
// also a success.
func TestSendObjectsSubBatchTransportErrorScope(t *testing.T) {
	collection := "TestCollection"
	objs := newObjs(collection, 20)
	// fanout is 10 for a vectorising collection, so sub-batches are objs[2i:2i+2]
	failFirstUuid := objs[4].GetUuid()

	mockBatcher := mocks.NewMockbatcher(t)
	mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
		require.Len(t, req.Objects, 2)
		if req.Objects[0].GetUuid() == failFirstUuid {
			return nil, errors.New("transport blew up")
		}
		return &pb.BatchObjectsReply{Took: 1}, nil
	}).Times(10)

	rep := processOneRequest(t, mockBatcher, objs, nil, map[string]bool{collection: true})

	require.ElementsMatch(t, uuidsOf(objs, 4, 5), errorUuids(rep), "only the failed sub-batch's objects are errors")
	require.ElementsMatch(t, uuidsOf(objs, 0, 1, 2, 3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19), successUuids(rep),
		"every other object succeeds, and no failed object is also reported successful")
}

// Reply errors are keyed within one collection's slice while successes span the
// whole batch; mixing collections must not cross the two up.
func TestSendObjectsMultiCollectionPartition(t *testing.T) {
	collectionA := "CollectionA"
	collectionB := "CollectionB"
	objsA := newObjs(collectionA, 2)
	objsB := newObjs(collectionB, 2)
	objs := []*pb.BatchObject{objsA[0], objsA[1], objsB[0], objsB[1]}

	mockBatcher := mocks.NewMockbatcher(t)
	mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
		if req.Objects[0].GetCollection() == collectionB {
			return &pb.BatchObjectsReply{Took: 1, Errors: []*pb.BatchObjectsReply_BatchError{
				{Error: "collection B index 0 failed", Index: 0},
			}}, nil
		}
		return &pb.BatchObjectsReply{Took: 1}, nil
	}).Times(2)

	// both collections are non-vectorising, so each is one call and the case is
	// deterministic regardless of reply order
	rep := processOneRequest(t, mockBatcher, objs, nil, map[string]bool{collectionA: false, collectionB: false})

	require.ElementsMatch(t, uuidsOf(objs, 2), errorUuids(rep))
	require.ElementsMatch(t, uuidsOf(objs, 0, 1, 3), successUuids(rep))
}

// An unattributable reply entry is dropped: never blamed on the wrong beacon,
// never fatal to the worker.
func TestSendReferencesReplyIndexGuards(t *testing.T) {
	refs := []*pb.BatchReference{
		{FromCollection: "Class", FromUuid: uuid.New().String(), ToUuid: uuid.New().String(), Name: "ref"},
		{FromCollection: "Class", FromUuid: uuid.New().String(), ToUuid: uuid.New().String(), Name: "ref"},
		{FromCollection: "Class", FromUuid: uuid.New().String(), ToUuid: uuid.New().String(), Name: "ref"},
	}
	beaconsOf := func(idxs ...int) []string {
		out := make([]string, 0, len(idxs))
		for _, i := range idxs {
			out = append(out, toBeacon(refs[i]))
		}
		return out
	}

	cases := []struct {
		name              string
		replyErrors       []*pb.BatchReferencesReply_BatchError
		expectedErrors    []string
		expectedSuccesses []string
	}{
		{
			name:              "nil entry",
			replyErrors:       []*pb.BatchReferencesReply_BatchError{nil},
			expectedErrors:    []string{},
			expectedSuccesses: beaconsOf(0, 1, 2),
		},
		{
			name:              "index beyond the request",
			replyErrors:       []*pb.BatchReferencesReply_BatchError{{Error: "boom", Index: 99}},
			expectedErrors:    []string{},
			expectedSuccesses: beaconsOf(0, 1, 2),
		},
		{
			name: "valid entry alongside unattributable ones",
			replyErrors: []*pb.BatchReferencesReply_BatchError{
				nil,
				{Error: "boom", Index: 99},
				{Error: "reference one failed", Index: 1},
			},
			expectedErrors:    beaconsOf(1),
			expectedSuccesses: beaconsOf(0, 2),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockBatcher := mocks.NewMockbatcher(t)
			mockBatcher.EXPECT().BatchReferences(mock.Anything, mock.Anything).Return(&pb.BatchReferencesReply{
				Took:   1,
				Errors: tc.replyErrors,
			}, nil).Once()

			rep := processOneRequest(t, mockBatcher, nil, refs, nil)

			require.ElementsMatch(t, tc.expectedErrors, errorBeacons(rep))
			require.ElementsMatch(t, tc.expectedSuccesses, successBeacons(rep))
		})
	}
}

// Drives the reply consumer directly, so reply order is a test input rather than
// a race. Errors must be attributed through the sub-batch, never arrival order.
func TestConsumeFanoutReplies(t *testing.T) {
	type replySpec struct {
		offset int
		length int
		err    error
		errors []*pb.BatchObjectsReply_BatchError
	}
	type errorSpec struct {
		outerIdx int
		text     string
	}

	transient := replicaerrors.ErrReplicas.Error()

	cases := []struct {
		name          string
		objCount      int
		outerIdxs     []int // nil means the collection owns the whole batch, in order
		replies       []replySpec
		wantErrors    []errorSpec
		wantErrored   []int
		wantRetriable []int
	}{
		{
			name:     "replies in position order",
			objCount: 6,
			replies: []replySpec{
				{offset: 0, length: 2, errors: []*pb.BatchObjectsReply_BatchError{{Index: 0, Error: "e0"}}},
				{offset: 2, length: 2, errors: []*pb.BatchObjectsReply_BatchError{{Index: 1, Error: "e1"}}},
				{offset: 4, length: 2, errors: []*pb.BatchObjectsReply_BatchError{{Index: 1, Error: "e2"}}},
			},
			wantErrors:  []errorSpec{{0, "e0"}, {3, "e1"}, {5, "e2"}},
			wantErrored: []int{0, 3, 5},
		},
		{
			name:     "replies in reverse order",
			objCount: 6,
			replies: []replySpec{
				{offset: 4, length: 2, errors: []*pb.BatchObjectsReply_BatchError{{Index: 1, Error: "e2"}}},
				{offset: 2, length: 2, errors: []*pb.BatchObjectsReply_BatchError{{Index: 1, Error: "e1"}}},
				{offset: 0, length: 2, errors: []*pb.BatchObjectsReply_BatchError{{Index: 0, Error: "e0"}}},
			},
			wantErrors:  []errorSpec{{0, "e0"}, {3, "e1"}, {5, "e2"}},
			wantErrored: []int{0, 3, 5},
		},
		{
			// a batch that is not a multiple of the fanout ends in a short sub-batch
			name:     "short final sub-batch, replies out of order",
			objCount: 5,
			replies: []replySpec{
				{offset: 4, length: 1, errors: []*pb.BatchObjectsReply_BatchError{{Index: 0, Error: "e2"}}},
				{offset: 0, length: 2, errors: []*pb.BatchObjectsReply_BatchError{{Index: 1, Error: "e0"}}},
				{offset: 2, length: 2, errors: []*pb.BatchObjectsReply_BatchError{{Index: 0, Error: "e1"}}},
			},
			wantErrors:  []errorSpec{{1, "e0"}, {2, "e1"}, {4, "e2"}},
			wantErrored: []int{1, 2, 4},
		},
		{
			name:     "nil entry in the reply's error list",
			objCount: 2,
			replies: []replySpec{
				{offset: 0, length: 2, errors: []*pb.BatchObjectsReply_BatchError{nil, {Index: 1, Error: "e1"}}},
			},
			wantErrors:  []errorSpec{{1, "e1"}},
			wantErrored: []int{1},
		},
		{
			name:     "reply error index outside the sub-batch",
			objCount: 2,
			replies: []replySpec{
				{offset: 0, length: 2, errors: []*pb.BatchObjectsReply_BatchError{{Index: 2, Error: "beyond"}, {Index: -1, Error: "negative"}}},
			},
			wantErrors:  nil,
			wantErrored: nil,
		},
		{
			name:     "whole sub-batch fails in transport",
			objCount: 4,
			replies: []replySpec{
				{offset: 0, length: 2, err: errors.New("transport blew up")},
				{offset: 2, length: 2},
			},
			wantErrors:  []errorSpec{{0, "transport blew up"}, {1, "transport blew up"}},
			wantErrored: []int{0, 1},
		},
		{
			name:      "objects interleaved with another collection",
			objCount:  4,
			outerIdxs: []int{3, 1},
			replies: []replySpec{
				{offset: 0, length: 2, errors: []*pb.BatchObjectsReply_BatchError{{Index: 1, Error: "e1"}}},
			},
			wantErrors:  []errorSpec{{1, "e1"}},
			wantErrored: []int{1},
		},
		{
			name:     "transient replication error is retried, not reported",
			objCount: 4,
			replies: []replySpec{
				{offset: 0, length: 2, errors: []*pb.BatchObjectsReply_BatchError{{Index: 1, Error: transient}}},
				{offset: 2, length: 2, errors: []*pb.BatchObjectsReply_BatchError{{Index: 0, Error: "e2"}}},
			},
			wantErrors:    []errorSpec{{2, "e2"}},
			wantErrored:   []int{1, 2},
			wantRetriable: []int{1},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objs := newObjs("TestCollection", tc.objCount)
			outerIdxs := tc.outerIdxs
			if outerIdxs == nil {
				outerIdxs = make([]int, 0, tc.objCount)
				for i := range objs {
					outerIdxs = append(outerIdxs, i)
				}
			}
			collectionObjs := make([]*pb.BatchObject, 0, len(outerIdxs))
			for _, i := range outerIdxs {
				collectionObjs = append(collectionObjs, objs[i])
			}

			replies := make(chan fanoutReply, len(tc.replies))
			for _, spec := range tc.replies {
				var reply *pb.BatchObjectsReply
				if spec.err == nil {
					reply = &pb.BatchObjectsReply{Took: 1, Errors: spec.errors}
				}
				replies <- fanoutReply{
					reply:    reply,
					err:      spec.err,
					subBatch: collectionObjs[spec.offset : spec.offset+spec.length],
					offset:   spec.offset,
				}
			}
			close(replies)

			w := &worker{logger: logrus.New()}
			errored := make(map[int]struct{})
			errs, retriable := w.consumeFanoutReplies(StreamId, replies, objs, outerIdxs, 0, errored)

			wantErrors := make([]string, 0, len(tc.wantErrors))
			for _, want := range tc.wantErrors {
				wantErrors = append(wantErrors, objs[want.outerIdx].GetUuid()+" => "+want.text)
			}
			gotErrors := make([]string, 0, len(errs))
			for _, err := range errs {
				gotErrors = append(gotErrors, err.GetUuid()+" => "+err.GetError())
			}
			require.ElementsMatch(t, wantErrors, gotErrors, "each error must name the object its reply was for")

			gotErrored := make([]int, 0, len(errored))
			for i := range errored {
				gotErrored = append(gotErrored, i)
			}
			require.ElementsMatch(t, tc.wantErrored, gotErrored, "the errored set decides which objects are reported successful")

			wantRetriable := make([]string, 0, len(tc.wantRetriable))
			for _, i := range tc.wantRetriable {
				wantRetriable = append(wantRetriable, objs[i].GetUuid())
			}
			gotRetriable := make([]string, 0, len(retriable))
			for _, obj := range retriable {
				gotRetriable = append(gotRetriable, obj.GetUuid())
			}
			require.ElementsMatch(t, wantRetriable, gotRetriable)
		})
	}
}
