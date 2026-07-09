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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/weaviate/tiktoken-go"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/sirupsen/logrus/hooks/test"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func maxTokensPerBatch(cfg moduletools.ClassConfig) int {
	return 100
}

func TestBatch(t *testing.T) {
	cfg := &fakeClassConfig{vectorizePropertyName: false, classConfig: map[string]interface{}{"vectorizeClassName": false}}
	logger, _ := test.NewNullLogger()
	cases := []struct {
		name       string
		objects    []*models.Object
		skip       []bool
		wantErrors map[int]error
		deadline   time.Duration
	}{
		{name: "skip all", objects: []*models.Object{{Class: "Car"}}, skip: []bool{true}},
		{name: "skip first", objects: []*models.Object{{Class: "Car"}, {Class: "Car", Properties: map[string]interface{}{"test": "test"}}}, skip: []bool{true, false}},
		{name: "one object errors", objects: []*models.Object{{Class: "Car", Properties: map[string]interface{}{"test": "test"}}, {Class: "Car", Properties: map[string]interface{}{"test": "error something"}}}, skip: []bool{false, false}, wantErrors: map[int]error{1: fmt.Errorf("something")}},
		{name: "first object errors", objects: []*models.Object{{Class: "Car", Properties: map[string]interface{}{"test": "error something"}}, {Class: "Car", Properties: map[string]interface{}{"test": "test"}}}, skip: []bool{false, false}, wantErrors: map[int]error{0: fmt.Errorf("something")}},
		{name: "vectorize all", objects: []*models.Object{{Class: "Car", Properties: map[string]interface{}{"test": "test"}}, {Class: "Car", Properties: map[string]interface{}{"test": "something"}}}, skip: []bool{false, false}},
		{name: "multiple vectorizer batches", objects: []*models.Object{
			{Class: "Car", Properties: map[string]interface{}{"test": "tokens 80"}}, // set limit so next 3 objects are one batch
			{Class: "Car", Properties: map[string]interface{}{"test": "first object first batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "second object first batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "third object first batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "first object second batch"}}, // rate is 100 again
			{Class: "Car", Properties: map[string]interface{}{"test": "second object second batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "third object second batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "fourth object second batch"}},
		}, skip: []bool{false, false, false, false, false, false, false, false}},
		{name: "multiple vectorizer batches with skips and errors", objects: []*models.Object{
			{Class: "Car", Properties: map[string]interface{}{"test": "tokens 70"}}, // set limit so next 3 objects are one batch
			{Class: "Car", Properties: map[string]interface{}{"test": "first object first batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "second object first batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "error something"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "first object second batch"}}, // rate is 100 again
			{Class: "Car", Properties: map[string]interface{}{"test": "second object second batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "third object second batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "fourth object second batch"}},
		}, skip: []bool{false, true, false, false, false, true, false, false}, wantErrors: map[int]error{3: fmt.Errorf("something")}},
		{name: "token too long", objects: []*models.Object{
			{Class: "Car", Properties: map[string]interface{}{"test": "tokens 10"}}, // set limit
			{Class: "Car", Properties: map[string]interface{}{"test": "long long long long, long, long, long, long"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "short"}},
		}, skip: []bool{false, false, false}, wantErrors: map[int]error{1: fmt.Errorf("text too long for vectorization from provider: got 43, total limit: 20, remaining: 10")}},
		{name: "token too long, last item in batch", objects: []*models.Object{
			{Class: "Car", Properties: map[string]interface{}{"test": "tokens 10"}}, // set limit
			{Class: "Car", Properties: map[string]interface{}{"test": "short"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "long long long long, long, long, long, long"}},
		}, skip: []bool{false, false, false}, wantErrors: map[int]error{2: fmt.Errorf("text too long for vectorization from provider: got 43, total limit: 20, remaining: 10")}},
		{name: "skip last item", objects: []*models.Object{
			{Class: "Car", Properties: map[string]interface{}{"test": "1. test object"}}, // set limit
			{Class: "Car", Properties: map[string]interface{}{"test": "1. obj 1. batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "2. obj 1. batch"}},
		}, skip: []bool{false, false, true}},
		// The deadline expires while the first batch is still in flight, so the call
		// is abandoned before any object is resolved: every non-skipped object gets
		// the deadline error and no vectors are returned.
		{name: "deadline", deadline: 200 * time.Millisecond, objects: []*models.Object{
			{Class: "Car", Properties: map[string]interface{}{"test": "tokens 40"}}, // set limit so next two items are in a batch
			{Class: "Car", Properties: map[string]interface{}{"test": "wait 400"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "long long long long"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "next batch, will be aborted due to context deadline"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "skipped"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "has error again"}},
		}, skip: []bool{false, false, false, false, true, false}, wantErrors: map[int]error{
			0: fmt.Errorf("context deadline exceeded"),
			1: fmt.Errorf("context deadline exceeded"),
			2: fmt.Errorf("context deadline exceeded"),
			3: fmt.Errorf("context deadline exceeded"),
			5: fmt.Errorf("context deadline exceeded"),
		}},
		{name: "request error", objects: []*models.Object{
			{Class: "Car", Properties: map[string]interface{}{"test": "ReqError something"}},
		}, skip: []bool{false}, wantErrors: map[int]error{0: fmt.Errorf("something")}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeBatchClientWithRL[[]float32]{} // has state

			v := NewBatchVectorizer[[]float32](client, 1*time.Second,
				Settings{MaxObjectsPerBatch: 2000, MaxTokensPerBatch: maxTokensPerBatch, MaxTimePerBatch: 10, ReturnsRateLimit: true, HasTokenLimit: true},
				logger, "test") // avoid waiting for rate limit
			deadline := time.Now().Add(10 * time.Second)
			if tt.deadline != 0 {
				deadline = time.Now().Add(tt.deadline)
			}

			texts, tokenCounts := generateTokens(tt.objects)

			ctx, cancl := context.WithDeadline(context.Background(), deadline)
			vecs, errs := v.SubmitBatchAndWait(ctx, cfg, tt.skip, tokenCounts, texts)

			require.Len(t, errs, len(tt.wantErrors))
			require.Len(t, vecs, len(tt.objects))

			for i := range tt.objects {
				if tt.wantErrors[i] != nil {
					require.Equal(t, tt.wantErrors[i], errs[i])
				} else if tt.skip[i] {
					require.Nil(t, vecs[i])
				} else {
					require.NotNil(t, vecs[i])
				}
			}
			cancl()
		})
	}
}

func TestBatchNoRLreturn(t *testing.T) {
	cfg := &fakeClassConfig{vectorizePropertyName: false, classConfig: map[string]interface{}{"vectorizeClassName": false}}
	logger, _ := test.NewNullLogger()
	cases := []struct {
		name       string
		objects    []*models.Object
		skip       []bool
		wantErrors map[int]error
		deadline   time.Duration
		resetRate  int
		tokenLimit int
	}{
		{name: "low reset time - dont deadlock", objects: []*models.Object{{Class: "Car", Properties: map[string]interface{}{"test": "more tokens than TL"}}}, skip: []bool{false}, resetRate: 0, tokenLimit: 1},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeBatchClientWithoutRL[[]float32]{defaultResetRate: tt.resetRate, defaultTPM: tt.tokenLimit} // has state

			v := NewBatchVectorizer(client, 1*time.Second,
				Settings{MaxObjectsPerBatch: 2000, MaxTokensPerBatch: maxTokensPerBatch, MaxTimePerBatch: 10},
				logger, "test") // avoid waiting for rate limit
			deadline := time.Now().Add(10 * time.Second)
			if tt.deadline != 0 {
				deadline = time.Now().Add(tt.deadline)
			}

			texts, tokenCounts := generateTokens(tt.objects)

			ctx, cancl := context.WithDeadline(context.Background(), deadline)
			vecs, errs := v.SubmitBatchAndWait(ctx, cfg, tt.skip, tokenCounts, texts)

			require.Len(t, errs, len(tt.wantErrors))
			require.Len(t, vecs, len(tt.objects))

			for i := range tt.objects {
				if tt.wantErrors[i] != nil {
					require.Equal(t, tt.wantErrors[i], errs[i])
				} else if tt.skip[i] {
					require.Nil(t, vecs[i])
				} else {
					require.NotNil(t, vecs[i])
				}
			}
			cancl()
		})
	}
}

func TestBatchMultiple(t *testing.T) {
	client := &fakeBatchClientWithRL[[]float32]{}
	cfg := &fakeClassConfig{vectorizePropertyName: false, classConfig: map[string]interface{}{"vectorizeClassName": false}}
	logger, _ := test.NewNullLogger()

	v := NewBatchVectorizer[[]float32](client, 40*time.Second, Settings{MaxObjectsPerBatch: 2000, MaxTokensPerBatch: maxTokensPerBatch, MaxTimePerBatch: 10, HasTokenLimit: true, ReturnsRateLimit: true}, logger, "test") // avoid waiting for rate limit
	res := make(chan int, 3)
	wg := sync.WaitGroup{}
	wg.Add(3)

	// send multiple batches to the vectorizer and check if they are processed in the correct order. Note that the
	// ObjectBatch function is doing some work before the objects are send to vectorization, so we need to leave some
	// time to account for that
	for i := 0; i < 3; i++ {
		i := i
		go func() {
			texts, tokenCounts := generateTokens([]*models.Object{
				{Class: "Car", Properties: map[string]interface{}{"test": "wait 100"}},
			})

			vecs, errs := v.SubmitBatchAndWait(context.Background(), cfg, []bool{false}, tokenCounts, texts)
			require.Len(t, vecs, 1)
			require.Len(t, errs, 0)
			res <- i
			wg.Done()
		}()

		time.Sleep(100 * time.Millisecond) // the vectorizer waits for 100ms with processing the object, so it is sa
	}

	wg.Wait()
	close(res)
	// check that the batches were processed in the correct order
	for i := 0; i < 3; i++ {
		require.Equal(t, i, <-res)
	}
}

func TestBatchTimeouts(t *testing.T) {
	client := &fakeBatchClientWithRL[[]float32]{defaultResetRate: 1}
	cfg := &fakeClassConfig{vectorizePropertyName: false, classConfig: map[string]interface{}{"vectorizeClassName": false}}
	logger, _ := test.NewNullLogger()

	objs := []*models.Object{
		{Class: "Car", Properties: map[string]interface{}{"test": "tokens 18"}}, // first request, set rate down so the next two items can be sent
		{Class: "Car", Properties: map[string]interface{}{"test": "wait 200"}},  // second batch, use up batch time to trigger waiting for refresh
		{Class: "Car", Properties: map[string]interface{}{"test": "tokens 20"}}, // set next rate so the next object is too long. Depending on the total batch time it either sleeps or not
		{Class: "Car", Properties: map[string]interface{}{"test": "next batch long long long long long"}},
	}
	skip := []bool{false, false, false, false}

	cases := []struct {
		batchTime      time.Duration
		expectedErrors int
	}{
		{batchTime: 100 * time.Millisecond, expectedErrors: 1},
		{batchTime: 1 * time.Second, expectedErrors: 0},
	}
	for _, tt := range cases {
		t.Run(fmt.Sprint("BatchTimeouts", tt.batchTime), func(t *testing.T) {
			v := NewBatchVectorizer[[]float32](client, tt.batchTime, Settings{MaxObjectsPerBatch: 2000, MaxTokensPerBatch: maxTokensPerBatch, MaxTimePerBatch: 10, HasTokenLimit: true, ReturnsRateLimit: true}, logger, "test") // avoid waiting for rate limit

			texts, tokenCounts := generateTokens(objs)

			_, errs := v.SubmitBatchAndWait(context.Background(), cfg, skip, tokenCounts, texts)

			require.Len(t, errs, tt.expectedErrors)
		})
	}
}

func TestBatchRequestLimit(t *testing.T) {
	client := &fakeBatchClientWithRL[[]float32]{defaultResetRate: 1}
	cfg := &fakeClassConfig{vectorizePropertyName: false, classConfig: map[string]interface{}{"vectorizeClassName": false}}
	longString := "ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab"
	logger, _ := test.NewNullLogger()

	objs := []*models.Object{
		{Class: "Car", Properties: map[string]interface{}{"test": "requests 0"}},              // wait for the rate limit to reset
		{Class: "Car", Properties: map[string]interface{}{"test": "requests 0" + longString}}, // fill up default limit of 100 tokens
	}
	skip := []bool{false, false, false, false}
	texts, tokenCounts := generateTokens(objs)

	cases := []struct {
		batchTime      time.Duration
		expectedErrors int
	}{
		{batchTime: 100 * time.Millisecond, expectedErrors: 1},
		{batchTime: 2 * time.Second, expectedErrors: 0},
	}
	for _, tt := range cases {
		t.Run(fmt.Sprint("Test request limit with", tt.batchTime), func(t *testing.T) {
			v := NewBatchVectorizer[[]float32](client, tt.batchTime, Settings{MaxObjectsPerBatch: 2000, MaxTokensPerBatch: maxTokensPerBatch, MaxTimePerBatch: 10, HasTokenLimit: true, ReturnsRateLimit: true}, logger, "test") // avoid waiting for rate limit

			_, errs := v.SubmitBatchAndWait(context.Background(), cfg, skip, tokenCounts, texts)
			require.Len(t, errs, tt.expectedErrors)
		})
	}
}

func TestBatchTokenLimitZero(t *testing.T) {
	client := &fakeBatchClientWithRL[[]float32]{
		defaultResetRate: 1,
		defaultRPM:       500,
		// token limits are all 0
		rateLimit: &modulecomponents.RateLimits{RemainingRequests: 100, LimitRequests: 100},
	}
	cfg := &fakeClassConfig{vectorizePropertyName: false, classConfig: map[string]interface{}{"vectorizeClassName": false}}
	longString := "ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab"
	logger, _ := test.NewNullLogger()

	objs := []*models.Object{
		{Class: "Car", Properties: map[string]interface{}{"test": "tokens 0"}},
		{Class: "Car", Properties: map[string]interface{}{"test": "tokens 0" + longString}},
	}
	skip := []bool{false, false}
	texts, tokenCounts := generateTokens(objs)

	v := NewBatchVectorizer(client, time.Second, Settings{MaxObjectsPerBatch: 2000, MaxTokensPerBatch: maxTokensPerBatch, MaxTimePerBatch: 10, HasTokenLimit: true, ReturnsRateLimit: true}, logger, "test") // avoid waiting for rate limit

	_, errs := v.SubmitBatchAndWait(context.Background(), cfg, skip, tokenCounts, texts)
	require.Len(t, errs, 0)
	// finishes without hanging
}

func generateTokens(objects []*models.Object) ([]string, []int) {
	texts := make([]string, len(objects))
	tokenCounts := make([]int, len(objects))
	// prepare input for vectorizer, and send it to the queue. Prepare here to avoid work in the queue-worker
	for i := range objects {
		var text string
		props, ok := objects[i].Properties.(map[string]interface{})
		if ok {
			if v, ok2 := props["test"]; ok2 {
				text = v.(string)
			}
		}
		texts[i] = text
		tokenCounts[i] = len(text)
	}

	return texts, tokenCounts
}

func TestBatchRequestMissingRLValues(t *testing.T) {
	client := &fakeBatchClientWithRL[[]float32]{defaultResetRate: 1}
	cfg := &fakeClassConfig{vectorizePropertyName: false, classConfig: map[string]interface{}{"vectorizeClassName": false}}
	logger, _ := test.NewNullLogger()

	v := NewBatchVectorizer[[]float32](client, time.Second, Settings{MaxObjectsPerBatch: 2000, MaxTokensPerBatch: maxTokensPerBatch, MaxTimePerBatch: 10, HasTokenLimit: true, ReturnsRateLimit: true}, logger, "test") // avoid waiting for rate limit
	skip := []bool{false}

	start := time.Now()
	// normal batch
	objs := []*models.Object{
		{Class: "Car", Properties: map[string]interface{}{"test": "text"}},
	}
	texts, tokenCounts := generateTokens(objs)

	_, errs := v.SubmitBatchAndWait(context.Background(), cfg, skip, tokenCounts, texts)
	require.Len(t, errs, 0)

	// now batch with missing values, this should not cause any waiting or failures
	objs = []*models.Object{
		{Class: "Car", Properties: map[string]interface{}{"test": "missingValues "}}, // first request, set rate down so the next two items can be sent
	}
	texts, tokenCounts = generateTokens(objs)

	_, errs = v.SubmitBatchAndWait(context.Background(), cfg, skip, tokenCounts, texts)
	require.Len(t, errs, 0)

	// normal batch that is unaffected by the change
	objs = []*models.Object{
		{Class: "Car", Properties: map[string]interface{}{"test": "text"}},
	}
	texts, tokenCounts = generateTokens(objs)
	_, errs = v.SubmitBatchAndWait(context.Background(), cfg, skip, tokenCounts, texts)
	require.Len(t, errs, 0)
	// refresh rate is 1s. If the missing values would have any effect the batch algo would wait for the refresh to happen
	require.Less(t, time.Since(start), time.Millisecond*900)
}

// A batch that needs more vectorizer sub-requests than the rate-limit channel can
// buffer must not deadlock: the worker is blocked inside sendBatch and cannot drain
// the channel, so the send has to be non-blocking.
func TestMakeRequestDoesNotBlockWhenRateLimitChannelFull(t *testing.T) {
	logger, _ := test.NewNullLogger()
	cfg := &fakeClassConfig{classConfig: map[string]interface{}{"vectorizeClassName": false}}
	b := &Batch[[]float32]{
		client:            &fakeBatchClientWithRL[[]float32]{},
		rateLimitChannel:  make(chan rateLimitJob, BatchChannelSize),
		endOfBatchChannel: make(chan endOfBatchJob, BatchChannelSize),
		logger:            logger,
		settings:          Settings{MaxObjectsPerBatch: 2000, MaxTokensPerBatch: maxTokensPerBatch, MaxTimePerBatch: 10, HasTokenLimit: true, ReturnsRateLimit: true},
		Label:             "test",
	}

	// fill the channel to capacity so a blocking send would hang forever
	for i := 0; i < BatchChannelSize; i++ {
		b.rateLimitChannel <- rateLimitJob{}
	}

	job := BatchJob[[]float32]{ctx: context.Background(), cfg: cfg, errs: map[int]error{}, vecs: make([][]float32, 1)}

	done := make(chan struct{})
	go func() {
		b.makeRequest(job, []string{"text"}, cfg, []int{0}, &modulecomponents.RateLimits{}, 4)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("makeRequest blocked on a full rateLimitChannel")
	}
}

// fakeShortVectorClient returns fewer vectors than requested, mimicking a provider
// that responds with a truncated result.
type fakeShortVectorClient struct{}

func (c *fakeShortVectorClient) Vectorize(ctx context.Context, text []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	return &modulecomponents.VectorizationResult[[]float32]{
		Vector: make([][]float32, 0),
		Errors: make([]error, len(text)),
	}, nil, 0, nil
}

func (c *fakeShortVectorClient) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	return &modulecomponents.RateLimits{}
}

func (c *fakeShortVectorClient) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	return [32]byte{}
}

// A provider returning fewer vectors than inputs must produce an error for the
// missing objects instead of panicking on an out-of-range index.
func TestMakeRequestFewerVectorsThanTexts(t *testing.T) {
	logger, _ := test.NewNullLogger()
	cfg := &fakeClassConfig{classConfig: map[string]interface{}{"vectorizeClassName": false}}
	b := &Batch[[]float32]{
		client:            &fakeShortVectorClient{},
		rateLimitChannel:  make(chan rateLimitJob, BatchChannelSize),
		endOfBatchChannel: make(chan endOfBatchJob, BatchChannelSize),
		logger:            logger,
		Label:             "test",
	}
	job := BatchJob[[]float32]{ctx: context.Background(), cfg: cfg, errs: map[int]error{}, vecs: make([][]float32, 2)}

	require.NotPanics(t, func() {
		b.makeRequest(job, []string{"a", "b"}, cfg, []int{0, 1}, &modulecomponents.RateLimits{}, 2)
	})
	require.Error(t, job.errs[0])
	require.Error(t, job.errs[1])
}

// fakePanicClient panics for the text "panic" and vectorizes everything else. Its
// rate limit selects which scheduling path the panic happens on.
type fakePanicClient struct {
	rateLimit *modulecomponents.RateLimits
}

func (c *fakePanicClient) Vectorize(ctx context.Context, text []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	for i := range text {
		if text[i] == "panic" {
			panic("boom from vectorizer")
		}
	}
	vectors := make([][]float32, len(text))
	for i := range vectors {
		vectors[i] = []float32{0, 1, 2, 3}
	}
	return &modulecomponents.VectorizationResult[[]float32]{Vector: vectors, Errors: make([]error, len(text))}, nil, 0, nil
}

func (c *fakePanicClient) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	return c.rateLimit
}

func (c *fakePanicClient) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	return [32]byte{}
}

func submitWithTimeout(t *testing.T, v *Batch[[]float32], cfg moduletools.ClassConfig, skip []bool, tokens []int, texts []string) ([][]float32, map[int]error) {
	t.Helper()
	type result struct {
		vecs [][]float32
		errs map[int]error
	}
	ch := make(chan result, 1)
	go func() {
		vecs, errs := v.SubmitBatchAndWait(context.Background(), cfg, skip, tokens, texts)
		ch <- result{vecs, errs}
	}()
	select {
	case r := <-ch:
		return r.vecs, r.errs
	case <-time.After(5 * time.Second):
		t.Fatal("SubmitBatchAndWait did not return in time")
		return nil, nil
	}
}

// A panic while vectorizing one batch must not kill the worker: the panicking batch
// must return an error for its objects (not a silent nil vector), and subsequent
// batches must still be processed instead of blocking forever in SubmitBatchAndWait.
func TestBatchWorkerSurvivesPanic(t *testing.T) {
	logger, _ := test.NewNullLogger()
	cfg := &fakeClassConfig{classConfig: map[string]interface{}{"vectorizeClassName": false}}

	cases := []struct {
		name      string
		rateLimit *modulecomponents.RateLimits
	}{
		{
			// RemainingRequests != 0 skips the probe path; a request limit of 1 keeps
			// every batch above CanSendFullBatch's threshold, forcing the sequential
			// path where a panic runs inside the worker goroutine.
			name: "sequential path",
			rateLimit: &modulecomponents.RateLimits{
				RemainingRequests: 100, RemainingTokens: 100,
				LimitRequests: 1, LimitTokens: 1000,
				ResetRequests: time.Now().Add(time.Minute), ResetTokens: time.Now().Add(time.Minute),
			},
		},
		{
			// zero remaining requests/tokens make the rate limit look uninitialized, so
			// the worker takes the probe path and panics there, outside sendBatch.
			name:      "probe path",
			rateLimit: &modulecomponents.RateLimits{},
		},
		{
			// ample headroom lets CanSendFullBatch pass, so the batch is sent
			// concurrently and panics inside the GoWrapper goroutine.
			name: "concurrent path",
			rateLimit: &modulecomponents.RateLimits{
				RemainingRequests: 1000, RemainingTokens: 1000,
				LimitRequests: 1000, LimitTokens: 1000,
				ResetRequests: time.Now().Add(time.Minute), ResetTokens: time.Now().Add(time.Minute),
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			v := NewBatchVectorizer[[]float32](&fakePanicClient{rateLimit: tt.rateLimit}, 200*time.Millisecond,
				Settings{MaxObjectsPerBatch: 2000, MaxTokensPerBatch: maxTokensPerBatch, MaxTimePerBatch: 10}, logger, "test")

			texts, tokenCounts := generateTokens([]*models.Object{{Class: "Car", Properties: map[string]interface{}{"test": "panic"}}})
			_, errs := submitWithTimeout(t, v, cfg, []bool{false}, tokenCounts, texts)
			require.Error(t, errs[0])

			// the worker must have survived, so a normal batch still gets vectorized
			texts, tokenCounts = generateTokens([]*models.Object{{Class: "Car", Properties: map[string]interface{}{"test": "normal"}}})
			vecs, errs := submitWithTimeout(t, v, cfg, []bool{false}, tokenCounts, texts)
			require.Len(t, errs, 0)
			require.NotNil(t, vecs[0])
		})
	}
}

// SubmitBatchAndWait must not hang forever when a job never signals completion:
// once the request context is cancelled it returns, giving only unresolved
// (non-skipped) objects the context error and no vectors.
func TestSubmitBatchAndWaitRespectsContext(t *testing.T) {
	cases := []struct {
		name    string
		newCtx  func() (context.Context, context.CancelFunc)
		wantErr string
	}{
		{
			name: "cancel",
			newCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // already cancelled so the escape must fire on ctx.Done
				return ctx, cancel
			},
			wantErr: "context cancelled",
		},
		{
			name: "deadline",
			newCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 50*time.Millisecond)
			},
			wantErr: "context deadline exceeded",
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			cfg := &fakeClassConfig{classConfig: map[string]interface{}{"vectorizeClassName": false}}
			// No worker drains jobQueueCh, so the enqueued job never completes and
			// wg.Done never fires - only the context escape can unblock the call.
			b := &Batch[[]float32]{
				client:            &fakeBatchClientWithRL[[]float32]{},
				jobQueueCh:        make(chan BatchJob[[]float32], BatchChannelSize),
				rateLimitChannel:  make(chan rateLimitJob, BatchChannelSize),
				endOfBatchChannel: make(chan endOfBatchJob, BatchChannelSize),
				logger:            logger,
				Label:             "test",
			}

			ctx, cancel := tt.newCtx()
			defer cancel()

			done := make(chan struct{})
			var vecs [][]float32
			var errs map[int]error
			go func() {
				vecs, errs = b.SubmitBatchAndWait(ctx, cfg, []bool{false, true}, []int{1, 1}, []string{"a", "b"})
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("SubmitBatchAndWait did not return after context cancellation")
			}
			require.Len(t, vecs, 2)
			require.Nil(t, vecs[0])
			require.EqualError(t, errs[0], tt.wantErr)
			_, skipped := errs[1]
			require.False(t, skipped, "skipped object must not get an error")
		})
	}
}

func TestEncoderCache(t *testing.T) {
	cache := NewEncoderCache()

	modelString := "text-embedding-ada-002"
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			tke, err := tiktoken.EncodingForModel(modelString)
			require.NoError(t, err)
			cache.Set(modelString, tke)
			wg.Done()
		}()

		go func() {
			cache.Get(modelString)
			wg.Done()
		}()
	}

	wg.Wait()
}
