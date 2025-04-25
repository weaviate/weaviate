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
		{name: "deadline", deadline: 200 * time.Millisecond, objects: []*models.Object{
			{Class: "Car", Properties: map[string]interface{}{"test": "tokens 40"}}, // set limit so next two items are in a batch
			{Class: "Car", Properties: map[string]interface{}{"test": "wait 400"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "long long long long"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "next batch, will be aborted due to context deadline"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "skipped"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "has error again"}},
		}, skip: []bool{false, false, false, false, true, false}, wantErrors: map[int]error{3: fmt.Errorf("context deadline exceeded"), 5: fmt.Errorf("context deadline exceeded")}},
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
