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

package vectorizer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents/batch"
	"github.com/weaviate/weaviate/usecases/modulecomponents/text2vecbase"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

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
			{Class: "Car", Properties: map[string]interface{}{"test": "tokens 50"}}, // set limit so next 3 objects are one batch
			{Class: "Car", Properties: map[string]interface{}{"test": "first object first batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "second object first batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "third object first batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "tokens 100"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "first object second batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "second object second batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "third object second batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "fourth object second batch"}},
		}, skip: []bool{false, false, false, false, false, false, false, false, false}},
		{name: "multiple vectorizer batches with skips and errors", objects: []*models.Object{
			{Class: "Car", Properties: map[string]interface{}{"test": "tokens 50"}}, // set limit so next 3 objects are one batch
			{Class: "Car", Properties: map[string]interface{}{"test": "first object first batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "second object first batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "error something"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "tokens 100"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "first object second batch"}}, // rate is 100 again
			{Class: "Car", Properties: map[string]interface{}{"test": "second object second batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "third object second batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "fourth object second batch"}},
		}, skip: []bool{false, true, false, false, false, false, true, false, false}, wantErrors: map[int]error{3: fmt.Errorf("something")}},
		{name: "token too long", objects: []*models.Object{
			{Class: "Car", Properties: map[string]interface{}{"test": "tokens 5"}}, // set limit
			{Class: "Car", Properties: map[string]interface{}{"test": "long long long long, long, long, long, long"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "short"}},
		}, skip: []bool{false, false, false}, wantErrors: map[int]error{1: fmt.Errorf("text too long for vectorization from provider: got 43, total limit: 10, remaining: 5")}},
		{name: "token too long, last item in batch", objects: []*models.Object{
			{Class: "Car", Properties: map[string]interface{}{"test": "tokens 5"}}, // set limit
			{Class: "Car", Properties: map[string]interface{}{"test": "short"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "long long long long, long, long, long, long"}},
		}, skip: []bool{false, false, false}, wantErrors: map[int]error{2: fmt.Errorf("text too long for vectorization from provider: got 43, total limit: 10, remaining: 5")}},
		{name: "skip last item", objects: []*models.Object{
			{Class: "Car", Properties: map[string]interface{}{"test": "fir test object"}}, // set limit
			{Class: "Car", Properties: map[string]interface{}{"test": "first object first batch"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "second object first batch"}},
		}, skip: []bool{false, false, true}},
		{name: "deadline", deadline: 400 * time.Millisecond, objects: []*models.Object{
			{Class: "Car", Properties: map[string]interface{}{"test": "tokens 15"}}, // set limit so next two items are in a batch
			{Class: "Car", Properties: map[string]interface{}{"test": "wait 500"}},  // needs to be higher than deadline, so all remaining objects time out
			{Class: "Car", Properties: map[string]interface{}{"test": "long long long"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "next batch, will be aborted due to context deadline"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "skipped"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "has error again"}},
		}, skip: []bool{false, false, false, false, true, false}, wantErrors: map[int]error{3: fmt.Errorf("context deadline exceeded"), 5: fmt.Errorf("context deadline exceeded")}},
		{name: "azure limit without total Limit", objects: []*models.Object{
			{Class: "Car", Properties: map[string]interface{}{"test": "azure_tokens 20"}}, // set azure limit without total Limit
			{Class: "Car", Properties: map[string]interface{}{"test": "long long long long"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "azure_tokens 0"}}, // simulate token limit hit
			{Class: "Car", Properties: map[string]interface{}{"test": "something"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "skipped"}},
			{Class: "Car", Properties: map[string]interface{}{"test": "all works"}},
		}, skip: []bool{false, false, false, false, true, false}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeBatchClient{}

			v := text2vecbase.New(client,
				batch.NewBatchVectorizer(client, 50*time.Second,
					batch.Settings{MaxObjectsPerBatch: 100, MaxTokensPerBatch: func(cfg moduletools.ClassConfig) int { return 500000 }, MaxTimePerBatch: 10, HasTokenLimit: true, ReturnsRateLimit: true},
					logger, "test"),
				batch.ReturnBatchTokenizer(1, "", false),
			)
			deadline := time.Now().Add(10 * time.Second)
			if tt.deadline != 0 {
				deadline = time.Now().Add(tt.deadline)
			}

			ctx, cancl := context.WithDeadline(context.Background(), deadline)
			vecs, errs := v.ObjectBatch(
				ctx, tt.objects, tt.skip, cfg,
			)

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

func BenchmarkEncoderCache(b *testing.B) {
	client := &fakeBatchClient{}
	logger, _ := test.NewNullLogger()

	v := text2vecbase.New(client,
		batch.NewBatchVectorizer(client, 50*time.Second,
			batch.Settings{MaxObjectsPerBatch: 100, MaxTokensPerBatch: func(cfg moduletools.ClassConfig) int { return 500000 }, MaxTimePerBatch: 10, HasTokenLimit: true, ReturnsRateLimit: true},
			logger, "test"),
		batch.ReturnBatchTokenizer(1, "", false),
	)
	deadline := time.Now().Add(10 * time.Second)
	cfg := &fakeClassConfig{vectorizePropertyName: false, classConfig: map[string]interface{}{"vectorizeClassName": false}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancl := context.WithDeadline(context.Background(), deadline)
		vecs, errs := v.ObjectBatch(
			ctx, []*models.Object{{Class: "Car"}}, []bool{false}, cfg,
		)
		cancl()
		require.Len(b, errs, 0)
		require.Len(b, vecs, 1)
	}
}
