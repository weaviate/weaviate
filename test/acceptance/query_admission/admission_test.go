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

package query_admission

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	className     = "AdmissionAcc"
	numObjects    = 3000
	numCategories = 10
	vectorDim     = 32
)

// searchShape is one admission-gated query shape. Both shapes take a grant at
// the shard: filtered BM25 through Shard.ObjectSearch, pure vector through
// Shard.ObjectVectorSearch (gated in full, filter or not). A pure-vector
// search holds its seat for far less time than a filtered BM25 one, so it
// gets a larger burst to guarantee enough overlap at budget=1/queue=1.
type searchShape struct {
	name   string
	req    *pb.SearchRequest
	burst  int
	sample int // moderate cross-node burst that must fit in budget+queue
}

func searchShapes() []searchShape {
	return []searchShape{
		{name: "filtered_bm25", req: filteredBM25Request(), burst: 50, sample: 2},
		{name: "pure_vector", req: pureVectorRequest(), burst: 100, sample: 2},
	}
}

// TestQueryAdmissionShedsUnderSaturation is journey 1: a saturated node under
// concurrent searches must shed via ResourceExhausted (429).
func TestQueryAdmissionShedsUnderSaturation(t *testing.T) {
	ctx := context.Background()
	compose, grpcClient := bootAdmission(t, ctx, false, 1, nil)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	for _, shape := range searchShapes() {
		t.Run(shape.name, func(t *testing.T) {
			res := burst(ctx, grpcClient, shape.req, shape.burst)
			t.Logf("journey1 saturation %s: success=%d shed=%d unexpected=%d of %d",
				shape.name, res.success, res.shed, res.other, shape.burst)
			require.NoError(t, res.otherErr, "responses must be success or ResourceExhausted only")
			require.Zero(t, res.other, "no unexpected error codes allowed")
			require.Positive(t, res.shed, "expected ResourceExhausted sheds under budget=1/queue=1 at %d concurrent", shape.burst)
			require.Positive(t, res.success, "expected at least one success to drain through the budget")
		})
	}
}

// TestQueryAdmissionDisabledNeverSheds is journey 2: the same tiny-budget burst
// with the kill switch engaged must never shed; admission is a passthrough.
func TestQueryAdmissionDisabledNeverSheds(t *testing.T) {
	ctx := context.Background()
	compose, grpcClient := bootAdmission(t, ctx, false, 1,
		map[string]string{"QUERY_ADMISSION_CONTROL_DISABLED": "true"})
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	for _, shape := range searchShapes() {
		t.Run(shape.name, func(t *testing.T) {
			res := burst(ctx, grpcClient, shape.req, shape.burst)
			t.Logf("journey2 disabled %s: success=%d shed=%d unexpected=%d of %d",
				shape.name, res.success, res.shed, res.other, shape.burst)
			require.NoError(t, res.otherErr, "responses must all succeed with admission disabled")
			require.Zero(t, res.other, "no unexpected error codes allowed")
			require.Zero(t, res.shed, "disabled admission must never shed")
			require.Equal(t, shape.burst, res.success, "every query must succeed with admission disabled")
		})
	}
}

// TestQueryAdmissionCrossNodeShed is journey 3: a 3-node, 3-shard cluster
// exercises the cross-node retry+shed path: moderate bursts succeed via
// retry, sustained bursts exhaust retries and surface ResourceExhausted.
func TestQueryAdmissionCrossNodeShed(t *testing.T) {
	ctx := context.Background()
	compose, grpcClient := bootAdmission(t, ctx, true, 3, nil)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	for _, shape := range searchShapes() {
		t.Run(shape.name, func(t *testing.T) {
			moderate := burst(ctx, grpcClient, shape.req, shape.sample)
			t.Logf("journey3 moderate %s: success=%d shed=%d unexpected=%d of %d",
				shape.name, moderate.success, moderate.shed, moderate.other, shape.sample)
			require.NoError(t, moderate.otherErr)
			require.Zero(t, moderate.other, "no unexpected error codes allowed")
			require.Equal(t, shape.sample, moderate.success, "moderate cross-node burst must succeed via the retry path")

			sustained := burst(ctx, grpcClient, shape.req, shape.burst)
			t.Logf("journey3 sustained %s: success=%d shed=%d unexpected=%d of %d",
				shape.name, sustained.success, sustained.shed, sustained.other, shape.burst)
			require.NoError(t, sustained.otherErr)
			require.Zero(t, sustained.other, "no unexpected error codes allowed")
			require.Positive(t, sustained.shed, "sustained cross-node saturation must surface ResourceExhausted")
		})
	}
}

// --- helpers ----------------------------------------------------------------

// bootAdmission starts Weaviate with the shared tiny budget (BUDGET=1,
// MAX_QUEUE=1) plus extraEnv. cluster selects a 3-node topology so queries
// fan out to remote shards; otherwise a single node.
func bootAdmission(t *testing.T, ctx context.Context, cluster bool, shards int,
	extraEnv map[string]string,
) (*docker.DockerCompose, pb.WeaviateClient) {
	t.Helper()
	builder := docker.New()
	if cluster {
		builder = builder.WithWeaviateClusterWithGRPC()
	} else {
		builder = builder.WithWeaviateWithGRPC()
	}
	builder = builder.
		WithWeaviateEnv("QUERY_ADMISSION_BUDGET", "1").
		WithWeaviateEnv("QUERY_ADMISSION_MAX_QUEUE", "1")
	for name, value := range extraEnv {
		builder = builder.WithWeaviateEnv(name, value)
	}
	compose, err := builder.Start(ctx)
	require.NoError(t, err)

	grpcClient := setupAdmissionCollection(t, compose.GetWeaviate().URI(),
		compose.GetWeaviate().GrpcURI(), shards)
	return compose, grpcClient
}

func setupAdmissionCollection(t *testing.T, httpURI, grpcURI string, shards int) pb.WeaviateClient {
	t.Helper()
	helper.SetupClient(httpURI)
	t.Cleanup(helper.ResetClient)

	conn, err := helper.CreateGrpcConnectionClient(grpcURI)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	grpcClient := helper.CreateGrpcWeaviateClient(conn)

	vFalse, vTrue := false, true
	class := &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:            "text",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "category",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationField,
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
			},
		},
		ShardingConfig: map[string]interface{}{"desiredCount": float64(shards)},
	}
	// No DeleteClass cleanup: the deferred compose.Terminate runs before any
	// t.Cleanup and destroys the container, so DeleteClass would race a dead one.
	helper.CreateClass(t, class)

	const chunk = 500
	batch := make([]*models.Object, 0, chunk)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		helper.CreateObjectsBatch(t, batch)
		batch = batch[:0]
	}
	for i := 0; i < numObjects; i++ {
		batch = append(batch, &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"text":     fmt.Sprintf("alpha beta gamma document number %d", i),
				"category": fmt.Sprintf("cat_%d", i%numCategories),
			},
			Vector: objectVector(i),
		})
		if len(batch) == chunk {
			flush()
		}
	}
	flush()

	return grpcClient
}

// objectVector returns a deterministic, non-degenerate vector for object i so
// the vector index has real structure to search without pulling in a RNG.
func objectVector(i int) models.C11yVector {
	vec := make(models.C11yVector, vectorDim)
	for d := range vec {
		vec[d] = float32((i*31+d*17)%97) / 97
	}
	return vec
}

// filteredBM25Request builds a filter+BM25 search that exercises the
// admission-gated shard path (filters != nil && keywordRanking != nil).
func filteredBM25Request() *pb.SearchRequest {
	return &pb.SearchRequest{
		Collection: className,
		Limit:      10,
		Bm25Search: &pb.BM25{Query: "alpha", Properties: []string{"text"}},
		Filters: &pb.Filters{
			Operator:  pb.Filters_OPERATOR_EQUAL,
			TestValue: &pb.Filters_ValueText{ValueText: "cat_0"},
			Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "category"}},
		},
		Uses_127Api: true,
	}
}

// pureVectorRequest builds a filterless nearVector search: no allow-list, no
// keyword phase, so the only grant it can take is the one ObjectVectorSearch
// holds for the vector phase itself.
func pureVectorRequest() *pb.SearchRequest {
	return &pb.SearchRequest{
		Collection:  className,
		Limit:       100,
		NearVector:  &pb.NearVector{Vector: objectVector(numObjects / 2)},
		Uses_127Api: true,
	}
}

type burstResult struct {
	success  int
	shed     int
	other    int
	otherErr error
}

// burst fires `concurrency` identical gRPC searches at once and classifies each
// response as success, ResourceExhausted (shed), or an unexpected error.
func burst(ctx context.Context, grpcClient pb.WeaviateClient, req *pb.SearchRequest, concurrency int) burstResult {
	var (
		wg       sync.WaitGroup
		success  atomic.Int64
		shed     atomic.Int64
		other    atomic.Int64
		otherErr atomic.Value
	)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			_, err := grpcClient.Search(cctx, req)
			switch {
			case err == nil:
				success.Add(1)
			case status.Code(err) == codes.ResourceExhausted:
				shed.Add(1)
			default:
				other.Add(1)
				otherErr.Store(err.Error())
			}
		}()
	}
	wg.Wait()

	res := burstResult{
		success: int(success.Load()),
		shed:    int(shed.Load()),
		other:   int(other.Load()),
	}
	if v := otherErr.Load(); v != nil {
		res.otherErr = fmt.Errorf("unexpected gRPC error: %v", v)
	}
	return res
}
