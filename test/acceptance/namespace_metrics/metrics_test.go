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

package namespace_metrics

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/byteops"
)

const (
	adminUser = "admin-user"
	adminKey  = "admin-key"

	// metricsPort is the in-container prometheus port; the compose does not
	// publish it, so every scrape runs through docker exec.
	metricsPort = 2112

	// class is created once per namespace under its short name; the resolver
	// qualifies it to "<ns>:Docs".
	class = "Docs"

	// objectCount and vectorDim fix the expected gauge values: 5 objects of 4
	// dimensions is 20 dimensions per shard.
	objectCount = 5
	vectorDim   = 4
)

var sharedCompose *docker.DockerCompose

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithApiKey().
		WithRBAC().
		WithUserApiKey(adminUser, adminKey).
		WithRbacRoots(adminUser).
		WithDbUsers().
		WithNamespaces().
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true").
		WithWeaviateEnv("TRACK_VECTOR_DIMENSIONS", "true").
		// The observer publishes the two dimension gauges on this tick; the
		// 5m default would outlast the test.
		WithWeaviateEnv("TRACK_VECTOR_DIMENSIONS_INTERVAL", "2s").
		WithWeaviateWithGRPC().
		Start(ctx)
	if err != nil {
		panic(errors.Wrap(err, "failed to start shared compose"))
	}
	sharedCompose = compose

	helper.SetupClient(compose.GetWeaviate().URI())

	code := m.Run()

	if err := compose.Terminate(ctx); err != nil {
		panic(errors.Wrap(err, "failed to terminate shared compose"))
	}
	os.Exit(code)
}

// execOutputSentinel absorbs the docker stream framing bytes that prefix each
// exec frame, so the lines the caller cares about arrive clean.
const execOutputSentinel = "___EXEC_BEGIN___"

// Every probe helper reports through assert.TestingT and a bool rather than
// require, because they all run inside EventuallyWithT callbacks: a FailNow
// there fires on a goroutine that is not the test's own.
func execInContainer(t assert.TestingT, c testcontainers.Container, cmd string) (string, bool) {
	code, reader, err := c.Exec(context.Background(), []string{"sh", "-c", "echo " + execOutputSentinel + "; " + cmd})
	if !assert.NoError(t, err, "exec %q", cmd) {
		return "", false
	}
	out, err := io.ReadAll(reader)
	if !assert.NoError(t, err) {
		return "", false
	}
	if !assert.Zero(t, code, "exec %q failed: %s", cmd, string(out)) {
		return "", false
	}
	if idx := strings.Index(string(out), execOutputSentinel); idx >= 0 {
		return string(out)[idx+len(execOutputSentinel):], true
	}
	return string(out), true
}

// scrape returns the metrics lines matching pattern. The whole page is far too
// large to survive an exec stream intact, so it is filtered in the container.
// A line count comes back with it, so an unreachable endpoint fails loudly
// instead of reading as "no such series".
func scrape(t assert.TestingT, pattern string) ([]string, bool) {
	out, ok := execInContainer(t, sharedCompose.GetWeaviate().Container(), fmt.Sprintf(
		"wget -qO- http://localhost:%d/metrics > /tmp/metrics.probe 2>/dev/null; "+
			"printf 'LINES=%%s\\n' \"$(wc -l < /tmp/metrics.probe)\"; "+
			"grep -F '%s' /tmp/metrics.probe || true",
		metricsPort, pattern))
	if !ok {
		return nil, false
	}

	var matched []string
	sawPage := false
	for _, line := range strings.Split(out, "\n") {
		line = cleanExecLine(line)
		if strings.HasPrefix(line, "LINES=") {
			pageLines, err := strconv.Atoi(strings.TrimPrefix(line, "LINES="))
			if !assert.NoError(t, err, "metrics probe returned no line count: %q", out) {
				return nil, false
			}
			if !assert.Positive(t, pageLines,
				"the metrics endpoint on port %d served nothing; this test needs "+
					"PROMETHEUS_MONITORING_ENABLED on the compose", metricsPort) {
				return nil, false
			}
			sawPage = true
			continue
		}
		if line != "" {
			matched = append(matched, line)
		}
	}
	if !assert.True(t, sawPage, "metrics probe returned no line count: %q", out) {
		return nil, false
	}
	return matched, true
}

func cleanExecLine(line string) string {
	return strings.TrimSpace(strings.Map(func(r rune) rune {
		if r < 0x20 {
			return -1
		}
		return r
	}, line))
}

// sampleValue returns the value of the single series whose /metrics line
// contains every fragment. It reports not-ok when no line matches, and fails
// when more than one does, so a scrape that silently changes shape cannot pass.
func sampleValue(t assert.TestingT, lines []string, fragments ...string) (float64, bool) {
	var hits []string
	for _, line := range lines {
		if containsAll(line, fragments...) {
			hits = append(hits, line)
		}
	}
	if len(hits) == 0 {
		return 0, false
	}
	if !assert.Len(t, hits, 1, "expected exactly one series for %v, got %v", fragments, hits) {
		return 0, false
	}
	idx := strings.LastIndex(hits[0], " ")
	if !assert.Positive(t, idx, "malformed exposition line %q", hits[0]) {
		return 0, false
	}
	value, err := strconv.ParseFloat(hits[0][idx+1:], 64)
	if !assert.NoError(t, err, "malformed exposition line %q", hits[0]) {
		return 0, false
	}
	return value, true
}

func containsAll(line string, fragments ...string) bool {
	for _, f := range fragments {
		if !strings.Contains(line, f) {
			return false
		}
	}
	return true
}

func nsLabel(ns string) string {
	return fmt.Sprintf(`collection_namespace="%s"`, ns)
}

func authCtx(key string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+key)
}

func newGrpcClient(t *testing.T) (pb.WeaviateClient, *grpc.ClientConn) {
	t.Helper()
	conn, err := helper.CreateGrpcConnectionClient(sharedCompose.GetWeaviate().GrpcURI())
	require.NoError(t, err)
	return helper.CreateGrpcWeaviateClient(conn), conn
}

// newNamespacedUser creates a user inside ns and grants it the built-in admin
// role, whose wildcard templates the RBAC matcher narrows to the caller's
// own namespace. Without the grant every schema and data call 403s.
func newNamespacedUser(t *testing.T, userID, ns string) string {
	t.Helper()
	key := helper.CreateUserWithNamespace(t, userID, ns, adminKey)
	helper.AssignRoleToUser(t, adminKey, authorization.Admin, ns+":"+userID)
	helper.WaitForOwnRole(t, key, authorization.Admin)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":"+userID, adminKey) })
	return key
}

func fixedVector() []float32 {
	vec := make([]float32, vectorDim)
	for i := range vec {
		vec[i] = float32(i+1) / 10
	}
	return vec
}

// TestNamespaceMetrics proves that a namespaced cluster attributes all five
// platform metrics per namespace, and that a namespace's series do not
// outlive it.
func TestNamespaceMetrics(t *testing.T) {
	const nsA, nsB = "ns-a", "ns-b"

	helper.CreateNamespace(t, nsA, adminKey)
	helper.CreateNamespace(t, nsB, adminKey)
	userAKey := newNamespacedUser(t, "user-a", nsA)
	userBKey := newNamespacedUser(t, "user-b", nsB)

	grpcClient, conn := newGrpcClient(t)
	defer conn.Close()

	// vectorizer "none" keeps the vectors the test supplies, so the dimension
	// gauges are exactly objectCount * vectorDim.
	for _, key := range []string{userAKey, userBKey} {
		helper.CreateClassAuth(t, &models.Class{
			Class:      class,
			Vectorizer: "none",
			Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}},
		}, key)
	}

	// ns-a ingests over REST, ns-b over gRPC, so each API contributes one
	// batch_size_bytes sample under a different namespace.
	restObjects := make([]*models.Object, objectCount)
	for i := range restObjects {
		restObjects[i] = &models.Object{
			Class:      class,
			ID:         strfmt.UUID(fmt.Sprintf("aaaaaaaa-0000-0000-0000-%012d", i)),
			Properties: map[string]any{"title": fmt.Sprintf("a-%d", i)},
			Vector:     fixedVector(),
		}
	}
	helper.CreateObjectsBatchAuth(t, restObjects, userAKey)

	grpcObjects := make([]*pb.BatchObject, objectCount)
	for i := range grpcObjects {
		grpcObjects[i] = &pb.BatchObject{
			Uuid:       fmt.Sprintf("bbbbbbbb-0000-0000-0000-%012d", i),
			Collection: class,
			Properties: &pb.BatchObject_Properties{
				NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{
					"title": structpb.NewStringValue(fmt.Sprintf("b-%d", i)),
				}},
			},
			Vectors: []*pb.Vectors{{
				Name:        "default",
				Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
				VectorBytes: byteops.Fp32SliceToBytes(fixedVector()),
			}},
		}
	}
	batchReply, err := grpcClient.BatchObjects(authCtx(userBKey), &pb.BatchObjectsRequest{Objects: grpcObjects})
	require.NoError(t, err)
	require.Empty(t, batchReply.Errors)

	// A gRPC Search reaches traverser.GetClass, which is the only writer of
	// queries_durations_ms.
	for _, key := range []string{userAKey, userBKey} {
		_, err := grpcClient.Search(authCtx(key), &pb.SearchRequest{
			Collection:  class,
			Limit:       objectCount,
			Uses_123Api: true,
			Uses_125Api: true,
			Uses_127Api: true,
		})
		require.NoError(t, err)
	}

	// An unfiltered Aggregate{ObjectsCount} reaches Bucket.Count, which writes
	// the object_count gauge synchronously.
	for _, key := range []string{userAKey, userBKey} {
		resp, err := grpcClient.Aggregate(authCtx(key), &pb.AggregateRequest{
			Collection:   class,
			ObjectsCount: true,
		})
		require.NoError(t, err)
		require.Equal(t, int64(objectCount), resp.GetSingleResult().GetObjectsCount())
	}

	t.Run("object_count per namespace", func(t *testing.T) {
		for _, ns := range []string{nsA, nsB} {
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				lines, ok := scrape(c, "object_count{")
				if !ok {
					return
				}
				got, ok := sampleValue(c, lines, `class_name="`+ns+`:`+class+`"`, nsLabel(ns))
				if !assert.True(c, ok, "no object_count series for %q", ns) {
					return
				}
				assert.Equal(c, float64(objectCount), got)
			}, 10*time.Second, 250*time.Millisecond)
		}
	})

	t.Run("vector_dimensions_sum and vector_segments_sum per namespace", func(t *testing.T) {
		for _, ns := range []string{nsA, nsB} {
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				lines, ok := scrape(c, `collection_namespace="`+ns+`"`)
				if !ok {
					return
				}
				dims, ok := sampleValue(c, lines, "vector_dimensions_sum{", nsLabel(ns))
				if !assert.True(c, ok, "no vector_dimensions_sum series for %q", ns) {
					return
				}
				assert.Equal(c, float64(objectCount*vectorDim), dims)

				segs, ok := sampleValue(c, lines, "vector_segments_sum{", nsLabel(ns))
				if !assert.True(c, ok, "no vector_segments_sum series for %q", ns) {
					return
				}
				assert.Zero(c, segs, "no quantization is configured")
			}, 15*time.Second, 500*time.Millisecond)
		}
	})

	t.Run("queries_durations_ms per namespace", func(t *testing.T) {
		for _, ns := range []string{nsA, nsB} {
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				lines, ok := scrape(c, "queries_durations_ms_count{")
				if !ok {
					return
				}
				got, ok := sampleValue(c, lines, `class_name="`+ns+`:`+class+`"`,
					`query_type="get_graphql"`, nsLabel(ns))
				if !assert.True(c, ok, "no queries_durations_ms_count series for %q", ns) {
					return
				}
				assert.GreaterOrEqual(c, got, 1.0)
			}, 10*time.Second, 250*time.Millisecond)
		}
	})

	t.Run("batch_size_bytes per namespace and api", func(t *testing.T) {
		tests := []struct {
			name string
			api  string
			ns   string
		}{
			{name: "rest batch under ns-a", api: "rest", ns: nsA},
			{name: "grpc batch under ns-b", api: "grpc", ns: nsB},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					lines, ok := scrape(c, "batch_size_bytes_")
					if !ok {
						return
					}
					count, ok := sampleValue(c, lines, "batch_size_bytes_count{",
						`api="`+tc.api+`"`, nsLabel(tc.ns))
					if !assert.True(c, ok, "no batch_size_bytes_count series for %q", tc.ns) {
						return
					}
					assert.Equal(c, 1.0, count)

					sum, ok := sampleValue(c, lines, "batch_size_bytes_sum{",
						`api="`+tc.api+`"`, nsLabel(tc.ns))
					if !assert.True(c, ok) {
						return
					}
					assert.Positive(c, sum)
				}, 10*time.Second, 250*time.Millisecond)
			})
		}
	})

	t.Run("global operator samples carry empty namespace", func(t *testing.T) {
		// The admin static key is a global operator, which has no namespace.
		// Its class is qualified explicitly because the resolver adds no prefix
		// for an unconfined caller.
		helper.CreateObjectsBatchAuth(t, []*models.Object{{
			Class:      nsA + ":" + class,
			ID:         strfmt.UUID("cccccccc-0000-0000-0000-000000000000"),
			Properties: map[string]any{"title": "operator"},
			Vector:     fixedVector(),
		}}, adminKey)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			lines, ok := scrape(c, "batch_size_bytes_count{")
			if !ok {
				return
			}
			got, ok := sampleValue(c, lines, `api="rest"`, nsLabel(""))
			if !assert.True(c, ok, "no empty-namespace batch_size_bytes_count series") {
				return
			}
			assert.GreaterOrEqual(c, got, 1.0)
		}, 10*time.Second, 250*time.Millisecond)
	})

	t.Run("deleting a namespace drops its per-namespace series", func(t *testing.T) {
		helper.DeleteNamespace(t, nsA, adminKey, helper.WithoutWaitForCleanup())
		helper.WaitForNamespaceGone(t, nsA, adminKey, 60*time.Second)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			lines, ok := scrape(c, `collection_namespace="`+nsA+`"`)
			if !ok {
				return
			}

			for _, metric := range []string{"object_count{", "queries_durations_ms_", "batch_size_bytes_"} {
				for _, line := range lines {
					assert.NotContains(c, line, metric,
						"%s must not outlive the namespace", metric)
				}
			}
		}, 30*time.Second, 500*time.Millisecond)

		// The surviving namespace is untouched.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			lines, ok := scrape(c, `collection_namespace="`+nsB+`"`)
			if !ok {
				return
			}
			_, ok = sampleValue(c, lines, "object_count{", nsLabel(nsB))
			assert.True(c, ok, "deleting one namespace must not touch another's series")
		}, 10*time.Second, 250*time.Millisecond)
	})

	// Runs on the namespace the subtest above deleted: the two gauges are the
	// one thing its removal must leave behind.
	t.Run("the dimension gauges are kept at zero for billing", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			lines, ok := scrape(c, `collection_namespace="`+nsA+`"`)
			if !ok {
				return
			}

			dims, ok := sampleValue(c, lines, "vector_dimensions_sum{", nsLabel(nsA))
			if !assert.True(c, ok, "vector_dimensions_sum is retained for billing") {
				return
			}
			assert.Zero(c, dims)

			segs, ok := sampleValue(c, lines, "vector_segments_sum{", nsLabel(nsA))
			if !assert.True(c, ok, "vector_segments_sum is retained for billing") {
				return
			}
			assert.Zero(c, segs)
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Cleanup(func() {
		helper.DeleteNamespace(t, nsB, adminKey)
	})
}
