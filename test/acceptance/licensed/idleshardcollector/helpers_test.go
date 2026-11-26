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

// Package idleshardcollector holds the end-to-end tests of the licensed idle shard
// collector: every knob is set through the runtime overrides file, loaded
// shards are read from the unloader's gauges, and every workload checks that
// no request fails and no write is lost while shards unload.
package idleshardcollector

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"golang.org/x/sync/errgroup"

	apiclient "github.com/weaviate/weaviate/client"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/docker"
)

const (
	overridesPath = "/etc/weaviate/runtime-overrides.yaml"
	metricsURL    = "http://127.0.0.1:2112/metrics"
	metricLoaded  = "weaviate_idle_shard_collector_loaded_shards"
	metricUnloads = "weaviate_idle_shard_collector_unloads_total"

	// idleTTL leaves each test time to see its shards loaded before they unload.
	idleTTL = "30s"
	// listLimit is the server's default QUERY_MAXIMUM_RESULTS; a truncated listing fails verify on a missing acknowledged ID.
	listLimit = 10_000
	// gaugeStable outlasts one publish of the loaded gauge, so a value published before the last change cannot satisfy a wait.
	gaugeStable = 1500 * time.Millisecond
)

// overrides makes the collector check every second; an empty idle ttl leaves idle unloading off.
func overrides(ttl string) []byte {
	y := "idle_shard_collector_check_interval: 1s\n"
	if ttl != "" {
		y += "idle_shard_collector_idle_ttl: " + ttl + "\n"
	}
	return []byte(y)
}

// newCompose is a cluster whose knobs come only from the runtime overrides file.
func newCompose(t *testing.T, licensed bool, nodeCount int, ttl string) *docker.Compose {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime-overrides.yaml")
	require.NoError(t, os.WriteFile(path, overrides(ttl), 0o644))
	b := docker.New().
		WithWeaviateCluster(nodeCount).
		WithWeaviateEnv("RUNTIME_OVERRIDES_ENABLED", "true").
		WithWeaviateEnv("RUNTIME_OVERRIDES_PATH", overridesPath).
		WithWeaviateEnv("RUNTIME_OVERRIDES_LOAD_INTERVAL", "1s").
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true").
		WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").
		WithWeaviateEnv("LOG_LEVEL", "info").
		WithWeaviateFiles(testcontainers.ContainerFile{HostFilePath: path, ContainerFilePath: overridesPath, FileMode: 0o644})
	if licensed {
		b = b.WithWeaviateLicense()
	}
	return b
}

// start brings the compose up and tears it down with the test, dumping logs on failure.
func start(ctx context.Context, t *testing.T, b *docker.Compose) *docker.DockerCompose {
	t.Helper()
	compose, err := b.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if t.Failed() {
			compose.DumpWeaviateLogs(context.Background(), &testLogWriter{t: t}, 150)
		}
		if err := compose.Terminate(context.Background()); err != nil {
			t.Errorf("terminate test containers: %v", err)
		}
	})
	return compose
}

type testLogWriter struct{ t *testing.T }

func (w *testLogWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

func newClient(uri string) *apiclient.Weaviate {
	return apiclient.New(httptransport.New(uri, "/v1", []string{"http"}), strfmt.Default)
}

// setIdleTTL rewrites the overrides file on every node; callers wait on the gauge for the reload.
func setIdleTTL(ctx context.Context, t *testing.T, compose *docker.DockerCompose, nodeCount int, ttl string) {
	t.Helper()
	for i := 1; i <= nodeCount; i++ {
		require.NoError(t, compose.GetWeaviateNode(i).Container().CopyToContainer(ctx, overrides(ttl), overridesPath, 0o644))
	}
}

// metric sums every series of name whose line contains label; ok is false while no such series exists.
func metric(ctx context.Context, c *docker.DockerContainer, name, label string) (value float64, ok bool, err error) {
	code, reader, err := c.Container().Exec(ctx,
		[]string{"sh", "-c", fmt.Sprintf("wget -qO- %s | grep -E '^%s( |\\{)' || true", metricsURL, name)},
		tcexec.Multiplexed())
	if err != nil {
		return 0, false, err
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		return 0, false, err
	}
	if code != 0 {
		return 0, false, fmt.Errorf("scrape %s: exit %d: %s", name, code, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" || !strings.Contains(line, label) {
			continue
		}
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			return 0, false, fmt.Errorf("parse %q: %w", line, err)
		}
		value += v
		ok = true
	}
	return value, ok, nil
}

// eventuallyLoaded waits until every node's loaded gauge has read want for gaugeStable.
func eventuallyLoaded(ctx context.Context, t *testing.T, compose *docker.DockerCompose, nodeCount, want int, msg string) {
	t.Helper()
	var heldSince time.Time
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for i := 1; i <= nodeCount; i++ {
			n, published, err := metric(ctx, compose.GetWeaviateNode(i), metricLoaded, "")
			held := err == nil && published && int(n) == want
			if !held {
				heldSince = time.Time{}
			}
			require.NoError(ct, err)
			require.True(ct, held, "node %d has %v loaded shards (published %v), want %d", i, n, published, want)
		}
		if heldSince.IsZero() {
			heldSince = time.Now()
		}
		require.GreaterOrEqual(ct, time.Since(heldSince), gaugeStable, "the loaded count has not held for a full publish interval")
	}, 2*time.Minute, 250*time.Millisecond, msg)
}

// loadedShards reads node's shard load states from the verbose nodes API, which pins every loaded shard it reports.
func loadedShards(ctx context.Context, cl *apiclient.Weaviate, class, node string) (map[string]bool, error) {
	out := verbosity.OutputVerbose
	resp, err := cl.Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithContext(ctx).WithClassName(class).WithOutput(&out), nil)
	if err != nil {
		return nil, err
	}
	shards := map[string]bool{}
	for _, n := range resp.Payload.Nodes {
		for _, s := range n.Shards {
			if n.Name == node {
				shards[s.Name] = s.Loaded
			}
		}
	}
	return shards, nil
}

func tenantNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("t%04d", i)
	}
	return names
}

func vectorClass(name string, multiTenant bool, rf int64) *models.Class {
	c := &models.Class{
		Class:      name,
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "owner", DataType: []string{"text"}},
			{Name: "seq", DataType: []string{"int"}},
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: rf},
	}
	if multiTenant {
		c.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	} else {
		c.ShardingConfig = map[string]any{"desiredCount": 1}
	}
	return c
}

func createClass(ctx context.Context, t *testing.T, cl *apiclient.Weaviate, c *models.Class) {
	t.Helper()
	_, err := cl.Schema.SchemaObjectsCreate(schema.NewSchemaObjectsCreateParams().WithContext(ctx).WithObjectClass(c), nil)
	require.NoError(t, err, "create class %s", c.Class)
}

func createTenants(ctx context.Context, t *testing.T, cl *apiclient.Weaviate, class string, names []string) {
	t.Helper()
	for chunk := range slices.Chunk(names, 20) {
		tenants := make([]*models.Tenant, len(chunk))
		for i, n := range chunk {
			tenants[i] = &models.Tenant{Name: n, ActivityStatus: models.TenantActivityStatusHOT}
		}
		_, err := cl.Schema.TenantsCreate(schema.NewTenantsCreateParams().WithContext(ctx).WithClassName(class).WithBody(tenants), nil)
		require.NoError(t, err)
	}
}

func setTenantStatus(ctx context.Context, cl *apiclient.Weaviate, class, status string, names ...string) error {
	tenants := make([]*models.Tenant, len(names))
	for i, n := range names {
		tenants[i] = &models.Tenant{Name: n, ActivityStatus: status}
	}
	_, err := cl.Schema.TenantsUpdate(schema.NewTenantsUpdateParams().WithContext(ctx).WithClassName(class).WithBody(tenants), nil)
	return err
}

// requireAllHot fails unless the class has want tenants, every one HOT.
func requireAllHot(ctx context.Context, t *testing.T, cl *apiclient.Weaviate, class string, want int) {
	t.Helper()
	resp, err := cl.Schema.TenantsGet(schema.NewTenantsGetParams().WithContext(ctx).WithClassName(class), nil)
	require.NoError(t, err)
	require.Len(t, resp.Payload, want)
	for _, tn := range resp.Payload {
		require.Equal(t, models.TenantActivityStatusHOT, tn.ActivityStatus, "tenant %s", tn.Name)
	}
}

// ledger records acknowledged writes by class and tenant ("" for single-tenant classes).
type ledger struct {
	mu    sync.Mutex
	acked map[string][]strfmt.UUID
}

func newLedger() *ledger { return &ledger{acked: map[string][]strfmt.UUID{}} }

func (l *ledger) add(class, tenant string, ids ...strfmt.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.acked[class+"/"+tenant] = append(l.acked[class+"/"+tenant], ids...)
}

func (l *ledger) ids(class, tenant string) []strfmt.UUID {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.acked[class+"/"+tenant])
}

func (l *ledger) remove(class, tenant string, id strfmt.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.acked[class+"/"+tenant] = slices.DeleteFunc(l.acked[class+"/"+tenant], func(x strfmt.UUID) bool { return x == id })
}

// writeObjects batch-writes n objects and records them only once every one is acknowledged.
func writeObjects(ctx context.Context, cl *apiclient.Weaviate, l *ledger, class, tenant string, n int, consistency *string) error {
	objs := make([]*models.Object, n)
	ids := make([]strfmt.UUID, n)
	for i := range objs {
		ids[i] = strfmt.UUID(uuid.NewString())
		vec := make([]float32, 8)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		objs[i] = &models.Object{Class: class, ID: ids[i], Tenant: tenant, Properties: map[string]any{"owner": tenant, "seq": i}, Vector: vec}
	}
	params := batch.NewBatchObjectsCreateParams().WithContext(ctx).WithBody(batch.BatchObjectsCreateBody{Objects: objs})
	if consistency != nil {
		params = params.WithConsistencyLevel(consistency)
	}
	resp, err := cl.Batch.BatchObjectsCreate(params, nil)
	if err != nil {
		return fmt.Errorf("batch write %s/%s: %w", class, tenant, err)
	}
	for _, o := range resp.Payload {
		if o.Result != nil && o.Result.Errors != nil && len(o.Result.Errors.Error) > 0 {
			return fmt.Errorf("batch write %s/%s: %s", class, tenant, o.Result.Errors.Error[0].Message)
		}
	}
	l.add(class, tenant, ids...)
	return nil
}

func tenantPtr(tenant string) *string {
	if tenant == "" {
		return nil
	}
	return &tenant
}

// readObject fails unless the object exists and belongs to the shard it was written to.
func readObject(ctx context.Context, cl *apiclient.Weaviate, class, tenant string, id strfmt.UUID, nodeName *string) error {
	params := objects.NewObjectsClassGetParams().WithContext(ctx).WithClassName(class).WithID(id).WithTenant(tenantPtr(tenant))
	if nodeName != nil {
		params = params.WithNodeName(nodeName)
	}
	resp, err := cl.Objects.ObjectsClassGet(params, nil)
	if err != nil {
		return fmt.Errorf("read %s/%s/%s: %w", class, tenant, id, err)
	}
	if props, ok := resp.Payload.Properties.(map[string]any); !ok || props["owner"] != tenant {
		return fmt.Errorf("read %s/%s/%s: unexpected properties %v", class, tenant, id, resp.Payload.Properties)
	}
	return nil
}

// verify checks every acknowledged object reads back and every shard lists only its own objects, each once;
// exact also forbids objects whose write was never acknowledged, which is what catches a resurrected delete.
func verify(ctx context.Context, t *testing.T, cl *apiclient.Weaviate, l *ledger, class string, tenants []string, exact bool) {
	t.Helper()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, tenant := range tenants {
		g.Go(func() error {
			limit := int64(listLimit)
			resp, err := cl.Objects.ObjectsList(objects.NewObjectsListParams().WithContext(gctx).WithClass(&class).
				WithTenant(tenantPtr(tenant)).WithLimit(&limit), nil)
			if err != nil {
				return fmt.Errorf("list %s/%s: %w", class, tenant, err)
			}
			listed := map[strfmt.UUID]bool{}
			for _, o := range resp.Payload.Objects {
				if props, ok := o.Properties.(map[string]any); listed[o.ID] || !ok || props["owner"] != tenant {
					return fmt.Errorf("%s/%s lists %s twice or with foreign properties %v", class, tenant, o.ID, o.Properties)
				}
				listed[o.ID] = true
			}
			acked := l.ids(class, tenant)
			if exact && len(listed) != len(acked) {
				return fmt.Errorf("%s/%s holds %d objects, %d acknowledged", class, tenant, len(listed), len(acked))
			}
			for _, id := range acked {
				if !listed[id] {
					return fmt.Errorf("%s/%s does not list acknowledged %s", class, tenant, id)
				}
				if err := readObject(gctx, cl, class, tenant, id, nil); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())
}

// seed writes perShard objects to every shard concurrently.
func seed(ctx context.Context, t *testing.T, cl *apiclient.Weaviate, l *ledger, class string, tenants []string, perShard int, consistency *string) {
	t.Helper()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, tenant := range tenants {
		g.Go(func() error { return writeObjects(gctx, cl, l, class, tenant, perShard, consistency) })
	}
	require.NoError(t, g.Wait())
}

// randomWorkload runs ops random reads and writes across shards; the first failure is returned.
func randomWorkload(ctx context.Context, cl *apiclient.Weaviate, l *ledger, class string, tenants []string, ops, workers int) error {
	g, gctx := errgroup.WithContext(ctx)
	for range workers {
		g.Go(func() error {
			for range ops / workers {
				tenant := tenants[rand.Intn(len(tenants))]
				ids := l.ids(class, tenant)
				if rand.Intn(2) == 0 || len(ids) == 0 {
					if err := writeObjects(gctx, cl, l, class, tenant, 1, nil); err != nil {
						return err
					}
				} else if err := readObject(gctx, cl, class, tenant, ids[rand.Intn(len(ids))], nil); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return g.Wait()
}

// every runs fn each interval until the returned stop, which returns fn's first error; errors after stop are ignored.
func every(ctx context.Context, t *testing.T, interval time.Duration, fn func(context.Context) error) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-ticker.C:
				if err := fn(gctx); err != nil && gctx.Err() == nil {
					return err
				}
			}
		}
	})
	stop := sync.OnceValue(func() error {
		cancel()
		return g.Wait()
	})
	t.Cleanup(func() { assert.NoError(t, stop()) })
	return stop
}
