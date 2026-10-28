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

// Package lazyshardunloading holds the end-to-end tests of licensed lazy shard
// unloading: every knob is set through the runtime overrides file, loaded
// shards are read from the unloader's gauges, and every workload checks that
// no request fails and no write is lost while shards unload.
package lazyshardunloading

import (
	"context"
	"errors"
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

	metricLoaded  = "weaviate_lazy_shard_unloading_loaded_shards"
	metricLimit   = "weaviate_lazy_shard_unloading_limit"
	metricUnloads = "weaviate_lazy_shard_unloading_unloads_total"
)

// knobs are lazy_shard_unloading_* runtime overrides, keyed without the prefix.
type knobs map[string]string

func (k knobs) yaml() string {
	var b strings.Builder
	for _, key := range slices.Sorted(func(yield func(string) bool) {
		for key := range k {
			if !yield(key) {
				return
			}
		}
	}) {
		fmt.Fprintf(&b, "lazy_shard_unloading_%s: %s\n", key, k[key])
	}
	return b.String()
}

// fastKnobs makes the background loop tick every second and keep nothing resident.
func fastKnobs(extra knobs) knobs {
	k := knobs{"interval": "1s", "min_residency": "0s", "idle_check_interval": "1s"}
	for key, v := range extra {
		k[key] = v
	}
	return k
}

// newCompose is a cluster whose knobs come only from the runtime overrides file.
func newCompose(t *testing.T, licensed bool, nodeCount int, initial knobs) *docker.Compose {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime-overrides.yaml")
	require.NoError(t, os.WriteFile(path, []byte(initial.yaml()), 0o644))
	b := docker.New().
		WithWeaviateEnv("RUNTIME_OVERRIDES_ENABLED", "true").
		WithWeaviateEnv("RUNTIME_OVERRIDES_PATH", overridesPath).
		WithWeaviateEnv("RUNTIME_OVERRIDES_LOAD_INTERVAL", "1s").
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true").
		WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").
		WithWeaviateEnv("LOG_LEVEL", "info").
		WithWeaviateFiles(testcontainers.ContainerFile{
			HostFilePath:      path,
			ContainerFilePath: overridesPath,
			FileMode:          0o644,
		})
	if nodeCount == 1 {
		b = b.WithWeaviate()
	} else {
		b = b.WithWeaviateCluster(nodeCount)
	}
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

// setKnobs rewrites the overrides file on every node; callers wait on a gauge for the reload.
func setKnobs(ctx context.Context, t *testing.T, compose *docker.DockerCompose, nodeCount int, k knobs) {
	t.Helper()
	for i := 1; i <= nodeCount; i++ {
		c := compose.GetWeaviateNode(i).Container()
		require.NoError(t, c.CopyToContainer(ctx, []byte(k.yaml()), overridesPath, 0o644))
	}
}

// metric sums every series of name on the node; ok is false while the series is absent.
func metric(ctx context.Context, c *docker.DockerContainer, name string, labelFragments ...string) (value float64, ok bool, err error) {
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
		if line == "" || !containsAll(line, labelFragments) {
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

func containsAll(s string, fragments []string) bool {
	for _, f := range fragments {
		if !strings.Contains(s, f) {
			return false
		}
	}
	return true
}

// loadedShards is the unloader's own count of loaded shards on the node.
func loadedShards(ctx context.Context, c *docker.DockerContainer) (int, error) {
	v, ok, err := metric(ctx, c, metricLoaded)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New(metricLoaded + " is not published")
	}
	return int(v), nil
}

// waitLimit blocks until every node's unloader applied max_loaded_shards=limit.
func waitLimit(ctx context.Context, t *testing.T, compose *docker.DockerCompose, nodeCount, limit int) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for i := 1; i <= nodeCount; i++ {
			v, ok, err := metric(ctx, compose.GetWeaviateNode(i), metricLimit)
			require.NoError(ct, err)
			require.True(ct, ok, "node %d publishes no %s", i, metricLimit)
			require.Equal(ct, float64(limit), v, "node %d", i)
		}
	}, 60*time.Second, 500*time.Millisecond, "max_loaded_shards=%d was not applied", limit)
}

// gaugeStable outlasts one publish of the loaded gauge, so a value published before the last change cannot satisfy a wait.
const gaugeStable = 1500 * time.Millisecond

// eventuallyLoaded waits until cond has held for every node's loaded count for gaugeStable.
func eventuallyLoaded(ctx context.Context, t *testing.T, compose *docker.DockerCompose, nodeCount int, timeout time.Duration, cond func(loaded int) bool, msg string) {
	t.Helper()
	var heldSince time.Time
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for i := 1; i <= nodeCount; i++ {
			n, err := loadedShards(ctx, compose.GetWeaviateNode(i))
			if err != nil || !cond(n) {
				heldSince = time.Time{}
			}
			require.NoError(ct, err)
			require.True(ct, cond(n), "node %d has %d loaded shards", i, n)
		}
		if heldSince.IsZero() {
			heldSince = time.Now()
		}
		require.GreaterOrEqual(ct, time.Since(heldSince), gaugeStable, "the loaded count has not held for a full publish interval")
	}, timeout, 250*time.Millisecond, msg)
}

// nodeLoadedShards reads shard load states from the nodes API; it pins every loaded shard it reports.
func nodeLoadedShards(ctx context.Context, cl *apiclient.Weaviate, class string) (map[string]map[string]bool, error) {
	out := verbosity.OutputVerbose
	resp, err := cl.Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithContext(ctx).WithClassName(class).WithOutput(&out), nil)
	if err != nil {
		return nil, err
	}
	byNode := map[string]map[string]bool{}
	for _, n := range resp.Payload.Nodes {
		shards := map[string]bool{}
		for _, s := range n.Shards {
			shards[s.Name] = s.Loaded
		}
		byNode[n.Name] = shards
	}
	return byNode, nil
}

// shardLoaded reads one shard's load state on the queried node; it pins that shard only.
func shardLoaded(ctx context.Context, cl *apiclient.Weaviate, class, shard string) (bool, error) {
	out := verbosity.OutputVerbose
	resp, err := cl.Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithContext(ctx).WithClassName(class).
		WithShardName(&shard).WithOutput(&out), nil)
	if err != nil {
		return false, err
	}
	for _, n := range resp.Payload.Nodes {
		for _, s := range n.Shards {
			if s.Name == shard {
				return s.Loaded, nil
			}
		}
	}
	return false, fmt.Errorf("shard %s/%s not reported", class, shard)
}

func countTrue(m map[string]bool) int {
	n := 0
	for _, v := range m {
		if v {
			n++
		}
	}
	return n
}

func tenantNames(prefix string, n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("%s%04d", prefix, i)
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
	for chunk := range slices.Chunk(names, 100) {
		tenants := make([]*models.Tenant, len(chunk))
		for i, n := range chunk {
			tenants[i] = &models.Tenant{Name: n, ActivityStatus: models.TenantActivityStatusHOT}
		}
		_, err := cl.Schema.TenantsCreate(schema.NewTenantsCreateParams().WithContext(ctx).WithClassName(class).WithBody(tenants), nil)
		require.NoError(t, err)
	}
}

func setTenantStatus(ctx context.Context, cl *apiclient.Weaviate, class, tenant, status string) error {
	_, err := cl.Schema.TenantsUpdate(schema.NewTenantsUpdateParams().WithContext(ctx).WithClassName(class).
		WithBody([]*models.Tenant{{Name: tenant, ActivityStatus: status}}), nil)
	return err
}

func tenantStatuses(ctx context.Context, cl *apiclient.Weaviate, class string) (map[string]string, error) {
	resp, err := cl.Schema.TenantsGet(schema.NewTenantsGetParams().WithContext(ctx).WithClassName(class), nil)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(resp.Payload))
	for _, tn := range resp.Payload {
		out[tn.Name] = tn.ActivityStatus
	}
	return out, nil
}

// ledger records every acknowledged write, keyed by class and tenant ("" for single-tenant classes).
type ledger struct {
	mu   sync.Mutex
	objs map[string][]strfmt.UUID
}

func newLedger() *ledger { return &ledger{objs: map[string][]strfmt.UUID{}} }

func ledgerKey(class, tenant string) string { return class + "/" + tenant }

func (l *ledger) add(class, tenant string, ids ...strfmt.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.objs[ledgerKey(class, tenant)] = append(l.objs[ledgerKey(class, tenant)], ids...)
}

func (l *ledger) ids(class, tenant string) []strfmt.UUID {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.objs[ledgerKey(class, tenant)])
}

func (l *ledger) remove(class, tenant string, id strfmt.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := ledgerKey(class, tenant)
	l.objs[key] = slices.DeleteFunc(l.objs[key], func(x strfmt.UUID) bool { return x == id })
}

func (l *ledger) random(class, tenant string, r *rand.Rand) (strfmt.UUID, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ids := l.objs[ledgerKey(class, tenant)]
	if len(ids) == 0 {
		return "", false
	}
	return ids[r.Intn(len(ids))], true
}

func randomVector(r *rand.Rand) []float32 {
	v := make([]float32, 8)
	for i := range v {
		v[i] = r.Float32()
	}
	return v
}

// writeObjects batch-writes n objects and records them only once every one is acknowledged.
func writeObjects(ctx context.Context, cl *apiclient.Weaviate, l *ledger, class, tenant string, n int, consistency *string, r *rand.Rand) error {
	objs := make([]*models.Object, n)
	ids := make([]strfmt.UUID, n)
	for i := range objs {
		ids[i] = strfmt.UUID(uuid.NewString())
		objs[i] = &models.Object{
			Class:      class,
			ID:         ids[i],
			Tenant:     tenant,
			Properties: map[string]any{"owner": tenant, "seq": i},
			Vector:     randomVector(r),
		}
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
	props, ok := resp.Payload.Properties.(map[string]any)
	if !ok || props["owner"] != tenant {
		return fmt.Errorf("read %s/%s/%s: unexpected properties %v", class, tenant, id, resp.Payload.Properties)
	}
	return nil
}

func objectGone(ctx context.Context, cl *apiclient.Weaviate, class, tenant string, id strfmt.UUID, nodeName string) (bool, error) {
	_, err := cl.Objects.ObjectsClassGet(objects.NewObjectsClassGetParams().WithContext(ctx).WithClassName(class).
		WithID(id).WithTenant(tenantPtr(tenant)).WithNodeName(&nodeName), nil)
	var notFound *objects.ObjectsClassGetNotFound
	if errors.As(err, &notFound) {
		return true, nil
	}
	return false, err
}

func countObjects(ctx context.Context, cl *apiclient.Weaviate, class, tenant string) (int, error) {
	limit := int64(10_000)
	resp, err := cl.Objects.ObjectsList(objects.NewObjectsListParams().WithContext(ctx).WithClass(&class).
		WithTenant(tenantPtr(tenant)).WithLimit(&limit), nil)
	if err != nil {
		return 0, fmt.Errorf("list %s/%s: %w", class, tenant, err)
	}
	return len(resp.Payload.Objects), nil
}

// verifyAll checks every acknowledged object reads back and every shard holds exactly the ledger's count.
func verifyAll(ctx context.Context, t *testing.T, cl *apiclient.Weaviate, l *ledger, class string, tenants []string) {
	t.Helper()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, tenant := range tenants {
		g.Go(func() error {
			ids := l.ids(class, tenant)
			n, err := countObjects(gctx, cl, class, tenant)
			if err != nil {
				return err
			}
			if n != len(ids) {
				return fmt.Errorf("%s/%s holds %d objects, %d acknowledged", class, tenant, n, len(ids))
			}
			for _, id := range ids {
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
	for i, tenant := range tenants {
		g.Go(func() error {
			return writeObjects(gctx, cl, l, class, tenant, perShard, consistency, rand.New(rand.NewSource(int64(i))))
		})
	}
	require.NoError(t, g.Wait())
}

// randomWorkload runs ops random reads and writes across shards; the first failure is returned.
func randomWorkload(ctx context.Context, cl *apiclient.Weaviate, l *ledger, class string, tenants []string, ops, workers int) error {
	g, gctx := errgroup.WithContext(ctx)
	per := ops / workers
	for w := 0; w < workers; w++ {
		g.Go(func() error {
			r := rand.New(rand.NewSource(int64(w) + time.Now().UnixNano()))
			for i := 0; i < per; i++ {
				tenant := tenants[r.Intn(len(tenants))]
				if r.Intn(2) == 0 {
					if err := writeObjects(gctx, cl, l, class, tenant, 1, nil, r); err != nil {
						return err
					}
					continue
				}
				id, ok := l.random(class, tenant, r)
				if !ok {
					continue
				}
				if err := readObject(gctx, cl, class, tenant, id, nil); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return g.Wait()
}

func envInt(t *testing.T, name string, def int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	require.NoError(t, err, "%s", name)
	return n
}
