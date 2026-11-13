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
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	// metricUnloading has no labels, so every tick publishes it, namespaces or not.
	metricUnloading = "weaviate_idle_shard_collector_unloading_shards"

	// listLimit is the server's default QUERY_MAXIMUM_RESULTS.
	listLimit = 10_000
)

// knobs are idle_shard_collector_* runtime overrides, keyed without the prefix.
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
		fmt.Fprintf(&b, "idle_shard_collector_%s: %s\n", key, k[key])
	}
	return b.String()
}

// idleTTL leaves each test time to see its shards loaded before they unload.
const idleTTL = "30s"

// fastKnobs makes the collector check every second.
func fastKnobs(extra knobs) knobs {
	k := knobs{"check_interval": "1s"}
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

// loadedShards is the collector's own count of loaded shards on the node; a running collector drops an emptied namespace's series.
func loadedShards(ctx context.Context, c *docker.DockerContainer) (int, error) {
	v, ok, err := metric(ctx, c, metricLoaded)
	if err != nil || ok {
		return int(v), err
	}
	if _, running, err := metric(ctx, c, metricUnloading); err != nil || !running {
		return 0, errors.Join(err, errors.New(metricLoaded+" is not published"))
	}
	return 0, nil
}

// waitUnloader blocks until every node's collector publishes its gauges.
func waitUnloader(ctx context.Context, t *testing.T, compose *docker.DockerCompose, nodeCount int) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for i := 1; i <= nodeCount; i++ {
			_, ok, err := metric(ctx, compose.GetWeaviateNode(i), metricUnloading)
			require.NoError(ct, err)
			require.True(ct, ok, "node %d publishes no %s", i, metricUnloading)
		}
	}, 60*time.Second, 500*time.Millisecond, "the unloader did not start")
}

// noneLoaded is the drain target of idle unloading.
func noneLoaded(n int) bool { return n == 0 }

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

// ledger records every acknowledged write and every attempted ID, keyed by class and tenant ("" for single-tenant classes).
type ledger struct {
	mu        sync.Mutex
	objs      map[string][]strfmt.UUID
	attempted map[string]map[strfmt.UUID]struct{}
}

func newLedger() *ledger {
	return &ledger{objs: map[string][]strfmt.UUID{}, attempted: map[string]map[strfmt.UUID]struct{}{}}
}

func ledgerKey(class, tenant string) string { return class + "/" + tenant }

func (l *ledger) add(class, tenant string, ids ...strfmt.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.objs[ledgerKey(class, tenant)] = append(l.objs[ledgerKey(class, tenant)], ids...)
}

func (l *ledger) attempt(class, tenant string, ids ...strfmt.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := ledgerKey(class, tenant)
	if l.attempted[key] == nil {
		l.attempted[key] = make(map[strfmt.UUID]struct{}, len(ids))
	}
	for _, id := range ids {
		l.attempted[key][id] = struct{}{}
	}
}

func (l *ledger) attemptedIDs(class, tenant string) map[strfmt.UUID]struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return maps.Clone(l.attempted[ledgerKey(class, tenant)])
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
	l.attempt(class, tenant, ids...)
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

// listObjectIDs returns one ID per listed object, so a duplicate stays visible; a full page may be cut short and is an error.
func listObjectIDs(ctx context.Context, cl *apiclient.Weaviate, class, tenant string) ([]strfmt.UUID, error) {
	limit := int64(listLimit)
	resp, err := cl.Objects.ObjectsList(objects.NewObjectsListParams().WithContext(ctx).WithClass(&class).
		WithTenant(tenantPtr(tenant)).WithLimit(&limit), nil)
	if err != nil {
		return nil, fmt.Errorf("list %s/%s: %w", class, tenant, err)
	}
	if len(resp.Payload.Objects) >= listLimit {
		return nil, fmt.Errorf("list %s/%s: %d objects reach the listing limit", class, tenant, len(resp.Payload.Objects))
	}
	ids := make([]strfmt.UUID, len(resp.Payload.Objects))
	for i, o := range resp.Payload.Objects {
		ids[i] = o.ID
	}
	return ids, nil
}

func countObjects(ctx context.Context, cl *apiclient.Weaviate, class, tenant string) (int, error) {
	ids, err := listObjectIDs(ctx, cl, class, tenant)
	if err != nil {
		return 0, err
	}
	return len(ids), nil
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

// checkListing compares one shard's listing with its ledger and returns how many unacknowledged writes it holds.
func checkListing(acked, listed []strfmt.UUID, attempted map[strfmt.UUID]struct{}) (int, error) {
	counts := make(map[strfmt.UUID]int, len(listed))
	for _, id := range listed {
		counts[id]++
	}
	var missing, duplicated, neverAttempted []strfmt.UUID
	for _, id := range acked {
		if counts[id] == 0 {
			missing = append(missing, id)
		}
	}
	for id, n := range counts {
		if n > 1 {
			duplicated = append(duplicated, id)
		}
		if _, ok := attempted[id]; !ok {
			neverAttempted = append(neverAttempted, id)
		}
	}
	slices.Sort(duplicated)
	slices.Sort(neverAttempted)
	var errs []error
	if len(missing) > 0 {
		errs = append(errs, fmt.Errorf("acknowledged but not listed: %v", missing))
	}
	if len(duplicated) > 0 {
		errs = append(errs, fmt.Errorf("listed more than once: %v", duplicated))
	}
	if len(neverAttempted) > 0 {
		errs = append(errs, fmt.Errorf("listed but never attempted: %v", neverAttempted))
	}
	if err := errors.Join(errs...); err != nil {
		return 0, err
	}
	return len(counts) - len(acked), nil
}

// verifyAcknowledged is verifyAll for writers that lose responses: an unacknowledged write may or may not exist, nothing else may differ.
func verifyAcknowledged(ctx context.Context, t *testing.T, cl *apiclient.Weaviate, l *ledger, class string, tenants []string) {
	t.Helper()
	var applied, unacknowledged atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, tenant := range tenants {
		g.Go(func() error {
			acked := l.ids(class, tenant)
			listed, err := listObjectIDs(gctx, cl, class, tenant)
			if err != nil {
				return err
			}
			attempted := l.attemptedIDs(class, tenant)
			n, err := checkListing(acked, listed, attempted)
			if err != nil {
				return fmt.Errorf("%s/%s: %w", class, tenant, err)
			}
			applied.Add(int64(n))
			unacknowledged.Add(int64(len(attempted) - len(acked)))
			for _, id := range acked {
				if err := readObject(gctx, cl, class, tenant, id, nil); err != nil {
					return fmt.Errorf("acknowledged but does not read back: %w", err)
				}
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())
	t.Logf("%s: %d of %d unacknowledged writes turned out applied", class, applied.Load(), unacknowledged.Load())
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

func TestCheckListing(t *testing.T) {
	t.Parallel()
	const a, b, c, d strfmt.UUID = "a", "b", "c", "d"
	attempted := map[strfmt.UUID]struct{}{a: {}, b: {}, c: {}}
	tests := []struct {
		name        string
		acked       []strfmt.UUID
		listed      []strfmt.UUID
		wantApplied int
		wantErrs    []string
	}{
		{name: "only acknowledged writes", acked: []strfmt.UUID{a, b}, listed: []strfmt.UUID{b, a}},
		{name: "unacknowledged write lost", acked: []strfmt.UUID{a}, listed: []strfmt.UUID{a}},
		{name: "unacknowledged writes applied", acked: []strfmt.UUID{a}, listed: []strfmt.UUID{a, b, c}, wantApplied: 2},
		{
			name: "acknowledged write missing", acked: []strfmt.UUID{a, b}, listed: []strfmt.UUID{a, c},
			wantErrs: []string{"acknowledged but not listed: [b]"},
		},
		{
			name: "acknowledged write listed twice", acked: []strfmt.UUID{a}, listed: []strfmt.UUID{a, a},
			wantErrs: []string{"listed more than once: [a]"},
		},
		{
			name: "unacknowledged write listed twice", acked: []strfmt.UUID{a}, listed: []strfmt.UUID{a, b, b},
			wantErrs: []string{"listed more than once: [b]"},
		},
		{
			name: "listed but never attempted", acked: []strfmt.UUID{a}, listed: []strfmt.UUID{a, d},
			wantErrs: []string{"listed but never attempted: [d]"},
		},
		{
			name: "every check breaks at once", acked: []strfmt.UUID{a, b}, listed: []strfmt.UUID{b, b, d},
			wantErrs: []string{"acknowledged but not listed: [a]", "listed more than once: [b]", "listed but never attempted: [d]"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			applied, err := checkListing(tt.acked, tt.listed, attempted)
			if len(tt.wantErrs) == 0 {
				require.NoError(t, err)
				require.Equal(t, tt.wantApplied, applied)
				return
			}
			for _, want := range tt.wantErrs {
				require.ErrorContains(t, err, want)
			}
		})
	}
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
