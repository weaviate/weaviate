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

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// legacyCounts are a filter on "hello world" and BM25 on "hello" and "hello world".
type legacyCounts struct{ filter, bm25Hello, bm25HelloWorld, scoreAtLeast5 int }

var (
	legacyWord  = legacyCounts{10, 10, 10, 0}
	legacyField = legacyCounts{5, 0, 5, 0}
	legacyEmpty = legacyCounts{0, 0, 0, 0}
)

// Shard folders written by v1.39.17 with their migration records (see each
// fixture's manifest.json), loaded by this version.
func TestReindexLegacyRecords(t *testing.T) {
	const filt, search = "property_title", "property_title_searchable"
	for _, tc := range []struct {
		fixture string
		name    string
		edit    func(t *testing.T, lsm string)
		want    legacyCounts
		wantErr string
	}{
		{fixture: "01-changetok-swapped-effect-shown", want: legacyField},
		{fixture: "03-changetok-promoted-effect-shown", want: legacyField},
		{fixture: "05-changetok-merged-not-switched", want: legacyWord},
		{fixture: "06-changetok-iterated-not-switched", want: legacyWord},
		{fixture: "07-changetok-iterated-v1-plus-swapped-v2-effect-shown", want: legacyField},
		{fixture: "08-rebuildsearchable-swapped", want: legacyWord},
		{fixture: "09-rebuildsearchable-promoted", want: legacyWord},
		{fixture: "10-rebuildsearchable-iterated-not-switched", want: legacyWord},
		{fixture: "11-changealgo-swapped-effect-shown", want: legacyWord},
		{fixture: "13-changealgo-merged-not-switched", want: legacyWord},
		{fixture: "14-enablerangeable-swapped-effect-shown", want: legacyCounts{10, 10, 10, 5}},
		// A partial run whose task v1.39.3-14 already deleted serves what the
		// class names, which is empty where the swap deleted the plain folder.
		{fixture: "02-changetok-swapped-effect-not-shown", want: legacyEmpty},
		{fixture: "04-changetok-promoted-effect-not-shown", want: legacyEmpty},
		{fixture: "12-changealgo-swapped-effect-not-shown", want: legacyCounts{10, 0, 0, 0}},
		{
			fixture: "05-changetok-merged-not-switched", name: "a word to field run, then a field to word run",
			edit: func(t *testing.T, lsm string) {
				removeLegacyRecords(t, lsm)
				for _, plain := range []string{filt, search} {
					require.NoError(t, os.Rename(filepath.Join(lsm, plain), filepath.Join(lsm, legacyStaged(plain, 2))))
					writeLegacyRecord(t, lsm, 1, "swapped", models.PropertyTokenizationField, plain, legacyStaged(plain, 1))
					writeLegacyRecord(t, lsm, 2, "swapped", models.PropertyTokenizationWord, plain, legacyStaged(plain, 2))
				}
			},
			want: legacyWord,
		},
		{
			fixture: "03-changetok-promoted-effect-shown", name: "an older promoted record under a newer one",
			edit: func(t *testing.T, lsm string) {
				for _, plain := range []string{filt, search} {
					writeLegacyRecord(t, lsm, 0, "promoted", models.PropertyTokenizationWhitespace, plain, "")
				}
			},
			want: legacyField,
		},
		{
			fixture: "08-rebuildsearchable-swapped", name: "merged",
			edit: func(t *testing.T, lsm string) {
				path := filepath.Join(lsm, legacyRecordsDir, "1_rebuild_searchable_foo-tenant__node1.json")
				raw, err := os.ReadFile(path)
				require.NoError(t, err)
				var rec map[string]any
				require.NoError(t, json.Unmarshal(raw, &rec))
				rec["state"] = "merged"
				raw, err = json.Marshal(rec)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, raw, 0o644))
			},
			want: legacyWord,
		},
		{
			fixture: "01-changetok-swapped-effect-shown", name: "a truncated record beside a swapped one",
			edit: func(t *testing.T, lsm string) {
				require.NoError(t, os.WriteFile(filepath.Join(lsm, legacyRecordsDir, "0_truncated.json"), []byte(`{"state":"swa`), 0o644))
			},
			wantErr: "0_truncated.json",
		},
	} {
		name := tc.fixture
		if tc.name != "" {
			name += "/" + tc.name
		}
		t.Run(name, func(t *testing.T) {
			n, lsm := loadLegacyFixture(t, tc.fixture, tc.edit)
			if tc.wantErr != "" {
				before := legacyFolders(t, lsm)
				_, err := n.index().initShard(context.Background(), legacyTenant, n.sr.ReadOnlyClass(n.c.class), nil, true, true)
				require.ErrorContains(t, err, tc.wantErr)
				require.Equal(t, before, legacyFolders(t, lsm), "records and folders stay")
				return
			}
			shd, err := n.index().initShard(context.Background(), legacyTenant, n.sr.ReadOnlyClass(n.c.class), nil, true, true)
			require.NoError(t, err)
			n.index().shards.Store(legacyTenant, shd)

			records, err := os.ReadDir(filepath.Join(lsm, legacyRecordsDir))
			require.NoError(t, err)
			require.Empty(t, records)
			for _, f := range legacyFolders(t, lsm) {
				require.NotRegexp(t, `_(ingest|reindex)_\d+$`, f, "a staged or sidecar folder is left")
			}
			got := legacyCounts{
				filter:         n.count(legacyTenant, n.filter(genProp, "hello world", schema.DataTypeText, filters.OperatorEqual)),
				bm25Hello:      n.bm25(legacyTenant, "hello"),
				bm25HelloWorld: n.bm25(legacyTenant, "hello world"),
			}
			if tc.want.scoreAtLeast5 > 0 {
				got.scoreAtLeast5 = n.count(legacyTenant, n.filter("score", 5, schema.DataTypeInt, filters.OperatorGreaterThanEqual))
			}
			require.Equal(t, tc.want, got)
		})
	}
}

const legacyTenant = "foo-tenant"

// loadLegacyFixture starts a node with the fixture's class and tenant, then
// puts the fixture's shard folder in place of the tenant's, unloaded.
func loadLegacyFixture(t *testing.T, fixture string, edit func(t *testing.T, lsm string)) (*genNode, string) {
	dir := filepath.Join("testdata", "reindex_legacy", fixture)
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	require.NoError(t, err)
	var m struct {
		Class    models.Class `json:"classAtUpgrade"`
		NodeName string       `json:"nodeName"`
	}
	require.NoError(t, json.Unmarshal(raw, &m))
	m.Class.VectorIndexConfig, m.Class.VectorIndexType = enthnsw.UserConfig{Skip: true}, "hnsw"
	c := &genCluster{t: t, ctx: context.Background(), class: m.Class.Class}
	c.startNode(m.NodeName)
	require.NoError(t, c.apply(api.ApplyRequest_TYPE_ADD_CLASS, m.Class.Class, api.AddClassRequest{
		Class: &m.Class, State: &sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{}},
	}))
	c.addTenants(legacyTenant)

	n := c.nodes[0]
	idx := n.index()
	require.NoError(t, n.shard(legacyTenant).Shutdown(c.ctx))
	idx.shards.LoadAndDelete(legacyTenant)
	require.NoError(t, os.RemoveAll(filepath.Join(idx.path(), legacyTenant)))
	out, err := exec.Command("tar", "-xzf", filepath.Join(dir, "shard.tar.gz"), "-C", idx.path()).CombinedOutput()
	require.NoError(t, err, string(out))
	lsm := filepath.Join(idx.path(), legacyTenant, "lsm")
	if edit != nil {
		edit(t, lsm)
	}
	return n, lsm
}

func legacyStaged(plain string, gen int) string {
	if strings.HasSuffix(plain, "_searchable") {
		return fmt.Sprintf("%s__retokenize_ingest_%d", plain, gen)
	}
	return fmt.Sprintf("%s__filt_retokenize_ingest_%d", plain, gen)
}

// writeLegacyRecord writes a change-tokenization record in the v1.39.17 format.
func writeLegacyRecord(t *testing.T, lsm string, version uint64, state, target, canonical, staged string) {
	raw, err := json.Marshal(map[string]any{"formatVersion": 1, "state": state, "subject": map[string]any{
		"key":           map[string]any{"taskVersion": version},
		"migrationType": ReindexTypeChangeTokenization, "targetTokenization": target,
		"props": map[string]any{"title": map[string]string{"staged": staged, "canonical": canonical}},
	}})
	require.NoError(t, err)
	name := filepath.Join(lsm, legacyRecordsDir, fmt.Sprintf("%d_%s.json", version, canonical))
	require.NoError(t, os.WriteFile(name, raw, 0o644))
}

func removeLegacyRecords(t *testing.T, lsm string) {
	require.NoError(t, os.RemoveAll(filepath.Join(lsm, legacyRecordsDir)))
	require.NoError(t, os.MkdirAll(filepath.Join(lsm, legacyRecordsDir), 0o755))
}

// legacyFolders lists the shard's top-level folders and everything under its migrations folder.
func legacyFolders(t *testing.T, lsm string) []string {
	entries, err := os.ReadDir(lsm)
	require.NoError(t, err)
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	require.NoError(t, filepath.WalkDir(filepath.Join(lsm, migrationsDir), func(path string, _ os.DirEntry, err error) error {
		out = append(out, strings.TrimPrefix(path, lsm))
		return err
	}))
	return out
}
