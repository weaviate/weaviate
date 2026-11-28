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
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

func cardinality(t *testing.T, b *lsmkv.Bucket, key []byte) uint64 {
	require.NotNil(t, b)
	bm, release, err := b.RoaringSetGet(context.Background(), key)
	require.NoError(t, err)
	defer release()
	return uint64(bm.GetCardinality())
}

func (f *viewFixture) refreshTo(states ...distributedtask.ShardTaskState) {
	f.tasks.set(states...)
	require.NoError(f.t, f.shard.refreshView(context.Background()))
}

func (f *viewFixture) putTitle(obj *storobj.Object, title any) {
	obj.Object.Properties = map[string]any{}
	if title != nil {
		obj.Object.Properties = map[string]any{"title": title}
	}
	require.NoError(f.t, f.shard.PutObject(context.Background(), obj))
}

// stagedTitle is the title build the fixture's current view stages.
func stagedTitle(f *viewFixture) *lsmkv.Bucket {
	staged := f.shard.loadView().staged
	require.Len(f.t, staged, 1)
	return staged[0].bucket
}

// postings reports, per term, how many documents b lists and their propLen.
func postings(t *testing.T, b *lsmkv.Bucket, terms ...string) string {
	require.NotNil(t, b)
	var out []string
	for _, term := range terms {
		pairs, err := b.MapList(context.Background(), []byte(term))
		require.NoError(t, err)
		lens := []float32{}
		for _, p := range pairs {
			lens = append(lens, math.Float32frombits(binary.LittleEndian.Uint32(p.Value[4:8])))
		}
		out = append(out, fmt.Sprintf("%s:%v", term, lens))
	}
	return strings.Join(out, " ")
}

func TestReindexViewWrites(t *testing.T) {
	off, on := false, true
	tests := []struct {
		name  string
		props []*models.Property          // the fixture's default title when empty
		run   func(f *viewFixture) string // reports what the folders hold
		want  string
	}{
		{
			name: "an update after a switch to BlockMax writes whole term sets that survive a reload",
			props: []*models.Property{{
				Name: "title", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord,
				IndexFilterable: &on, IndexSearchable: &off,
			}},
			run: func(f *viewFixture) string {
				enable := func(switched bool) distributedtask.ShardTaskState {
					st := viewTaskState("en", 1, ReindexTypeEnableSearchable, distributedtask.TaskStatusStarted, switched, true)
					h := st.Header.(ReindexTaskHeader)
					h.TargetTokenization = models.PropertyTokenizationWord
					st.Header = h
					return st
				}
				f.refreshTo(enable(false))
				f.refreshTo(enable(true))
				obj := testObject(f.class.Class)
				f.putTitle(obj, "a b")
				f.putTitle(obj, "a b c")
				f.editTitle(func(p *models.Property) { p.IndexSearchable = &on })
				f.setLive(map[string]uint64{indexTypeSearchable: 1})
				f.tasks.set()
				f.reload()
				return postings(f.t, f.shard.store.Bucket(reindexBucketName("title", indexTypeSearchable, 1)), "a", "b", "c")
			},
			want: "a:[3] b:[3] c:[3]",
		},
		{
			name: "a write on the old view lands in the served and the staged folder",
			run: func(f *viewFixture) string {
				f.refreshTo(stagedTask("a"))
				f.putTitle(testObject(f.class.Class), "hello")
				return fmt.Sprint(cardinality(f.t, f.shard.store.Bucket(plainTitle), []byte("hello")),
					cardinality(f.t, stagedTitle(f), []byte("hello")))
			},
			want: "1 1",
		},
		{
			name: "an update on the old view moves the staged entry",
			run: func(f *viewFixture) string {
				f.refreshTo(stagedTask("a"))
				obj := testObject(f.class.Class)
				f.putTitle(obj, "hello")
				f.putTitle(obj, "bye")
				return fmt.Sprint(cardinality(f.t, stagedTitle(f), []byte("hello")),
					cardinality(f.t, stagedTitle(f), []byte("bye")))
			},
			want: "0 1",
		},
		{
			name: "a delete on the old view removes the staged entry",
			run: func(f *viewFixture) string {
				f.refreshTo(stagedTask("a"))
				obj := testObject(f.class.Class)
				f.putTitle(obj, "hello")
				require.NoError(f.t, f.shard.DeleteObject(context.Background(), obj.ID(), time.Now()))
				return fmt.Sprint(cardinality(f.t, stagedTitle(f), []byte("hello")))
			},
			want: "0",
		},
		{
			name: "a write on the new view is analyzed with the view's tokenization",
			run: func(f *viewFixture) string {
				tok := func(switched bool) distributedtask.ShardTaskState {
					return viewTaskState("tok", 1, ReindexTypeChangeTokenization, distributedtask.TaskStatusStarted, switched, true)
				}
				f.refreshTo(tok(false))
				f.refreshTo(tok(true))
				f.putTitle(testObject(f.class.Class), "hello world")
				served := f.shard.loadView().Bucket(plainTitle)
				return fmt.Sprint(filepath.Base(served.GetDir()), " ",
					cardinality(f.t, served, []byte("hello world")), cardinality(f.t, served, []byte("hello")))
			},
			want: gen1Title + " 1 0",
		},
		{
			name: "a staged build is analyzed with the view's tokenization, not the class's",
			run: func(f *viewFixture) string {
				tok := func(switched bool) distributedtask.ShardTaskState {
					return viewTaskState("tok", 1, ReindexTypeChangeTokenization, distributedtask.TaskStatusFinished, switched, true)
				}
				f.refreshTo(tok(false))
				f.refreshTo(tok(true), viewTaskState("rep", 2, ReindexTypeRepairFilterable, distributedtask.TaskStatusStarted, false, true))
				f.putTitle(testObject(f.class.Class), "hello world")
				return fmt.Sprint(cardinality(f.t, stagedTitle(f), []byte("hello world")), cardinality(f.t, stagedTitle(f), []byte("hello")))
			},
			want: "1 0",
		},
		{
			name: "a length or null write after a switch lands in the plain length and null folders",
			run: func(f *viewFixture) string {
				f.refreshTo(stagedTask("a"))
				f.refreshTo(switchedTask("a"))
				f.putTitle(testObject(f.class.Class), "hi")
				f.putTitle(testObject(f.class.Class), nil)
				lengthKey, err := bucketKeyPropertyLength(2)
				require.NoError(f.t, err)
				nullKey, err := bucketKeyPropertyNull(true)
				require.NoError(f.t, err)
				return fmt.Sprint(
					cardinality(f.t, f.shard.store.Bucket(helpers.BucketFromPropNameLengthLSM("title")), lengthKey),
					cardinality(f.t, f.shard.store.Bucket(helpers.BucketFromPropNameNullLSM("title")), nullKey))
			},
			want: "1 1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.run(newViewFixture(t, "ViewTest", tc.props...)))
		})
	}
}

// titleBuckets lists the open buckets of the fixture's title property.
func titleBuckets(s *Shard) []string {
	var out []string
	for name := range s.store.GetBucketsByName() {
		if strings.HasPrefix(name, "property_title") {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// A run that turns on a property's first index leaves the shard with the
// length and null buckets a load opens, so writes after the switch count.
func TestReindexViewWritesEnableFirstIndex(t *testing.T) {
	off, on := false, true
	for _, tc := range []struct {
		mt       ReindexMigrationType
		dataType []string
		value    any
		length   int // the length key the value writes; 0 for a type without a length index
		enable   func(p *models.Property)
	}{
		{ReindexTypeEnableFilterable, schema.DataTypeText.PropString(), "hi", 2, func(p *models.Property) { p.IndexFilterable = &on }},
		{ReindexTypeEnableSearchable, schema.DataTypeText.PropString(), "hi", 2, func(p *models.Property) { p.IndexSearchable = &on }},
		{ReindexTypeEnableRangeable, schema.DataTypeInt.PropString(), float64(2), 0, func(p *models.Property) { p.IndexRangeFilters = &on }},
	} {
		t.Run(string(tc.mt), func(t *testing.T) {
			f := newViewFixture(t, "ViewTest", &models.Property{
				Name: "title", DataType: tc.dataType, Tokenization: models.PropertyTokenizationWord,
				IndexFilterable: &off, IndexSearchable: &off, IndexRangeFilters: &off,
			})
			if tc.length == 0 {
				f.class.Properties[0].Tokenization = ""
			}
			run := func(switched bool) distributedtask.ShardTaskState {
				return viewTaskState("en", 1, tc.mt, distributedtask.TaskStatusStarted, switched, true)
			}
			f.refreshTo(run(false))
			staged := titleBuckets(f.shard)
			f.refreshTo(run(true))
			require.Equal(t, staged, titleBuckets(f.shard), "the switch opens nothing")
			f.putTitle(testObject(f.class.Class), tc.value)

			nullKey, err := bucketKeyPropertyNull(false)
			require.NoError(t, err)
			require.Equal(t, uint64(1), cardinality(t, f.shard.store.Bucket(helpers.BucketFromPropNameNullLSM("title")), nullKey))
			if tc.length > 0 {
				lengthKey, err := bucketKeyPropertyLength(tc.length)
				require.NoError(t, err)
				require.Equal(t, uint64(1), cardinality(t, f.shard.store.Bucket(helpers.BucketFromPropNameLengthLSM("title")), lengthKey))
			}

			afterSwitch := titleBuckets(f.shard)
			types, _ := ReindexIndexTypes(tc.mt)
			tc.enable(f.class.Properties[0])
			f.setLive(map[string]uint64{types[0]: 1})
			f.tasks.set()
			f.reload()
			require.Equal(t, titleBuckets(f.shard), afterSwitch, "a load opens the same buckets")
		})
	}
}

// A class update on a property no run governs leaves the shard with the buckets a load opens.
func TestReindexViewRefreshOpensWhatALoadOpens(t *testing.T) {
	off, on := false, true
	text := func(filterable, searchable bool) *models.Property {
		return &models.Property{
			Name: "title", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord,
			IndexFilterable: &filterable, IndexSearchable: &searchable, IndexRangeFilters: &off,
		}
	}
	for _, tc := range []struct {
		name   string
		prop   *models.Property
		update func(p *models.Property)
	}{
		{name: "filterable turned on", prop: text(false, true), update: func(p *models.Property) { p.IndexFilterable = &on }},
		{name: "searchable turned on", prop: text(true, false), update: func(p *models.Property) { p.IndexSearchable = &on }},
		{name: "the first index turned on", prop: text(false, false), update: func(p *models.Property) { p.IndexFilterable = &on }},
		{
			name: "rangeable turned on", update: func(p *models.Property) { p.IndexRangeFilters = &on },
			prop: &models.Property{Name: "title", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &on, IndexRangeFilters: &off},
		},
		{name: "filterable turned off", prop: text(true, true), update: func(p *models.Property) { p.IndexFilterable = &off }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewFixture(t, "ViewTest", tc.prop)
			f.editTitle(tc.update)
			require.NoError(t, f.shard.refreshView(context.Background()))
			f.shard.closeViews() // waits for the retire of a turned-off index
			refreshed := titleBuckets(f.shard)
			f.reload()
			require.Equal(t, titleBuckets(f.shard), refreshed)
		})
	}
}

// genModel is what a tenant must hold: each object's latest value, and every value ever written.
type genModel struct {
	mu      sync.Mutex
	live    map[strfmt.UUID]string
	written []string
}

// Writers insert, update and delete across staging, the build and the switch;
// afterwards each live value is found once and no stale value is found.
func TestReindexViewWrites_Stress(t *testing.T) {
	c := newGenCluster(t, genDefaultOpts(), 1)
	n := c.nodes[0]
	tenants := []string{"t1", "t2", "t3", "t4"}
	c.addTenants(tenants...)
	held := map[string]*genModel{}
	for _, tenant := range tenants {
		held[tenant] = &genModel{live: map[strfmt.UUID]string{}}
	}
	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			own := map[string][]strfmt.UUID{}
			for i := 0; ctx.Err() == nil; i++ {
				tenant := tenants[i%len(tenants)]
				if err := genWrite(n, rng, held[tenant], own, tenant, fmt.Sprintf("w%dv%d", w, i)); err != nil {
					t.Errorf("writer %d: %v", w, err)
					return
				}
			}
		}, n.db.logger)
	}
	c.waitFor("every tenant to hold objects", func() bool {
		for _, m := range held {
			m.mu.Lock()
			size := len(m.live)
			m.mu.Unlock()
			if size < 50 {
				return false
			}
		}
		return true
	})
	id, err := c.submit(ReindexTypeChangeTokenization, models.PropertyTokenizationField)
	require.NoError(t, err)
	c.waitGone(id)
	for _, tenant := range tenants {
		c.waitServed(tenant, genProp, 1, indexTypeFilterable, indexTypeSearchable)
	}
	stop()
	wg.Wait()

	for _, tenant := range tenants {
		m := held[tenant]
		live := map[string]bool{}
		for _, v := range m.live {
			live[v] = true
		}
		lost, stray := 0, 0
		for _, v := range m.written {
			got := n.count(tenant, n.filter(genProp, v, schema.DataTypeText, filters.OperatorEqual))
			switch {
			case live[v] && got != 1:
				lost++
			case !live[v] && got != 0:
				stray++
			}
		}
		require.Equal(t, [2]int{0, 0}, [2]int{lost, stray}, "%s: lost and stray of %d values", tenant, len(m.written))
	}
}

func genWrite(n *genNode, rng *rand.Rand, m *genModel, own map[string][]strfmt.UUID, tenant, value string) error {
	s := n.shard(tenant)
	ids := own[tenant]
	op := rng.Intn(3)
	if len(ids) == 0 {
		op = 0
	}
	var id strfmt.UUID
	switch op {
	case 0:
		obj := genObject(n.c.class, tenant, value, 0)
		if err := s.PutObject(context.Background(), obj); err != nil {
			return err
		}
		id, own[tenant] = obj.ID(), append(ids, obj.ID())
	case 1:
		id = ids[rng.Intn(len(ids))]
		obj := genObject(n.c.class, tenant, value, 0)
		obj.Object.ID = id
		if err := s.PutObject(context.Background(), obj); err != nil {
			return err
		}
	default:
		i := rng.Intn(len(ids))
		id = ids[i]
		if err := s.DeleteObject(context.Background(), id, time.Now()); err != nil {
			return err
		}
		own[tenant] = append(ids[:i], ids[i+1:]...)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if op == 2 {
		delete(m.live, id)
		return nil
	}
	m.live[id] = value
	m.written = append(m.written, value)
	return nil
}
