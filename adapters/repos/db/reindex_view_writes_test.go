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
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
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

func TestReindexViewWrites(t *testing.T) {
	tests := []struct {
		name string
		run  func(f *viewFixture) string // reports what the folders hold
		want string
	}{
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
			require.Equal(t, tc.want, tc.run(newViewFixture(t, "ViewTest")))
		})
	}
}
