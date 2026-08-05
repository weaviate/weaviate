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

package lsmkv

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// The Get family logs unconditionally once a read crosses 100ms, so a Bucket
// without a logger panics there. Test files are in scope too: a bare literal is
// the only way a nil logger can reach that branch.
func TestEveryBucketLiteralSetsLogger(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	literals := 0

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") {
			continue
		}

		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)

		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if ident, ok := lit.Type.(*ast.Ident); !ok || ident.Name != "Bucket" {
				return true
			}

			literals++
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "logger" {
					return true
				}
			}

			t.Errorf("%s: Bucket literal does not set a logger", fset.Position(lit.Pos()))
			return true
		})
	}

	require.NotZero(t, literals, "no Bucket literals found, the scan is looking in the wrong place")
}

func TestBucketConstructorsSetLogger(t *testing.T) {
	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()

	newBucket := func(t *testing.T, logger logrus.FieldLogger) *Bucket {
		b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
			noopCB, noopCB, WithStrategy(StrategyReplace))
		require.NoError(t, err)
		return b
	}

	tests := []struct {
		name  string
		build func(t *testing.T, logger logrus.FieldLogger) *Bucket
	}{
		{
			name:  "NewBucket",
			build: newBucket,
		},
		{
			name: "NewSnapshotBucket",
			build: func(t *testing.T, logger logrus.FieldLogger) *Bucket {
				src := newBucket(t, logger)
				defer src.Shutdown(ctx)

				require.NoError(t, src.Put([]byte("key1"), []byte("value1")))
				require.NoError(t, src.FlushAndSwitch())

				dir, err := src.CreateSnapshot(ctx, t.TempDir(), "logger")
				require.NoError(t, err)

				b, err := NewSnapshotBucket(ctx, dir, logger, WithStrategy(StrategyReplace))
				require.NoError(t, err)
				return b
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.build(t, nullLogger())
			t.Cleanup(func() { _ = b.Shutdown(ctx) })

			require.NotNil(t, b.logger)
		})
	}
}

// slowMemtable pushes a read past the 100ms threshold that gates the slow-read
// logging, the only branch in the Get family that touches b.logger.
type slowMemtable struct {
	memtable
	delay time.Duration
}

func (m slowMemtable) get(key []byte) ([]byte, error) {
	time.Sleep(m.delay)
	return m.memtable.get(key)
}

// Returns NotFound rather than delegating: the fixture has no secondary-index
// arrays, and the branch under test runs in a defer regardless of the result.
func (m slowMemtable) getBySecondary(int, []byte) ([]byte, []byte, error) {
	time.Sleep(m.delay)
	return nil, nil, lsmkv.NotFound
}

func TestBucketSlowReadIsLogged(t *testing.T) {
	const delay = 110 * time.Millisecond

	tests := []struct {
		name   string
		action string
		read   func(t *testing.T, b *Bucket)
	}{
		{
			name:   "get",
			action: "lsm_bucket_get_active_memtable",
			read: func(t *testing.T, b *Bucket) {
				v, err := b.Get([]byte("key1"))
				require.NoError(t, err)
				require.Equal(t, []byte("value1"), v)
			},
		},
		{
			name:   "get_by_secondary",
			action: "lsm_bucket_getbysecondary_active_memtable",
			read: func(t *testing.T, b *Bucket) {
				_, err := b.GetBySecondary(context.Background(), 0, []byte("key1"))
				require.NoError(t, err)
			},
		},
		{
			name:   "acquire_flush_lock",
			action: "lsm_bucket_get_acquire_flush_lock",
			read: func(t *testing.T, b *Bucket) {
				held := make(chan struct{})
				released := make(chan struct{})

				go func() {
					b.flushLock.Lock()
					close(held)
					time.Sleep(delay)
					b.flushLock.Unlock()
					close(released)
				}()

				<-held
				b.GetConsistentView().ReleaseView()
				<-released
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)

			b := Bucket{
				active: slowMemtable{
					memtable: newTestMemtableReplace(map[string][]byte{"key1": []byte("value1")}),
					delay:    delay,
				},
				disk:     &SegmentGroup{strategy: StrategyReplace},
				strategy: StrategyReplace,
				// Without this getBySecondaryCore returns at its "no secondary
				// index at pos" guard and never reaches the branch under test.
				secondaryIndices: 1,
				logger:           logger,
			}

			tt.read(t, &b)

			var actions []string
			for _, entry := range hook.AllEntries() {
				actions = append(actions, fmt.Sprint(entry.Data["action"]))
			}
			require.Contains(t, actions, tt.action)
		})
	}
}
