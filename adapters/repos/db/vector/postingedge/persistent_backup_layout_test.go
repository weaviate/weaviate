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

package postingedge

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// While a backup holds the index no pass over its vectors starts, and one
// that runs stops at its next step and goes on once the backup lets go: a
// pass ends by naming another generation of the files in the meta and
// removing those of the one before, which the backup links.
func TestPersistentBackupHoldsTheLayout(t *testing.T) {
	ctx := context.Background()
	const total = 2*persistentBase + 300
	var hook atomic.Pointer[func(step int)]
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) {
		cfg.noBackground = true
		cfg.layoutHook = func(step int) {
			if h := hook.Load(); h != nil {
				(*h)(step)
			}
		}
	})
	f.add(t, 0, total)
	generation := func() int {
		f.index.mu.RLock()
		defer f.index.mu.RUnlock()
		return f.index.g.meta.VectorGen
	}
	require.True(t, f.index.g.layoutDue())
	gen := generation()

	require.NoError(t, f.index.PrepareForBackup(ctx))
	require.NoError(t, f.index.maintain(ctx, true))
	require.NoError(t, f.index.Settle(ctx), "nothing is due while the backup holds the index")
	require.Equal(t, gen, generation(), "no pass starts during a backup")
	require.Zero(t, f.index.layouts.Load())
	require.NoError(t, f.index.ResumeAfterBackup(ctx))

	stepped := make(chan struct{})
	h := func(step int) {
		if step == 1 {
			assert.NoError(t, f.index.PrepareForBackup(ctx))
			close(stepped)
		}
	}
	hook.Store(&h)
	done := make(chan error, 1)
	enterrors.GoWrapper(func() { done <- f.index.layOut(ctx) }, f.cfg.Logger)
	select {
	case <-stepped:
	case <-time.After(30 * time.Second):
		t.Fatal("the pass does not start")
	}
	select {
	case err := <-done:
		t.Fatalf("the pass ended while a backup held the index: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	require.Equal(t, gen, generation())
	listed, err := f.index.ListFiles(ctx, f.root)
	require.NoError(t, err)
	dir := helpers.PostingEdgeDirName(f.cfg.ID)
	assert.Contains(t, listed, filepath.Join(dir, growVectorsName(gen)))
	assert.NotContains(t, listed, filepath.Join(dir, growVectorsName(gen+1)))

	require.NoError(t, f.index.ResumeAfterBackup(ctx))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("the pass does not go on after the backup")
	}
	require.Equal(t, gen+1, generation())
	f.covered(t, "laid out after the backup")
	f.vectorsIntact(t, "laid out after the backup")
}
