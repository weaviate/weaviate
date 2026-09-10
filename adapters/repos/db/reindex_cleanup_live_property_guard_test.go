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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// The property whose main bucket wears a sidecar's name. Sweeping "category"
// looks for exactly "property_category__enable_filterable_ingest_1", and that
// is byte-for-byte this property's own filterable bucket.
const (
	liveVictimProp = "category__enable_filterable_ingest_1"
	// A cancelled attempt's real leftovers, at a generation no property
	// claims. Present so a guard that simply stopped sweeping would fail
	// these tests rather than pass them.
	staleSidecarDir = "property_category__enable_filterable_ingest_2"
)

func liveVictimBucket() string {
	return helpers.BucketFromPropNameLSM(liveVictimProp)
}

// weaviate/weaviate#12621, the half no filename rule can close: matching the
// whole suffix against the strategy registry stops the sweep at properties
// whose names merely end in a role word, but a property named after a whole
// suffix still collides exactly. The sweep has to ask the class who owns the
// dir before it removes it.
//
// Without the guard this test fails twice over: the bucket is shut down in
// step 1 and its directory — the property's live data — is removed in step 2.
func TestCleanStalePartialReindexStateKeepsALivePropertysOwnMainBucket(t *testing.T) {
	ctx := testCtx()
	className := "LivePropGuard" + uuid.NewString()[:8]
	shd, _ := testShardWithSettings(t, ctx,
		newTestClassWithProps(className, []string{"category", liveVictimProp}),
		enthnsw.UserConfig{Skip: true}, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(testCtx()) })

	victimPath := filepath.Join(shard.pathLSM(), liveVictimBucket())
	require.DirExists(t, victimPath,
		"fixture: the colliding property has to own a bucket before the sweep can take it")
	require.NotNil(t, shard.store.Bucket(liveVictimBucket()),
		"fixture: that bucket has to be loaded, so step 1 has something to shut down")

	sidecarPath := filepath.Join(shard.pathLSM(), staleSidecarDir)
	require.NoError(t, os.MkdirAll(sidecarPath, 0o755))
	mkTrackerDir(t, shard.pathLSM(), "enable_filterable_category_2", "started.mig")

	_, err := shard.CleanStalePartialReindexState(ctx, "category", "filterable")
	require.NoError(t, err)

	require.DirExists(t, victimPath,
		"the sweep removed a live property's own main bucket dir: #12621 data loss")
	require.NotNil(t, shard.store.Bucket(liveVictimBucket()),
		"the sweep shut down a live property's own main bucket")
	require.NoDirExists(t, sidecarPath,
		"the guard cannot buy safety by refusing to sweep: a real stale sidecar still goes")
}

// The DELETE path (updatePropertyBuckets → cleanStaleSidecarDirs) sweeps the
// same dirs with the same matcher, so it needs the same guard. Exercised
// through the helper both callers share.
func TestCleanStaleSidecarDirsKeepsALivePropertysOwnMainBucket(t *testing.T) {
	ctx := testCtx()
	className := "LivePropGuardDelete" + uuid.NewString()[:8]
	shd, _ := testShardWithSettings(t, ctx,
		newTestClassWithProps(className, []string{"category", liveVictimProp}),
		enthnsw.UserConfig{Skip: true}, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(testCtx()) })

	victimPath := filepath.Join(shard.pathLSM(), liveVictimBucket())
	require.DirExists(t, victimPath)

	sidecarPath := filepath.Join(shard.pathLSM(), staleSidecarDir)
	require.NoError(t, os.MkdirAll(sidecarPath, 0o755))

	shard.cleanStaleSidecarDirs(helpers.BucketFromPropNameLSM("category"), shard.liveMainBuckets())

	require.DirExists(t, victimPath,
		"the DELETE-path sweep removed a live property's own main bucket dir")
	require.NoDirExists(t, sidecarPath,
		"the DELETE-path sweep still has to remove a real stale sidecar")
}

// The guard reads the class, so what it collects out of one is worth pinning
// on its own: every bucket name a property owns, whatever its index flags
// currently say, because the guarantee is about the property existing.
func TestMainBucketNamesOfCollectsEveryBucketAPropertyOwns(t *testing.T) {
	names := mainBucketNamesOf(&models.Class{
		Class:      "Article",
		Properties: []*models.Property{{Name: "title"}, nil},
	})

	for _, want := range []string{
		helpers.BucketFromPropNameLSM("title"),
		helpers.BucketSearchableFromPropNameLSM("title"),
		helpers.BucketRangeableFromPropNameLSM("title"),
		helpers.BucketFromPropNameLengthLSM("title"),
		helpers.BucketFromPropNameNullLSM("title"),
		helpers.BucketFromPropNameMetaCountLSM("title"),
	} {
		require.Truef(t, names[want], "%q is a live property's bucket and must be excluded", want)
	}
	require.False(t, names[helpers.BucketFromPropNameLSM("other")],
		"a property the class does not have owns nothing")
}

// An unreadable class is told apart from a class with no properties, and the
// sweep then behaves as it did before the guard. Failing closed instead would
// leave the slate dirty for the next submit, which is the Sev 1 the sweep
// exists to prevent; an unreadable class here means the class is going away.
func TestTheLiveBucketGuardFailsOpenOnAnUnreadableClass(t *testing.T) {
	require.Nil(t, mainBucketNamesOf(nil),
		"an unreadable class has to be distinguishable from one with no properties")
	require.NotNil(t, mainBucketNamesOf(&models.Class{Class: "Empty"}),
		"a class with no properties is readable, and excludes nothing")

	guard := &liveMainBuckets{built: true, names: mainBucketNamesOf(nil)}
	require.False(t, guard.has(helpers.BucketFromPropNameLSM("anything")),
		"with no class to consult the sweep goes ahead, as it did before the guard")
}
