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

package reindex_singlenode

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const opaquePromotionObjectCount = 30

// testPromotionRunsOnRecordedHandles is the executable form of the acceptance
// requirement that no directory name is ever inferred: a migration's live data
// is parked at a randomly named directory that no strategy, prefix table or
// generation suffix could reproduce, and only the record says where it is.
//
// A restart has to promote it to the canonical name and serve queries from it.
// Deriving the name instead finds nothing, and the property answers from an
// empty bucket while the schema reports it ready.
func testPromotionRunsOnRecordedHandles(t *testing.T, compose *docker.DockerCompose) {
	const class = "OpaquePromotion"
	ctx := context.Background()
	trueVal := true

	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "score", DataType: []string{"int"}, IndexFilterable: &trueVal},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	for i := 0; i < opaquePromotionObjectCount; i++ {
		score := 10
		if i%2 == 0 {
			score = 100
		}
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class, Properties: map[string]interface{}{"score": score},
		}))
	}
	require.Equal(t, opaquePromotionObjectCount/2, rangeFilterHits(t, class, "score", 50),
		"the fixture must serve before anything is moved")

	container := compose.GetWeaviate().Container()
	lsmPath := findShardPathInContainer(t, container, class) + "/lsm"

	// A name with a random infix: no prefix table, property name or generation
	// suffix in the codebase can produce it, so a reader that finds this
	// directory found it through the record.
	staged := "m_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	code, _, err := container.Exec(ctx, []string{
		"mv", lsmPath + "/property_score", lsmPath + "/" + staged,
	})
	require.NoError(t, err)
	require.Zero(t, code, "moving the live bucket aside must succeed")

	plantSwappedRecordAcrossRestart(t, compose, lsmPath, staged)

	require.Equal(t, opaquePromotionObjectCount/2, rangeFilterHits(t, class, "score", 50),
		"the recorded staged directory %q was not promoted to the canonical name; the "+
			"property is answering from a bucket that holds none of its data while the "+
			"schema reports it ready", staged)

	code, _, err = compose.GetWeaviate().Container().Exec(ctx, []string{"test", "-d", lsmPath + "/" + staged})
	require.NoError(t, err)
	require.NotZero(t, code, "the staged directory must be gone once its data is at the canonical name")
}

// plantSwappedRecordAcrossRestart writes the record of a migration whose flip
// decision is durable but whose promotion never ran, then restarts the node so
// reconciliation meets it at load. The staged directory is the one holding the
// live data; the canonical name is where promotion has to put it.
func plantSwappedRecordAcrossRestart(t *testing.T, compose *docker.DockerCompose, lsmPath, staged string) {
	t.Helper()
	ctx := context.Background()

	record := fmt.Sprintf(`{"formatVersion":1,"state":"swapped","subject":{`+
		`"key":{"taskVersion":4711,"strategyCode":"filterable_roaringset_refresh","unitID":"u0"},`+
		`"taskID":"opaque-promotion","migrationType":"repair-filterable",`+
		`"properties":["score"],"iterationCutoff":%q,"trackerDir":"opaque_promotion_tracker",`+
		`"stagedDirs":{"score":%q},"canonicalDirs":{"score":"property_score"}},`+
		`"flip":{"flipped":["score"],"displacedDirs":{"score":"property_score"}}}`,
		time.Now().UTC().Format(time.RFC3339Nano), staged)

	require.NoError(t, compose.StopAt(ctx, 0, nil),
		"graceful stop before planting the record must succeed")

	// CopyDirToContainer works against a stopped container; docker exec does not.
	stagedRoot := t.TempDir()
	dotMigrations := filepath.Join(stagedRoot, ".migrations")
	require.NoError(t, os.MkdirAll(filepath.Join(dotMigrations, "records"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(dotMigrations, "opaque_promotion_tracker"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dotMigrations, "records", "4711_filterable_roaringset_refresh.json"),
		[]byte(record), 0o666))

	require.NoError(t,
		compose.GetWeaviate().Container().CopyDirToContainer(ctx, dotMigrations, lsmPath+"/.migrations", 0o755),
		"CopyDirToContainer must succeed against the stopped container")

	require.NoError(t, compose.StartAt(ctx, 0), "restart after planting must succeed")
	helper.SetupClient(compose.GetWeaviate().URI())
}
