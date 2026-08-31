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
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/helpers/reindexrecords"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const opaquePromotionObjectCount = 30

// opaquePromotionGeneration is the generation the recorded migration ran at,
// carried by every directory it names.
const opaquePromotionGeneration = 1

// testPromotionRunsOnRecordedHandles pins that no directory name is ever
// inferred: a migration's live data sits at a randomly named directory only
// the record can locate, and a restart must promote it to the canonical
// name from that record. Deriving the name instead finds nothing, serving
// an empty bucket under a schema that reports ready.
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

	// A random infix, so no strategy's suffix table can produce this name and a
	// reader that finds the directory found it through the record. The
	// surrounding shape is the writer's, because a record naming a staged
	// directory that is not shaped like a sidecar of a property bucket is
	// refused before it reaches disk.
	handles := reindexrecords.HandlesFor(t, db.StrategyCodeFilterableRoaringsetRefresh,
		"score", opaquePromotionGeneration)
	staged := fmt.Sprintf("%s__%s_ingest_%d", handles.Canonical,
		strings.ReplaceAll(uuid.NewString(), "-", ""), opaquePromotionGeneration)
	code, _, err := container.Exec(ctx, []string{
		"mv", lsmPath + "/" + handles.Canonical, lsmPath + "/" + staged,
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

	subject := opaqueMigrationSubject(t, staged)
	recordName, record := reindexrecords.Encode(t, db.NewMigrationRecordSwapped(
		subject, []string{"score"}, subject.CanonicalDirs))

	// Repoint on every exit path: the restart rebinds the host port, and a
	// failure in between would otherwise strand the client on the old one.
	defer func() { helper.SetupClient(compose.GetWeaviate().URI()) }()

	require.NoError(t, compose.StopAt(ctx, 0, nil),
		"graceful stop before planting the record must succeed")

	// CopyDirToContainer works against a stopped container; docker exec does not.
	stagedRoot := t.TempDir()
	dotMigrations := filepath.Join(stagedRoot, ".migrations")
	require.NoError(t, os.MkdirAll(filepath.Join(dotMigrations, "records"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(dotMigrations, "opaque_promotion_tracker"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dotMigrations, "records", recordName), []byte(record), 0o666))

	require.NoError(t,
		compose.GetWeaviate().Container().CopyDirToContainer(ctx, dotMigrations, lsmPath+"/.migrations", 0o755),
		"CopyDirToContainer must succeed against the stopped container")

	require.NoError(t, compose.StartAt(ctx, 0), "restart after planting must succeed")
}

// opaqueMigrationSubject is the one-property repair-filterable the planter
// records; staged is where its data currently sits.
func opaqueMigrationSubject(t *testing.T, staged string) db.MigrationSubject {
	t.Helper()

	handles := reindexrecords.HandlesFor(t, db.StrategyCodeFilterableRoaringsetRefresh,
		"score", opaquePromotionGeneration)
	return db.MigrationSubject{
		Key: db.MigrationRecordKey{
			TaskVersion:  4711,
			StrategyCode: db.StrategyCodeFilterableRoaringsetRefresh,
			UnitID:       "u0",
		},
		TaskID:          "opaque-promotion",
		MigrationType:   db.ReindexTypeRepairFilterable,
		Properties:      []string{"score"},
		IterationCutoff: time.Now().UTC(),
		TrackerDir:      "opaque_promotion_tracker",
		StagedDirs:      map[string]string{"score": staged},
		CanonicalDirs:   map[string]string{"score": handles.Canonical},
	}
}
