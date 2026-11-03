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

//go:build integrationTest

package integrationslowtest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestShardDropProceedsWhenDrainTimesOut pins that a drop outlives a pinned
// reference: it waits out the drain window, then proceeds and says so.
func TestShardDropProceedsWhenDrainTimesOut(t *testing.T) {
	const className = "Test"
	ctx := context.Background()

	logger, hook := test.NewNullLogger()
	repo, _ := newRepo(t, repoParams{logger: logger}, &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
	})
	for i := 0; i < 10; i++ {
		v := float32(i)
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
		require.NoError(t, repo.PutObject(ctx, &models.Object{Class: className, ID: id},
			[]float32{v, v + 1, v + 2, v + 3}, nil, nil, nil, 0))
	}

	index := repo.GetIndex(schema.ClassName(className))
	shard := singleShard(t, repo, className)
	_, release, err := index.GetShard(ctx, shard.Name()) // pin is never released before the drop
	require.NoError(t, err)
	defer release()

	start := time.Now()
	dropped := make(chan error, 1)
	go func() { dropped <- repo.DeleteIndex(schema.ClassName(className)) }()

	select {
	case err := <-dropped:
		require.NoError(t, err)
	case <-time.After(time.Minute):
		t.Fatal("drop never completed")
	}
	// ~30s window; near-instant means it never waited
	require.Greater(t, time.Since(start), 10*time.Second, "drop gave up well short of the drain window")

	var warned bool
	for _, e := range hook.AllEntries() {
		warned = warned || (e.Level == logrus.ErrorLevel &&
			strings.Contains(e.Message, "proceeding with drop while references are still held"))
	}
	require.True(t, warned, "a drop that outran its drain must be logged, not silent")
}
