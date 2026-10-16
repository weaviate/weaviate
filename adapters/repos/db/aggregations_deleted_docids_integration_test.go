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

package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
)

// TestAggregate_DenyListCountSkipsDeletedDocIDs pins that a deny-list filter (NotEqual,
// Not) never counts a deleted object. The doc id universe a deny-list filter inverts
// against is rebuilt at shard init from the doc id counter, so it holds every id ever
// allocated, deleted ones included. Object reads drop a dead id when they meet it, but a
// meta count reads no object, so before the fix a restart brought every deleted object
// back into the count (gh-10260).
func TestAggregate_DenyListCountSkipsDeletedDocIDs(t *testing.T) {
	const (
		inserted = 10
		deleted  = 4
		live     = inserted - deleted
	)

	notEqualAbsent := &filters.Clause{
		Operator: filters.OperatorNotEqual,
		On: &filters.Path{
			Class:    batchDeleteClassName,
			Property: schema.PropertyName("stringProp"),
		},
		Value: &filters.Value{Value: "absent", Type: schema.DataTypeText},
	}
	notOfEqualAbsent := &filters.Clause{
		Operator: filters.OperatorNot,
		Operands: []filters.Clause{{
			Operator: filters.OperatorEqual,
			On: &filters.Path{
				Class:    batchDeleteClassName,
				Property: schema.PropertyName("stringProp"),
			},
			Value: &filters.Value{Value: "absent", Type: schema.DataTypeText},
		}},
	}

	tests := []struct {
		name    string
		filter  *filters.Clause
		restart bool
	}{
		{name: "NotEqual, same process", filter: notEqualAbsent},
		{name: "NotEqual, after restart", filter: notEqualAbsent, restart: true},
		{name: "Not(Equal), same process", filter: notOfEqualAbsent},
		{name: "Not(Equal), after restart", filter: notOfEqualAbsent, restart: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			rootDir := t.TempDir()
			shardState := singleShardState()

			repo := newBatchDeleteRepoAt(t, rootDir, batchDeleteTestClass(false), shardState, 0)
			insertBatchDeleteObjects(t, repo, 0, inserted)
			for i := 0; i < deleted; i++ {
				require.NoError(t, repo.DeleteObject(ctx, batchDeleteClassName,
					batchDeleteObjectID(i), time.Now(), nil, "", 0))
			}

			if tt.restart {
				require.NoError(t, repo.Shutdown(ctx))
				repo = newBatchDeleteRepoAt(t, rootDir, batchDeleteTestClass(false), shardState, 0)
			}
			t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })

			params := aggregation.Params{
				ClassName:        schema.ClassName(batchDeleteClassName),
				Filters:          &filters.LocalFilter{Root: tt.filter},
				IncludeMetaCount: true,
			}

			// Twice: a count must neither depend on nor be changed by an earlier count.
			for call := 0; call < 2; call++ {
				res, err := repo.Aggregate(ctx, params, nil)
				require.NoError(t, err)
				require.Len(t, res.Groups, 1)
				require.Equal(t, live, res.Groups[0].Count,
					"call %d counts only the objects that still exist", call)
			}

			// Grouped: every object has its own stringProp value, so each live object is
			// a group of one and a deleted one must not show up as a group.
			grouped := params
			grouped.GroupBy = &filters.Path{
				Class:    schema.ClassName(batchDeleteClassName),
				Property: schema.PropertyName("stringProp"),
			}
			res, err := repo.Aggregate(ctx, grouped, nil)
			require.NoError(t, err)
			require.Len(t, res.Groups, live, "one group per live object")
			for _, g := range res.Groups {
				require.Equal(t, 1, g.Count, "group %v", g.GroupedBy.Value)
			}
		})
	}
}
