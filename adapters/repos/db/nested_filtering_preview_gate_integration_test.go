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

// This file proves the load-bearing constraint of the
// WEAVIATE_PREVIEW_NESTED_FILTERING gate: when the flag is off there are
// no persistent data side effects (no nested filterable / meta buckets
// created, no inverted index entries appended) and any filter request
// surfaces an actionable user-facing error.
//
// The file is part of the temporary preview gate and goes away with the
// gate at GA. See entities/config/feature_flags.go.

package db

import (
	"context"
	"os"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Nested filtering is preview-gated. Enable the gate at package init via
// the env var so all nested tests in this binary see the gate as
// enabled. This file's own test overrides per-case via t.Setenv (which
// auto-restores via cleanup and panics on t.Parallel()).
func init() { os.Setenv(entcfg.EnvNestedFilteringPreview, "true") }

// TestNestedFilteringPreviewGateOff proves the load-bearing constraint
// of the preview gate. With the gate off:
//
//   - writes succeed at the doc-store level (no panic, no error)
//   - no nested filterable value bucket or meta bucket is created in
//     the shard store
//   - any nested filter request returns the user-facing
//     "preview disabled" error including the env-var name so the user
//     knows how to opt in
//
// Cycle behaviour (off → on with previously-written data) is
// intentionally out of scope here — the gate-off window leaves no
// nested infrastructure on the shard, so a later flip-on doesn't
// produce empty results but rather "bucket not found" until the shard
// is recreated. Documented as accepted behaviour for the preview
// (see entities/config/feature_flags.go and the gate memo).
func TestNestedFilteringPreviewGateOff(t *testing.T) {
	// The package init sets the gate-on env var for the rest of this
	// binary; t.Setenv overrides it to off for this test only, restores
	// on cleanup, and panics if a parallel test or a parallel ancestor
	// tries the same — automatic isolation against future t.Parallel()
	// in this package.
	t.Setenv(entcfg.EnvNestedFilteringPreview, "")

	const className = "NestedFilteringPreviewGateOff"
	vTrue := true
	class := &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{
				Name: "cars", DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{
						Name:            "make",
						DataType:        schema.DataTypeText.PropString(),
						Tokenization:    models.NestedPropertyTokenizationField,
						IndexFilterable: &vTrue,
					},
				},
			},
		},
	}
	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
	ctx := context.Background()

	t.Run("writes succeed without nested analysis", func(t *testing.T) {
		// The nested analyzer skip in adapters/repos/db/inverted/objects.go
		// does not return an error; it silently continues so the doc-store
		// write completes normally.
		objID := strfmt.UUID("00000000-0000-0000-0000-000000000001")
		require.NoError(t, db.PutObject(ctx, &models.Object{
			Class: className, ID: objID,
			Properties: map[string]any{
				"cars": []any{map[string]any{"make": "Toyota"}},
			},
		}, nil, nil, nil, nil, 0))
	})

	t.Run("no nested buckets created in shard store", func(t *testing.T) {
		// The bucket-creation skip in shard_init_properties.go avoids the
		// goroutine entirely; createNestedPropertyBuckets is never reached,
		// so the buckets are not in the store.
		idx := db.GetIndex(schema.ClassName(className))
		require.NotNil(t, idx, "index for the test class should exist")
		var shard ShardLike
		require.NoError(t, idx.ForEachShard(func(_ string, s ShardLike) error {
			shard = s
			return nil
		}))
		require.NotNil(t, shard, "test database should expose at least one shard")
		assert.Nil(t, shard.Store().Bucket(helpers.BucketNestedFromPropNameLSM("cars")),
			"nested filterable value bucket must not exist when gate is off")
		assert.Nil(t, shard.Store().Bucket(helpers.BucketNestedMetaFromPropNameLSM("cars")),
			"nested meta bucket must not exist when gate is off")
	})

	t.Run("filter returns user-facing preview-disabled error", func(t *testing.T) {
		// Validator gate at the top of filters_validator.go. The message
		// must name the env var so users know how to opt in.
		_, err := db.Search(ctx, dto.GetParams{
			ClassName:  className,
			Pagination: &filters.Pagination{Limit: 10},
			Filters: &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				Value:    &filters.Value{Type: schema.DataTypeText, Value: "Toyota"},
				On:       &filters.Path{Class: schema.ClassName(className), Property: "cars.make"},
			}},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), entcfg.EnvNestedFilteringPreview,
			"error must name the env var so users can opt in")
		assert.Contains(t, err.Error(), "preview",
			"error must identify nested filtering as a preview feature")
	})
}
