//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multishard"
)

func getWithCursorSearch(t *testing.T) {
	t.Run("listing objects using cursor api", func(t *testing.T) {
		tests := []struct {
			name             string
			className        string
			after            string
			limit            int
			filter           string
			expectedIDs      []strfmt.UUID
			expectedErrorMsg string
		}{
			{
				name:      `cursor with after: "" limit: 2`,
				className: "CursorClass",
				after:     "",
				limit:     2,
				expectedIDs: []strfmt.UUID{
					cursorClassID1,
					cursorClassID2,
					cursorClassID3,
					cursorClassID4,
					cursorClassID5,
					cursorClassID6,
					cursorClassID7,
				},
			},
			{
				name:      fmt.Sprintf("cursor with after: \"%s\" limit: 1", cursorClassID4),
				className: "CursorClass",
				after:     cursorClassID4.String(),
				limit:     1,
				expectedIDs: []strfmt.UUID{
					cursorClassID5,
					cursorClassID6,
					cursorClassID7,
				},
			},
			{
				name:             "error with offset",
				className:        "CursorClass",
				filter:           `limit: 1 after: "" offset: 1`,
				expectedErrorMsg: "cursor api: invalid 'after' parameter: offset cannot be set with after and limit parameters",
			},
			{
				name:             "error with nearObject",
				className:        "CursorClass",
				filter:           fmt.Sprintf("limit: 1 after: \"\" nearObject:{id:\"%s\"}", cursorClassID1),
				expectedErrorMsg: "cursor api: invalid 'after' parameter: other params cannot be set with after and limit parameters",
			},
			{
				name:             "error with nearVector",
				className:        "CursorClass",
				filter:           `limit: 1 after: "" nearVector:{vector:[0.1, 0.2]}`,
				expectedErrorMsg: "cursor api: invalid 'after' parameter: other params cannot be set with after and limit parameters",
			},
			{
				name:             "error with hybrid",
				className:        "CursorClass",
				filter:           `limit: 1 after: "" hybrid:{query:"cursor api"}`,
				expectedErrorMsg: "cursor api: invalid 'after' parameter: other params cannot be set with after and limit parameters",
			},
			{
				name:             "error with bm25",
				className:        "CursorClass",
				filter:           `limit: 1 after: "" bm25:{query:"cursor api"}`,
				expectedErrorMsg: "cursor api: invalid 'after' parameter: other params cannot be set with after and limit parameters",
			},
			{
				name:             "error with sort",
				className:        "CursorClass",
				filter:           `limit: 1 after: "" sort:{path:"name"}`,
				expectedErrorMsg: "cursor api: invalid 'after' parameter: sort cannot be set with after and limit parameters",
			},
			{
				name:             "error with where",
				className:        "CursorClass",
				filter:           `limit: 1 after: "" where:{path:"id" operator:Like valueText:"*"}`,
				expectedErrorMsg: "cursor api: invalid 'after' parameter: where cannot be set with after and limit parameters",
			},
			{
				name:             "error with bm25, hybrid and offset",
				className:        "CursorClass",
				filter:           `limit: 1 after: "" bm25:{query:"cursor api"} hybrid:{query:"cursor api"} offset:1`,
				expectedErrorMsg: "cursor api: invalid 'after' parameter: other params cannot be set with after and limit parameters",
			},
			{
				name:             "error with no limit set",
				className:        "CursorClass",
				filter:           `after:"00000000-0000-0000-0000-000000000000"`,
				expectedErrorMsg: "cursor api: invalid 'after' parameter: limit parameter must be set",
			},
			// multi shard
			{
				name:      `multi shard cursor with after: "" limit: 1`,
				className: "MultiShard",
				after:     "",
				limit:     1,
				expectedIDs: []strfmt.UUID{
					multishard.MultiShardID1,
					multishard.MultiShardID2,
					multishard.MultiShardID3,
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				query := "{ Get { " + tt.className + " %s { _additional { id } } } }"
				if len(tt.expectedErrorMsg) > 0 {
					errQuery := fmt.Sprintf(query, fmt.Sprintf("(%s)", tt.filter))
					result := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, errQuery)
					assert.Len(t, result, 1)

					errMsg := result[0].Message
					assert.Equal(t, tt.expectedErrorMsg, errMsg)
				} else {
					parseResults := func(t *testing.T, cities []interface{}) []strfmt.UUID {
						var ids []strfmt.UUID
						for _, city := range cities {
							id, ok := city.(map[string]interface{})["_additional"].(map[string]interface{})["id"]
							require.True(t, ok)

							idString, ok := id.(string)
							require.True(t, ok)

							ids = append(ids, strfmt.UUID(idString))
						}
						return ids
					}
					// use cursor api
					cursorSearch := func(t *testing.T, className, after string, limit int) []strfmt.UUID {
						cursor := fmt.Sprintf(`(limit: %v after: "%s")`, limit, after)
						result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, cursor))
						cities := result.Get("Get", className).AsSlice()
						return parseResults(t, cities)
					}

					var cursorIDs []strfmt.UUID
					after, limit := tt.after, tt.limit
					for {
						result := cursorSearch(t, tt.className, after, limit)
						cursorIDs = append(cursorIDs, result...)
						if len(result) == 0 {
							break
						}
						after = result[len(result)-1].String()
					}

					assert.ElementsMatch(t, tt.expectedIDs, cursorIDs)
					require.Equal(t, len(tt.expectedIDs), len(cursorIDs))
					for i := range tt.expectedIDs {
						assert.Equal(t, tt.expectedIDs[i], cursorIDs[i])
					}
				}
			})
		}
	})
}
