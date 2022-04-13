//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"strings"
	"testing"

	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func aggregatesWithExpectedFailures(t *testing.T) {
	t.Run("with nearVector, no certainty", func(t *testing.T) {
		result := ErrorGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					CustomVectorClass(
						nearVector: {
							vector: [1,0,0]
						}
					){
						meta {
							count
						}
						name {
							topOccurrences {
								occurs
								value
							}
							type
							count
						}
					}
				}
			}
		`)

		require.NotEmpty(t, result)
		require.Len(t, result, 1)
		assert.True(t, strings.Contains(result[0].Message, "must provide certainty with vector search"))
	})

	t.Run("with nearObject, no certainty", func(t *testing.T) {
		result := ErrorGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					City(
						nearObject: {
							id: "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
						}
					){
						meta {
							count
						}
						name {
							topOccurrences {
								occurs
								value
							}
							type
							count
						}
					}
				}
			}
		`)

		require.NotEmpty(t, result)
		require.Len(t, result, 1)
		assert.True(t, strings.Contains(result[0].Message, "must provide certainty with vector search"))
	})

	t.Run("with nearText, no certainty", func(t *testing.T) {
		result := ErrorGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					City(
						nearText: {
							concepts: ["Amsterdam"]
						}
					){
						meta {
							count
						}
						name {
							topOccurrences {
								occurs
								value
							}
							type
							count
						}
					}
				}
			}
		`)

		require.NotEmpty(t, result)
		require.Len(t, result, 1)
		assert.True(t, strings.Contains(result[0].Message, "must provide certainty with vector search"))
	})

	// with where

	t.Run("with nearVector, where filter, no certainty", func(t *testing.T) {
		result := ErrorGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					CustomVectorClass(
						where: {
							valueString: "Mercedes",
							operator: Equal,
							path: ["name"]
						}
						nearVector: {
							vector: [1,0,0]
						}
					){
						meta {
							count
						}
						name {
							topOccurrences {
								occurs
								value
							}
							type
							count
						}
					}
				}
			}
		`)

		require.NotEmpty(t, result)
		require.Len(t, result, 1)
		assert.True(t, strings.Contains(result[0].Message, "must provide certainty with vector search"))
	})

	t.Run("with nearObject, where filter, no certainty", func(t *testing.T) {
		result := ErrorGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					City (where: {
						valueBoolean: true,
						operator: Equal,
						path: ["isCapital"]
					}
					nearObject: {
						id: "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
					}
					){
						meta {
							count
						}
						isCapital {
							count
							percentageFalse
							percentageTrue
							totalFalse
							totalTrue
							type
						}
						population {
							mean
							count
							maximum
							minimum
							sum
							type
						}
						inCountry {
							pointingTo
							type
						}
						name {
							topOccurrences {
								occurs
								value
							}
							type
							count
						}
					}
				}
			}
		`)

		require.NotEmpty(t, result)
		require.Len(t, result, 1)
		assert.True(t, strings.Contains(result[0].Message, "must provide certainty with vector search"))
	})

	t.Run("with nearText, where filter, no certainty", func(t *testing.T) {
		result := ErrorGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					City (where: {
						valueBoolean: true,
						operator: Equal,
						path: ["isCapital"]
					}
					nearText: {
						concepts: ["Amsterdam"]
					}
					){
						meta {
							count
						}
						isCapital {
							count
							percentageFalse
							percentageTrue
							totalFalse
							totalTrue
							type
						}
						population {
							mean
							count
							maximum
							minimum
							sum
							type
						}
						inCountry {
							pointingTo
							type
						}
						name {
							topOccurrences {
								occurs
								value
							}
							type
							count
						}
					}
				}
			}
		`)

		require.NotEmpty(t, result)
		require.Len(t, result, 1)
		assert.True(t, strings.Contains(result[0].Message, "must provide certainty with vector search"))
	})
}
