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

package test

import (
	"fmt"
	"testing"

	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
)

// ransomNoteAnchorID is the RansomNote object the nearObject cases below search
// from. The notes are random and get new UUIDs on every run, so picking one of
// them at query time makes the neighbour count depend on which note came back.
// The fixture pins this one instead, see addTestDataRansomNotes.
const ransomNoteAnchorID = "11111111-1111-4111-8111-111111111111"

// ransomNoteAnchorContents is fixed, so the anchor's vector does not move with
// the random contents of the other notes.
const ransomNoteAnchorContents = "qzv wkpt xjr bnmq lfd hgty zzrk pwoq vbnx mmtr"

// ransomNoteAnchorVector returns the vector of ransomNoteAnchorID as a GraphQL
// literal, so nearObject and nearVector search from the same point and cover
// the same neighbourhood.
func ransomNoteAnchorVector(t *testing.T) string {
	obj, err := helper.GetObject(t, "RansomNote", ransomNoteAnchorID, "vector")
	require.NoError(t, err)
	require.NotEmpty(t, obj.Vector)
	return graphqlhelper.Vec2String(obj.Vector)
}

func gettingObjectsWithNearFields(t *testing.T) {
	defaultLimit := 100

	// nearVector

	t.Run("nearVector: with implicit unlimited search - no limit provided (with distance)", func(t *testing.T) {
		query := `
		{
			Get {
				RansomNote(
					nearVector: {
						distance: 1.8
						vector: ` + ransomNoteAnchorVector(t) + `
					}
				) {
					_additional {
						vector
					}
					contents
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Greater(t, len(notes), defaultLimit)
	})

	t.Run("nearVector: with implicit unlimited search - no limit provided (with certainty)", func(t *testing.T) {
		query := `
		{
			Get {
				RansomNote(
					nearVector: {
						certainty: 0.1
						vector: ` + ransomNoteAnchorVector(t) + `
					}
				) {
					_additional {
						vector
					}
					contents
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Greater(t, len(notes), defaultLimit)
	})

	t.Run("nearVector: with implicit unlimited search - negative limit provided (with distance)", func(t *testing.T) {
		query := `
		{
			Get {
				RansomNote(
					limit: -1
					nearVector: {
						distance: 0.9
						vector: ` + ransomNoteAnchorVector(t) + `
					}
				) {
					_additional {
						vector
					}
					contents
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Greater(t, len(notes), defaultLimit)
	})

	t.Run("nearVector: with implicit unlimited search - negative limit provided (with certainty)", func(t *testing.T) {
		query := `
		{
			Get {
				RansomNote(
					limit: -1
					nearVector: {
						certainty: 0.1
						vector: ` + ransomNoteAnchorVector(t) + `
					}
				) {
					_additional {
						vector
					}
					contents
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Greater(t, len(notes), defaultLimit)
	})

	t.Run("nearVector: with limited search - limit provided (with distance)", func(t *testing.T) {
		query := `
		{
			Get {
				RansomNote(
					limit: 10
					nearVector: {
						distance: 0.9
						vector: ` + ransomNoteAnchorVector(t) + `
					}
				) {
					_additional {
						vector
					}
					contents
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Equal(t, 10, len(notes))
	})

	t.Run("nearVector: with limited search - limit provided (with certainty)", func(t *testing.T) {
		query := `
		{
			Get {
				RansomNote(
					limit: 10
					nearVector: {
						certainty: 0.1
						vector: ` + ransomNoteAnchorVector(t) + `
					}
				) {
					_additional {
						vector
					}
					contents
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Equal(t, 10, len(notes))
	})

	t.Run("nearVector: results limited by distance", func(t *testing.T) {
		query := `
		{
			Get {
				RansomNote(
					nearVector: {
						distance: 0.01
						vector: ` + ransomNoteAnchorVector(t) + `
					}
				) {
					_additional {
						id
					}
					contents
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Less(t, len(notes), defaultLimit)
	})

	t.Run("nearVector: results limited by certainty", func(t *testing.T) {
		query := `
		{
			Get {
				RansomNote(
					nearVector: {
						certainty: 0.99
						vector: ` + ransomNoteAnchorVector(t) + `
					}
				) {
					_additional {
						id
					}
					contents
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Less(t, len(notes), defaultLimit)
	})

	// nearObject

	nearObjID := ransomNoteAnchorID

	t.Run("nearObject: with implicit unlimited search - no limit provided (with distance)", func(t *testing.T) {
		query := fmt.Sprintf(`
		{
			Get {
				RansomNote(
					nearObject: {
						distance: 1.8
						id: "%s"
					}
				) {
					contents
				}
			}
		}
		`, nearObjID)
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Greater(t, len(notes), defaultLimit)
	})

	t.Run("nearObject: with implicit unlimited search - no limit provided (with certainty)", func(t *testing.T) {
		query := fmt.Sprintf(`
		{
			Get {
				RansomNote(
					nearObject: {
						certainty: 0.1
						id: "%s"
					}
				) {
					contents
				}
			}
		}
		`, nearObjID)
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Greater(t, len(notes), defaultLimit)
	})

	t.Run("nearObject: with implicit unlimited search - negative limit provided (with distance)", func(t *testing.T) {
		query := fmt.Sprintf(`
		{
			Get {
				RansomNote(
					limit: -1
					nearObject: {
						distance: 0.9
						id: "%s"
					}
				) {
					contents
				}
			}
		}
		`, nearObjID)
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Greater(t, len(notes), defaultLimit)
	})

	t.Run("nearObject: with implicit unlimited search - negative limit provided (with certainty)", func(t *testing.T) {
		query := fmt.Sprintf(`
		{
			Get {
				RansomNote(
					limit: -1
					nearObject: {
						certainty: 0.1
						id: "%s"
					}
				) {
					contents
				}
			}
		}
		`, nearObjID)
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Greater(t, len(notes), defaultLimit)
	})

	t.Run("nearObject: with limited search - limit provided (with distance)", func(t *testing.T) {
		query := fmt.Sprintf(`
		{
			Get {
				RansomNote(
					limit: 13
					nearObject: {
						distance: 0.9
						id: "%s"
					}
				) {
					contents
				}
			}
		}
		`, nearObjID)
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Equal(t, 13, len(notes))
	})

	t.Run("nearObject: with limited search - limit provided (with certainty)", func(t *testing.T) {
		query := fmt.Sprintf(`
		{
			Get {
				RansomNote(
					limit: 13
					nearObject: {
						certainty: 0.1
						id: "%s"
					}
				) {
					contents
				}
			}
		}
		`, nearObjID)
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Equal(t, 13, len(notes))
	})

	t.Run("nearObject: results limited by distance", func(t *testing.T) {
		query := fmt.Sprintf(`
		{
			Get {
				RansomNote(
					nearObject: {
						distance: 0.01
						id: "%s"
					}
				) {
					_additional {
						id
					}
					contents
				}
			}
		}
		`, nearObjID)

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Less(t, len(notes), defaultLimit)
	})

	t.Run("nearObject: results limited by certainty", func(t *testing.T) {
		query := fmt.Sprintf(`
			{
				Get {
					RansomNote(
						nearObject: {
							certainty: 0.99
							id: "%s"
						}
					) {
						_additional {
							id
						}
						contents
					}
				}
			}
			`, nearObjID)

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Less(t, len(notes), defaultLimit)
	})

	// nearText

	t.Run("nearText: with implicit unlimited search - no limit provided (with distance)", func(t *testing.T) {
		query := `
			{
				Get {
					RansomNote(
						nearText: {
							distance: 1.8
							concepts: ["abcd"]
						}
					) {
						contents
					}
				}
			}
			`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Greater(t, len(notes), defaultLimit)
	})

	t.Run("nearText: with implicit unlimited search - no limit provided (with certainty)", func(t *testing.T) {
		query := `
			{
				Get {
					RansomNote(
						nearText: {
							certainty: 0.1
							concepts: ["abcd"]
						}
					) {
						contents
					}
				}
			}
			`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Greater(t, len(notes), defaultLimit)
	})

	t.Run("nearText: with implicit unlimited search - negative limit provided (with distance)", func(t *testing.T) {
		query := `
				{
					Get {
						RansomNote(
							limit: -1
							nearText: {
								distance: 1.8
								concepts: ["abcd"]
							}
						) {
							contents
						}
					}
				}
				`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Greater(t, len(notes), defaultLimit)
	})

	t.Run("nearText: with implicit unlimited search - negative limit provided (with certainty)", func(t *testing.T) {
		query := `
				{
					Get {
						RansomNote(
							limit: -1
							nearText: {
								certainty: 0.1
								concepts: ["abcd"]
							}
						) {
							contents
						}
					}
				}
				`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Greater(t, len(notes), defaultLimit)
	})

	t.Run("nearText: with limited search - limit provided (with distance)", func(t *testing.T) {
		query := `
				{
					Get {
						RansomNote(
							limit: 5
							nearText: {
								distance: 0.9
								concepts: ["abcd"]
							}
						) {
							contents
						}
					}
				}
				`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Equal(t, 5, len(notes))
	})

	t.Run("nearText: with limited search - limit provided (with certainty)", func(t *testing.T) {
		query := `
				{
					Get {
						RansomNote(
							limit: 5
							nearText: {
								certainty: 0.1
								concepts: ["abcd"]
							}
						) {
							contents
						}
					}
				}
				`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.NotEmpty(t, notes)
		require.Equal(t, 5, len(notes))
	})

	t.Run("nearText: results limited by distance", func(t *testing.T) {
		query := `
				{
					Get {
						RansomNote(
							nearText: {
								distance: 0.2
								concepts: ["abcd"]
							}
						) {
							_additional {
								id
							}
							contents
						}
					}
				}
				`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.Less(t, len(notes), defaultLimit)
	})

	t.Run("nearText: results limited by certainty", func(t *testing.T) {
		query := `
				{
					Get {
						RansomNote(
							nearText: {
								certainty: 0.9
								concepts: ["abcd"]
							}
						) {
							_additional {
								id
							}
							contents
						}
					}
				}
				`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "RansomNote").AsSlice()
		require.Less(t, len(notes), defaultLimit)
	})
}

func gettingObjectsWithNearFieldsMultiShard(t *testing.T) {
	t.Run("nearText: results limited by distance with multi shard", func(t *testing.T) {
		query := `
				{
					Get {
						MultiShard(
							nearText: {
								distance: 0.9
								concepts: ["multi shard"]
							}
						) {
							_additional {
								id
							}
						}
					}
				}
				`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "MultiShard").AsSlice()
		require.Equal(t, len(notes), 3)
	})

	t.Run("nearText: results limited by certainty with multi shard", func(t *testing.T) {
		query := `
				{
					Get {
						MultiShard(
							nearText: {
								certainty: 0.1
								concepts: ["multi shard"]
							}
						) {
							_additional {
								id
							}
						}
					}
				}
				`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		notes := result.Get("Get", "MultiShard").AsSlice()
		require.Equal(t, len(notes), 3)
	})
}
