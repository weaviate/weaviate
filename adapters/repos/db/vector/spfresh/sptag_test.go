//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

// func TestBruteForceSPTAG_Search(t *testing.T) {
// 	dim := 64
// 	q := compressionhelpers.NewRotationalQuantizer(dim, 42, 8, distancer.NewL2SquaredProvider())
// 	sptag := NewBruteForceSPTAG(NewMetrics(nil, "n/a", "n/a"), 10, 10)
// 	sptag.Init(int32(dim), distancer.NewL2SquaredProvider())

// 	// Seed vectors
// 	vectors := map[uint64][]float32{
// 		1: make([]float32, dim),
// 		2: make([]float32, dim),
// 		3: make([]float32, dim),
// 	}

// 	// Populate with some deterministic values for reproducibility
// 	for i := range dim {
// 		vectors[1][i] = float32(i)
// 		vectors[2][i] = float32(i + 10)
// 		vectors[3][i] = float32(2*i + 10)
// 	}

// 	// Encode and upsert
// 	for id, v := range vectors {
// 		encoded := q.Encode(v)
// 		err := sptag.Insert(id, NewAnonymousCompressedVector(encoded))
// 		require.NoError(t, err)
// 	}

// 	// Use a query close to vector 2
// 	query := make([]float32, dim)
// 	for i := range dim {
// 		query[i] = float32(i + 11) // close to vector 2
// 	}
// 	encodedQuery := q.Encode(query)

// 	results, err := sptag.Search(NewAnonymousCompressedVector(encodedQuery), 2)
// 	require.NoError(t, err)
// 	require.True(t, len(results.data) >= 1)

// 	// Vector 2 should be one of the closest
// 	require.Equal(t, uint64(2), results.data[0].ID)
// 	require.NotZero(t, results.data[0].Distance)

// 	// Delete vector 2 and search again
// 	err = sptag.MarkAsDeleted(2)
// 	require.NoError(t, err)

// 	results, err = sptag.Search(NewAnonymousCompressedVector(encodedQuery), 2)
// 	require.NoError(t, err)
// 	require.NotContains(t, results.data, uint64(2))

// 	// Ensure other vectors are still present
// 	require.Equal(t, uint64(1), results.data[0].ID)
// 	require.Equal(t, uint64(3), results.data[1].ID)

// 	// Test with an empty search
// 	results, err = sptag.Search(NewAnonymousCompressedVector(encodedQuery), 0)
// 	require.NoError(t, err)
// 	require.Empty(t, results)

// 	// Get existing vector
// 	existingVector := sptag.Get(1)
// 	require.NotNil(t, existingVector)
// 	require.Equal(t, q.Encode(vectors[1]), existingVector.Vector.(CompressedVector).Data())
// 	require.True(t, sptag.Exists(1))

// 	// Get non-existing vector
// 	nonExistingVector := sptag.Get(999)
// 	require.Nil(t, nonExistingVector)
// 	require.False(t, sptag.Exists(999))
// }

// func BenchmarkSPTAGSearch(b *testing.B) {
// 	dim := 64
// 	q := compressionhelpers.NewRotationalQuantizer(dim, 42, 8, distancer.NewL2SquaredProvider())
// 	sptag := NewBruteForceSPTAG(NewMetrics(nil, "n/a", "n/a"), 1024*1024, 1024)
// 	sptag.Init(int32(dim), distancer.NewL2SquaredProvider())
// 	logger, _ := test.NewNullLogger()
// 	vectors_size := 1000_000
// 	queries_size := 100

// 	vectors, _ := testinghelpers.RandomVecsFixedSeed(vectors_size, queries_size, dim)

// 	for i, v := range vectors {
// 		encoded := q.Encode(v)
// 		err := sptag.Insert(uint64(i), NewAnonymousCompressedVector(encoded))
// 		require.NoError(b, err)
// 	}

// 	b.ResetTimer()
// 	for b.Loop() {
// 		compressionhelpers.Concurrently(logger, 100, func(taskIndex uint64) {
// 			sptag.Search(NewAnonymousCompressedVector(q.Encode(vectors[0])), 64)
// 		})
// 	}
// }
