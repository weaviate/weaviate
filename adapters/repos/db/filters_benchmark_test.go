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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// vectorIndexSetup describes a vector index configuration to test against.
type vectorIndexSetup struct {
	name              string
	vectorIndexType   string
	vectorIndexConfig interface{}
}

func hnswDefault() vectorIndexSetup {
	return vectorIndexSetup{
		name:              "hnsw",
		vectorIndexType:   "hnsw",
		vectorIndexConfig: enthnsw.NewDefaultUserConfig(),
	}
}

func hnswAcorn() vectorIndexSetup {
	uc := enthnsw.NewDefaultUserConfig()
	uc.FilterStrategy = enthnsw.FilterStrategyAcorn
	return vectorIndexSetup{
		name:              "hnsw_acorn",
		vectorIndexType:   "hnsw",
		vectorIndexConfig: uc,
	}
}

func hnswWithPQ() vectorIndexSetup {
	uc := enthnsw.NewDefaultUserConfig()
	uc.PQ = enthnsw.PQConfig{
		Enabled:       true,
		Segments:      0,
		Centroids:     256,
		TrainingLimit: 10,
		Encoder: enthnsw.PQEncoder{
			Type:         enthnsw.DefaultPQEncoderType,
			Distribution: enthnsw.DefaultPQEncoderDistribution,
		},
	}
	return vectorIndexSetup{
		name:              "hnsw_pq",
		vectorIndexType:   "hnsw",
		vectorIndexConfig: uc,
	}
}

func hnswWithBQ() vectorIndexSetup {
	uc := enthnsw.NewDefaultUserConfig()
	uc.BQ = enthnsw.BQConfig{Enabled: true}
	return vectorIndexSetup{
		name:              "hnsw_bq",
		vectorIndexType:   "hnsw",
		vectorIndexConfig: uc,
	}
}

func hnswWithSQ() vectorIndexSetup {
	uc := enthnsw.NewDefaultUserConfig()
	uc.SQ = enthnsw.SQConfig{
		Enabled:       true,
		TrainingLimit: 10,
		RescoreLimit:  20,
	}
	return vectorIndexSetup{
		name:              "hnsw_sq",
		vectorIndexType:   "hnsw",
		vectorIndexConfig: uc,
	}
}

func flatDefault() vectorIndexSetup {
	return vectorIndexSetup{
		name:              "flat",
		vectorIndexType:   "flat",
		vectorIndexConfig: flatent.NewDefaultUserConfig(),
	}
}

func flatWithBQ() vectorIndexSetup {
	uc := flatent.NewDefaultUserConfig()
	uc.BQ = flatent.CompressionUserConfig{Enabled: true, RescoreLimit: 0, Cache: false}
	return vectorIndexSetup{
		name:              "flat_bq",
		vectorIndexType:   "flat",
		vectorIndexConfig: uc,
	}
}

// allVectorIndexSetups returns all vector index configurations to test.
func allVectorIndexSetups() []vectorIndexSetup {
	return []vectorIndexSetup{
		hnswDefault(),
		hnswAcorn(),
		hnswWithPQ(),
		hnswWithBQ(),
		hnswWithSQ(),
		flatDefault(),
		flatWithBQ(),
	}
}

// filterTestCase describes a filter combination to test.
type filterTestCase struct {
	name                    string
	filter                  *filters.LocalFilter
	expectedMatchPercentage int
}

func buildBenchFilter(className, propName string, value interface{}, operator filters.Operator, schemaType schema.DataType) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			On: &filters.Path{
				Class:    schema.ClassName(className),
				Property: schema.PropertyName(propName),
			},
			Value: &filters.Value{
				Value: value,
				Type:  schemaType,
			},
		},
	}
}

func benchCompoundFilter(operator filters.Operator, operands ...*filters.LocalFilter) (*filters.LocalFilter, string) {
	clauses := make([]filters.Clause, len(operands))

	name := ""
	for i, f := range operands {
		clauses[i] = *f.Root
		name += fmt.Sprintf("%s_%s_%v_%s_", f.Root.Operator.Name(), f.Root.On.Property, f.Root.Value.Value, strings.ToUpper(operator.Name()))
	}
	name = name[:len(name)-1-len(operator.Name())] // remove trailing operator name and underscore
	name += ""

	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			Operands: clauses,
		},
	}, name
}

func buildFilterTestCase(filter *filters.LocalFilter) filterTestCase {
	name := fmt.Sprintf("%s_%s_%v", filter.Root.On.Property, filter.Root.Operator.Name(), filter.Root.Value.Value)
	return filterTestCase{
		name:                    name,
		filter:                  filter,
		expectedMatchPercentage: computeExpectedMatchPercentage(filter),
	}
}

func buildCompundFilterTestCase(operator filters.Operator, operands ...*filters.LocalFilter) filterTestCase {
	filter, name := benchCompoundFilter(operator, operands...)
	return filterTestCase{
		name:                    name,
		filter:                  filter,
		expectedMatchPercentage: computeExpectedMatchPercentage(filter),
	}
}

func computeExpectedMatchPercentage(filter *filters.LocalFilter) int {
	// This function computes the expected number of matches for a given filter based on how the benchmark data is set up.
	// For single filters, we can directly compute the match percentage based on the value and operator.
	// For compound filters, we need to compute the expected matches for each operand and then combine them according to the operator (AND/OR).
	switch filter.Root.Operator {
	case filters.OperatorEqual:
		return 1
	case filters.OperatorNotEqual:
		return 99
	case filters.OperatorNot:
		return 100 - computeExpectedMatchPercentage(&filters.LocalFilter{Root: &filter.Root.Operands[0]})
	case filters.OperatorGreaterThan:
		value := filter.Root.Value.Value.(int)
		return 100 - value // all values greater than the given value
	case filters.OperatorGreaterThanEqual:
		value := filter.Root.Value.Value.(int)
		return 100 - (value - 1) // all values greater than or equal to the given value
	case filters.OperatorLessThan:
		value := filter.Root.Value.Value.(int)
		return (value - 1) // all values less than the given value
	case filters.OperatorLessThanEqual:
		value := filter.Root.Value.Value.(int)
		return value // all values less than or equal to the given value
	case filters.OperatorAnd:
		normalizedFound := make(map[int]bool)
		for i, operand := range filter.Root.Operands {
			if operand.Operator != filters.OperatorEqual && operand.Operator != filters.OperatorNotEqual &&
				operand.Operator != filters.OperatorGreaterThan && operand.Operator != filters.OperatorGreaterThanEqual &&
				operand.Operator != filters.OperatorLessThan && operand.Operator != filters.OperatorLessThanEqual {
				panic("Only one level of and/or supported")
			}
			value := operand.Value.Value.(int)
			op := operand.Operator
			if operand.On.Property == "intFieldRev" {
				value = 101 - value
				switch op {
				case filters.OperatorGreaterThan:
					op = filters.OperatorLessThan
				case filters.OperatorGreaterThanEqual:
					op = filters.OperatorLessThanEqual
				case filters.OperatorLessThan:
					op = filters.OperatorGreaterThan
				case filters.OperatorLessThanEqual:
					op = filters.OperatorGreaterThanEqual
				default:
				}
			}
			if i == 0 {
				switch op {
				case filters.OperatorEqual:
					normalizedFound[value] = true
				case filters.OperatorNotEqual:
					for i := 1; i <= 100; i++ {
						if i != value {
							normalizedFound[i] = true
						}
					}
				case filters.OperatorGreaterThan:
					for i := value + 1; i <= 100; i++ {
						normalizedFound[i] = true
					}
				case filters.OperatorGreaterThanEqual:
					for i := value; i <= 100; i++ {
						normalizedFound[i] = true
					}
				case filters.OperatorLessThan:
					for i := 1; i < value; i++ {
						normalizedFound[i] = true
					}
				case filters.OperatorLessThanEqual:
					for i := 1; i <= value; i++ {
						normalizedFound[i] = true
					}
				default:
				}
			} else {
				switch op {
				case filters.OperatorEqual:
					for k := range normalizedFound {
						if k != value {
							delete(normalizedFound, k)
						}
					}
				case filters.OperatorNotEqual:
					for k := range normalizedFound {
						if k == value {
							delete(normalizedFound, k)
						}
					}
				case filters.OperatorGreaterThan:
					for k := range normalizedFound {
						if k <= value {
							delete(normalizedFound, k)
						}
					}
				case filters.OperatorGreaterThanEqual:
					for k := range normalizedFound {
						if k < value {
							delete(normalizedFound, k)
						}
					}
				case filters.OperatorLessThan:
					for k := range normalizedFound {
						if k >= value {
							delete(normalizedFound, k)
						}
					}
				case filters.OperatorLessThanEqual:
					for k := range normalizedFound {
						if k > value {
							delete(normalizedFound, k)
						}
					}
				default:
				}
			}
		}
		return len(normalizedFound)
	case filters.OperatorOr:
		// For OR, we need to subtract the overlap between operands. For simplicity, we assume independence and use the formula:
		// Filters are not independent, we know that intField and intFieldRev are perfectly inverted,
		// so we can directly compute the expected matches for the OR of two filters on intField and intFieldRev.
		normalizedFound := make(map[int]bool)
		for _, operand := range filter.Root.Operands {
			if operand.Operator != filters.OperatorEqual && operand.Operator != filters.OperatorNotEqual &&
				operand.Operator != filters.OperatorGreaterThan && operand.Operator != filters.OperatorGreaterThanEqual &&
				operand.Operator != filters.OperatorLessThan && operand.Operator != filters.OperatorLessThanEqual {
				panic("Only one level of and/or supported")
			}
			value := operand.Value.Value.(int)
			op := operand.Operator
			if operand.On.Property == "intFieldRev" {
				value = 101 - value
				switch op {
				case filters.OperatorGreaterThan:
					op = filters.OperatorLessThan
				case filters.OperatorGreaterThanEqual:
					op = filters.OperatorLessThanEqual
				case filters.OperatorLessThan:
					op = filters.OperatorGreaterThan
				case filters.OperatorLessThanEqual:
					op = filters.OperatorGreaterThanEqual
				default:
				}
			}
			switch op {
			case filters.OperatorEqual:
				normalizedFound[value] = true
			case filters.OperatorNotEqual:
				for i := 1; i <= 100; i++ {
					if i != value {
						normalizedFound[i] = true
					}
				}
			case filters.OperatorGreaterThan:
				for i := value + 1; i <= 100; i++ {
					normalizedFound[i] = true
				}
			case filters.OperatorGreaterThanEqual:
				for i := value; i <= 100; i++ {
					normalizedFound[i] = true
				}
			case filters.OperatorLessThan:
				for i := 1; i < value; i++ {
					normalizedFound[i] = true
				}
			case filters.OperatorLessThanEqual:
				for i := 1; i <= value; i++ {
					normalizedFound[i] = true
				}
			default:
			}
		}
		return len(normalizedFound)
	default:
		panic(fmt.Sprintf("unsupported operator: %v", filter.Root.Operator))
	}
}

func allFilterTestCases(matchPcts []int) []filterTestCase {
	results := make([]filterTestCase, 0)

	// Single filters

	for _, matchPct := range matchPcts {
		for _, propName := range []string{"intField", "intFieldRev"} {
			results = append(results, buildFilterTestCase(buildBenchFilter("TestClass", propName, matchPct, filters.OperatorNotEqual, schema.DataTypeInt)))
		}
	}

	results = append(results, buildCompundFilterTestCase(filters.OperatorAnd, buildBenchFilter("TestClass", "intField", 10, filters.OperatorNotEqual, schema.DataTypeInt), buildBenchFilter("TestClass", "intFieldRev", 90, filters.OperatorNotEqual, schema.DataTypeInt)))
	results = append(results, buildCompundFilterTestCase(filters.OperatorOr, buildBenchFilter("TestClass", "intField", 10, filters.OperatorNotEqual, schema.DataTypeInt), buildBenchFilter("TestClass", "intFieldRev", 90, filters.OperatorNotEqual, schema.DataTypeInt)))

	return results
}

func setupBenchmarkDB(t testing.TB) (*DB, *fakeSchemaGetter) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, &FakeNodeResolver{}, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(testCtx()))
	t.Cleanup(func() {
		repo.Shutdown(context.Background())
	})

	return repo, schemaGetter
}

func benchmarkFilterClass(className string, vis vectorIndexSetup) *models.Class {
	vTrue := true
	return &models.Class{
		Class: className,
		VectorConfig: map[string]models.VectorConfig{
			"default": {
				VectorIndexType:   vis.vectorIndexType,
				VectorIndexConfig: vis.vectorIndexConfig,
			},
			"other": {
				VectorIndexType:   vis.vectorIndexType,
				VectorIndexConfig: vis.vectorIndexConfig,
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords: &models.StopwordConfig{
				Preset: "none",
			},
			Bm25: &models.BM25Config{
				K1: 1.2,
				B:  0.75,
			},
			IndexNullState:      true,
			IndexPropertyLength: true,
			UsingBlockMaxWAND:   config.DefaultUsingBlockMaxWAND,
		},
		Properties: []*models.Property{
			{
				Name:            "text",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vTrue,
				IndexSearchable: &vTrue,
			},
			{
				Name:              "intField",
				DataType:          []string{string(schema.DataTypeInt)},
				IndexRangeFilters: &vTrue,
			},
			{
				Name:              "intFieldRev",
				DataType:          []string{string(schema.DataTypeInt)},
				IndexRangeFilters: &vTrue,
			},
		},
	}
}

// waitForIndexing flushes all async vector index queues and waits until
// every enqueued vector has been fully processed by the scheduler.
func waitForIndexing(t testing.TB, repo *DB, className string) {
	t.Helper()
	idx := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, idx)

	err := idx.ForEachShard(func(_ string, shard ShardLike) error {
		return shard.ForEachVectorQueue(func(_ string, queue *VectorIndexQueue) error {
			if err := queue.Flush(); err != nil {
				return err
			}
			for queue.Size() > 0 {
				time.Sleep(100 * time.Millisecond)
			}
			return nil
		})
	})
	require.NoError(t, err)
}

func insertBenchmarkData(t testing.TB, repo *DB, className string, corpusCount int, vectorDim int) {
	t.Helper()
	ctx := context.Background()
	vector := make([]float32, vectorDim)
	for d := range vector {
		vector[d] = 0.5
	}

	batchSize := 1000
	batch := make(objects.BatchObjects, 0, batchSize)
	for i := 0; i < corpusCount; i++ {
		id := strfmt.UUID(uuid.NewString())
		obj := &models.Object{
			ID:    id,
			Class: className,
			Properties: map[string]interface{}{
				"text":        "benchmark",
				"intField":    (i % 100) + 1,   // values from 1 to 100, each repeated corpusCount/100 times
				"intFieldRev": 100 - (i % 100), // values from 100 to 1, each repeated corpusCount/100 times
			},
			Vectors: models.Vectors{
				"default": vector,
				"other":   vector,
			},
		}
		batch = append(batch, objects.BatchObject{
			OriginalIndex: len(batch),
			Object:        obj,
			UUID:          id,
		})

		if len(batch) >= batchSize {
			resp, err := repo.BatchPutObjects(ctx, batch, nil, 0)
			require.NoError(t, err)
			for _, r := range resp {
				require.NoError(t, r.Err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		resp, err := repo.BatchPutObjects(ctx, batch, nil, 0)
		require.NoError(t, err)
		for _, r := range resp {
			require.NoError(t, r.Err)
		}
	}
}

func TestFiltersBenchmark(t *testing.T) {
	corpusCounts := []int{100}
	matchPcts := []int{1, 10, 50, 90, 100}
	vectorDim := 4

	searchVector := make([]float32, vectorDim)
	for d := range searchVector {
		searchVector[d] = 0.5
	}

	vectorSetups := allVectorIndexSetups()

	filterCases := allFilterTestCases(matchPcts)

	for _, vis := range vectorSetups {
		t.Run(fmt.Sprintf("index_%s", vis.name), func(t *testing.T) {
			repo, schemaGetter := setupBenchmarkDB(t)
			logger, _ := test.NewNullLogger()

			for _, corpusCount := range corpusCounts {
				if corpusCount%100 != 0 {
					t.Fatalf("corpusCount must be a multiple of 100 to match the way filter values are set up")
				}
				t.Run(fmt.Sprintf("corpus_%d", corpusCount), func(t *testing.T) {
					className := fmt.Sprintf("Bench_%s_%d", vis.name, corpusCount)
					class := benchmarkFilterClass(className, vis)

					schemaGetter.schema = schema.Schema{
						Objects: &models.Schema{
							Classes: []*models.Class{class},
						},
					}

					migrator := NewMigrator(repo, logger, "node1")
					require.NoError(t, migrator.AddClass(context.Background(), class))

					insertBenchmarkData(t, repo, className, corpusCount, vectorDim)
					waitForIndexing(t, repo, className)

					idx := repo.GetIndex(schema.ClassName(className))
					require.NotNil(t, idx)

					props := make([]string, len(class.Properties))
					for i, prop := range class.Properties {
						props[i] = prop.Name
					}

					for _, fc := range filterCases {
						t.Run(fc.name, func(t *testing.T) {
							// Multi vector search with filter
							t.Run("multi_vector_search", func(t *testing.T) {
								params := dto.GetParams{
									ClassName:  className,
									Pagination: &filters.Pagination{Limit: corpusCount},
									Filters:    fc.filter,
									NearVector: &searchparams.NearVector{
										Distance:      100,
										WithDistance:  true,
										TargetVectors: []string{"default", "other"},
									},
									TargetVectorCombination: &dto.TargetCombination{
										Type: dto.Minimum,
									},
									AdditionalProperties: additional.Properties{Distance: true},
								}
								res, err := repo.VectorSearch(
									context.Background(),
									params,
									[]string{"default", "other"},
									[]models.Vector{searchVector, searchVector},
								)
								require.NoError(t, err)
								require.NotNil(t, res)
								require.Equal(t, corpusCount*fc.expectedMatchPercentage, len(res)*100)
							})
							// Object search (inverted index only)
							t.Run("object_search", func(t *testing.T) {
								params := dto.GetParams{
									ClassName:  className,
									Pagination: &filters.Pagination{Limit: corpusCount},
									Filters:    fc.filter,
								}
								res, err := repo.Search(context.Background(), params)
								require.NoError(t, err)
								require.NotNil(t, res)
								require.Equal(t, corpusCount*fc.expectedMatchPercentage, len(res)*100)
							})

							// Keyword (BM25) search with filter
							t.Run("keyword_bm25", func(t *testing.T) {
								addit := additional.Properties{}
								kwr := &searchparams.KeywordRanking{
									Type:       "bm25",
									Properties: []string{"text"},
									Query:      "benchmark",
								}
								res, _, err := idx.objectSearch(
									context.Background(),
									corpusCount,
									fc.filter,
									kwr,
									nil,
									nil,
									addit,
									nil,
									"",
									0,
									props,
								)
								require.NoError(t, err)
								require.NotNil(t, res)
								require.Equal(t, corpusCount*fc.expectedMatchPercentage, len(res)*100)
							})

							// Vector search with filter
							t.Run("vector_search", func(t *testing.T) {
								params := dto.GetParams{
									ClassName:  className,
									Pagination: &filters.Pagination{Limit: corpusCount},
									Filters:    fc.filter,
									NearVector: &searchparams.NearVector{
										Distance:      100,
										WithDistance:  true,
										TargetVectors: []string{"default"},
									},
									AdditionalProperties: additional.Properties{Distance: true},
								}
								res, err := repo.VectorSearch(
									context.Background(),
									params,
									[]string{"default"},
									[]models.Vector{searchVector},
								)
								require.NoError(t, err)
								require.NotNil(t, res)
								require.Equal(t, corpusCount*fc.expectedMatchPercentage, len(res)*100)
							})
						})
					}

					t.Cleanup(func() {
						migrator.DropClass(context.Background(), className, false)
					})
				})
			}
		})
	}
}

func BenchmarkFilters(b *testing.B) {
	// set env var to increase search limit
	corpusCounts := []int{100}
	matchPcts := []int{1, 50, 99}
	vectorDim := 4

	searchVector := make([]float32, vectorDim)
	for d := range searchVector {
		searchVector[d] = 0.5
	}

	vectorSetups := allVectorIndexSetups()
	filterCases := allFilterTestCases(matchPcts)

	for v, vis := range vectorSetups {
		b.Run(fmt.Sprintf("index_%s", vis.name), func(b *testing.B) {
			repo, schemaGetter := setupBenchmarkDB(b)
			logger, _ := test.NewNullLogger()

			for _, corpusCount := range corpusCounts {
				b.Run(fmt.Sprintf("corpus_%d", corpusCount), func(b *testing.B) {
					className := fmt.Sprintf("Bench_%s_%d", vis.name, corpusCount)
					class := benchmarkFilterClass(className, vis)

					schemaGetter.schema = schema.Schema{
						Objects: &models.Schema{
							Classes: []*models.Class{class},
						},
					}

					migrator := NewMigrator(repo, logger, "node1")
					require.NoError(b, migrator.AddClass(context.Background(), class))

					insertBenchmarkData(b, repo, className, corpusCount, vectorDim)
					waitForIndexing(b, repo, className)

					idx := repo.GetIndex(schema.ClassName(className))
					require.NotNil(b, idx)

					props := make([]string, len(class.Properties))
					for i, prop := range class.Properties {
						props[i] = prop.Name
					}

					for _, matchPct := range matchPcts {
						b.Run(fmt.Sprintf("match_%dpct", matchPct), func(b *testing.B) {
							for _, fc := range filterCases {
								b.Run(fc.name, func(b *testing.B) {
									// For the first vector index setup, we run all benchmarks. For the others, we only run the vector search benchmark to save time.
									if v == 0 {
										b.Run("object_search", func(b *testing.B) {
											params := dto.GetParams{
												ClassName:  className,
												Pagination: &filters.Pagination{Limit: corpusCount},
												Filters:    fc.filter,
											}
											b.ResetTimer()
											for i := 0; i < b.N; i++ {
												_, err := repo.Search(context.Background(), params)
												require.NoError(b, err)
											}
										})

										b.Run("keyword_bm25", func(b *testing.B) {
											addit := additional.Properties{}
											kwr := &searchparams.KeywordRanking{
												Type:       "bm25",
												Properties: []string{"text"},
												Query:      "benchmark",
											}
											b.ResetTimer()
											for i := 0; i < b.N; i++ {
												_, _, err := idx.objectSearch(
													context.Background(),
													corpusCount,
													fc.filter,
													kwr,
													nil,
													nil,
													addit,
													nil,
													"",
													0,
													props,
												)
												require.NoError(b, err)
											}
										})
									}
									b.Run("vector_search", func(b *testing.B) {
										params := dto.GetParams{
											ClassName:  className,
											Pagination: &filters.Pagination{Limit: corpusCount},
											Filters:    fc.filter,
											NearVector: &searchparams.NearVector{
												Distance:      100,
												WithDistance:  true,
												TargetVectors: []string{"default"},
											},
											AdditionalProperties: additional.Properties{Distance: true},
										}
										b.ResetTimer()
										for i := 0; i < b.N; i++ {
											_, err := repo.VectorSearch(
												context.Background(),
												params,
												[]string{"default"},
												[]models.Vector{searchVector},
											)
											require.NoError(b, err)
										}
									})
								})
							}
						})
					}
				})
			}
		})
	}
}
