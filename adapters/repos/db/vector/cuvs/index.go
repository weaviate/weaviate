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

//go:build cuvs

package cuvs_index

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"io"
	"sync"

	"github.com/pkg/errors"
	cuvs "github.com/rapidsai/cuvs/go"
	"github.com/rapidsai/cuvs/go/cagra"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	cuvsEnt "github.com/weaviate/weaviate/entities/vectorindex/cuvs"
)

type VectorIndex interface {
	// Dump(labels ...string)
	Add(id uint64, vector []float32) error
	AddBatch(ctx context.Context, id []uint64, vector [][]float32) error
	Delete(id ...uint64) error
	SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error)
	// SearchByVectorDistance(vector []float32, dist float32,
	// 	maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error)
	// UpdateUserConfig(updated schemaconfig.VectorIndexConfig, callback func()) error
	// Drop(ctx context.Context) error
	// Shutdown(ctx context.Context) error
	// Flush() error
	// SwitchCommitLogs(ctx context.Context) error
	// ListFiles(ctx context.Context, basePath string) ([]string, error)
	PostStartup()
	// Compressed() bool
	// ValidateBeforeInsert(vector []float32) error
	// DistanceBetweenVectors(x, y []float32) (float32, error)
	// ContainsNode(id uint64) bool
	// DistancerProvider() distancer.Provider
	AlreadyIndexed() uint64
	// QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer
}

type cuvs_index struct {
	sync.Mutex
	id              string
	targetVector    string
	dims            int32
	store           *lsmkv.Store
	logger          logrus.FieldLogger
	distanceMetric  cuvs.Distance
	cuvsIndex       *cagra.CagraIndex
	cuvsIndexParams *cagra.IndexParams
	dlpackTensor    *cuvs.Tensor[float32]
	idCuvsIdMap     map[uint32]uint64
	cuvsResource    *cuvs.Resource
	cuvsExtendCount uint64
	cuvsNumExtends  uint64
	searchBatcher   *SearchBatcher

	// rescore             int64
	// bq                  compressionhelpers.BinaryQuantizer

	// pqResults *common.PqMaxPool
	// pool      *pools

	// compression string
	// bqCache     cache.Cache[uint64]
	count uint64
}

func New(cfg Config, uc cuvsEnt.UserConfig, store *lsmkv.Store) (*cuvs_index, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	logger := cfg.Logger
	if logger == nil {
		l := logrus.New()
		l.Out = io.Discard
		logger = l
	}

	res, err := cuvs.NewResource(nil)

	if err != nil {
		return nil, err
	}

	cuvsIndexParams, err := cagra.CreateIndexParams()

	cuvsIndexParams.SetGraphDegree(32)
	cuvsIndexParams.SetIntermediateGraphDegree(32)
	cuvsIndexParams.SetBuildAlgo(cagra.NnDescent)

	if err != nil {
		return nil, err
	}

	cuvsIndex, err := cagra.CreateIndex()

	if err != nil {
		return nil, fmt.Errorf("create cuvs index: %w", err)
	}

	index := &cuvs_index{
		id:              cfg.ID,
		targetVector:    cfg.TargetVector,
		logger:          logger,
		distanceMetric:  cuvs.DistanceL2, //TODO: make configurable
		cuvsIndex:       cuvsIndex,
		cuvsIndexParams: cuvsIndexParams,
		cuvsResource:    &res,
		dlpackTensor:    nil,
		idCuvsIdMap:     make(map[uint32]uint64),
	}

	// index.searchBatcher = NewSearchBatcher(index, 512, 40000*time.Microsecond)
	index.searchBatcher = nil

	// if err := index.initBuckets(context.Background()); err != nil {
	// 	return nil, fmt.Errorf("init cuvs index buckets: %w", err)
	// }

	return index, nil

}

func byteSliceFromFloat32Slice(vector []float32, slice []byte) []byte {
	for i := range vector {
		binary.LittleEndian.PutUint32(slice[i*4:], math.Float32bits(vector[i]))
	}
	return slice
}

func shouldExtend(index *cuvs_index, num_new uint64) bool {

	// return false

	// if index.cuvsNumExtends > 1 {
	// 	println("cuvs num extends is greater than 8; rebuilding")
	// 	return false
	// }

	// if index.count > 40 {
	// 	println("cuvs count is greater than 900; extending")
	// 	return true
	// }

	if num_new > 100 {
		println("num_new is greater than 100; rebuilding")
		return false
	}

	if index.count < 300_000 {
		println("index count is less than 300_000; rebuilding")
		return false
	}

	if index.dlpackTensor == nil {
		println("dlpack tensor is nil; rebuilding")
		return false
	}

	percentNewVectors := (float32(num_new+index.cuvsExtendCount) / float32(index.count+num_new)) * 100
	println("percentNewVectors: ", percentNewVectors)
	// why 20? https://weaviate-org.slack.com/archives/C05V3MGDGQY/p1722897390825229?thread_ts=1722894509.398509&cid=C05V3MGDGQY
	if percentNewVectors > 20 {
		println("percentNewVectors is greater than 20; rebuilding")
		return false
	}

	println("extending")

	return true

}

func Normalize_Temp(vector []float32) []float32 {
	for i := range vector {
		vector[i] = vector[i] * 0.146

	}
	return vector
}

func (index *cuvs_index) Add(id uint64, vector []float32) error {
	index.logger.Debug("adding single")
	return index.AddBatch(context.Background(), []uint64{id}, [][]float32{vector})
}

func (index *cuvs_index) AddBatch(ctx context.Context, id []uint64, vector [][]float32) error {
	index.Lock()
	defer index.Unlock()

	// println("cuvs index post startup")

	// err := cuvs.ResetMemoryResource()

	// if err != nil {
	// 	panic(err)
	// }

	if index == nil {
		return errors.New("cuvs index is nil")
	}

	for v := range vector {
		vector[v] = Normalize_Temp(vector[v])
	}

	if shouldExtend(index, uint64(len(id))) {
		index.cuvsExtendCount += uint64(len(id))
		index.cuvsNumExtends += 1
		return AddWithExtend(index, id, vector)
	} else {
		index.cuvsExtendCount = 0
		return AddWithRebuild(index, id, vector)
	}

}

func AddWithRebuild(index *cuvs_index, id []uint64, vector [][]float32) error {
	// index.Lock()
	// defer index.Unlock()

	index.logger.Debug("adding batch, batch size: ", len(id))
	index.logger.Debug("adding batch, batch dimension: ", len(vector[0]))
	// if len(vector[0]) != 128 {
	// 	panic("length not 128")
	// }

	// if err := ctx.Err(); err != nil {
	// 	return err
	// }

	if index.cuvsIndex == nil {
		return errors.New("cuvs index is nil")
	}

	// store in bucket
	// for i := range id {
	// 	slice := make([]byte, len(vector)*4)
	// 	idBytes := make([]byte, 8)
	// 	binary.BigEndian.PutUint64(idBytes, id[i])
	// 	index.store.Bucket(index.getBucketName()).Put(idBytes, byteSliceFromFloat32Slice(vector[i], slice))
	// }

	for i := range id {
		index.idCuvsIdMap[uint32(index.count)] = id[i]
		index.count += 1
	}

	// index.dlpackTensor.Expand(index.cuvsResource, vector)
	if index.dlpackTensor == nil {
		tensor, err := cuvs.NewTensor(false, vector, true)
		if err != nil {
			return err
		}
		_, err = tensor.ToDevice(index.cuvsResource)
		if err != nil {
			return err
		}
		index.dlpackTensor = &tensor
	} else {
		println("getShape:" + strconv.FormatInt(index.dlpackTensor.GetShape()[1], 10))
		_, err := index.dlpackTensor.Expand(index.cuvsResource, vector)
		if err != nil {
			return err
		}
	}

	err := cagra.BuildIndex(*index.cuvsResource, index.cuvsIndexParams, index.dlpackTensor, index.cuvsIndex)

	if err != nil {
		return err
	}

	// err = index.dlpackTensor.Close()
	// if err != nil {
	// 	return err
	// }

	return nil

}

func AddWithExtend(index *cuvs_index, id []uint64, vector [][]float32) error {
	// index.Lock()
	// defer index.Unlock()
	// id = id[:64]
	// vector = vector[:64]

	tensor, err := cuvs.NewTensor(true, vector, false)
	if err != nil {
		return err
	}
	_, err = tensor.ToDevice(index.cuvsResource)
	if err != nil {
		return err
	}

	ExtendParams, err := cagra.CreateExtendParams()
	if err != nil {
		return err
	}
	println("return SIZE")
	println(index.count)
	println(index.count + uint64(len(id)))
	ReturnDataset := make([][]float32, index.count+uint64(len(id)))
	for i := range ReturnDataset {
		ReturnDataset[i] = make([]float32, len(vector[0]))
	}

	returnTensor, err := cuvs.NewTensor(true, ReturnDataset, false)
	if err != nil {
		return err
	}
	_, err = returnTensor.ToDevice(index.cuvsResource)
	if err != nil {
		return err
	}

	println("before extending")
	for i := range id {
		index.idCuvsIdMap[uint32(index.count)] = id[i]
		index.count += 1
	}

	err = cagra.ExtendIndex(*index.cuvsResource, ExtendParams, &tensor, &returnTensor, index.cuvsIndex)

	println("done extending")
	println("done extending")
	// return nil
	if err != nil {
		return err
	}

	// err = index.dlpackTensor.Close()

	if err != nil {
		return err
	}

	index.dlpackTensor = &returnTensor

	return nil

}

func (index *cuvs_index) Delete(ids ...uint64) error {
	return nil
}

func (index *cuvs_index) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	if index.searchBatcher != nil {
		return index.searchBatcher.SearchByVector(context.Background(), vector, k, allow)

	}
	// index.Lock()
	// defer index.Unlock()
	// start := time.Now()

	// index.cuvsResource.Close()
	// res, err := cuvs.NewResource(nil)

	// index.cuvsResource = &res

	vector = Normalize_Temp(vector)
	// normalizeDuration := time.Since(start)
	// fmt.Printf("Normalize_Temp duration: %v\n", normalizeDuration)

	// tensorStart := time.Now()
	// tensor, err := cuvs.NewTensor(false, [][]float32{vector}, false)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// tensorDuration := time.Since(tensorStart)
	// fmt.Printf("NewTensor duration: %v\n", tensorDuration)

	// toDeviceStart := time.Now()
	// _, err = tensor.ToDevice(index.cuvsResource)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// toDeviceDuration := time.Since(toDeviceStart)
	// fmt.Printf("ToDevice duration: %v\n", toDeviceDuration)

	// queriesStart := time.Now()
	queries, err := cuvs.NewTensor(true, [][]float32{vector}, false)
	if err != nil {
		return nil, nil, err
	}
	// queriesDuration := time.Since(queriesStart)
	// fmt.Printf("NewTensor for queries duration: %v\n", queriesDuration)

	// neighborsStart := time.Now()
	// index.cuvsResource.Sync()
	// queries, err := cuvs.NewTensorOnDevice[float32](index.cuvsResource, []int64{int64(1), int64(len(vector))}, false)
	// if err != nil {
	// 	return nil, nil, err
	// }
	neighbors, err := cuvs.NewTensorOnDevice[uint32](index.cuvsResource, []int64{int64(1), int64(k)}, false)
	if err != nil {
		return nil, nil, err
	}
	distances, err := cuvs.NewTensorOnDevice[float32](index.cuvsResource, []int64{int64(1), int64(k)}, false)
	if err != nil {
		return nil, nil, err
	}
	// NeighborsDataset := make([][]uint32, 1)
	// for i := range NeighborsDataset {
	// 	NeighborsDataset[i] = make([]uint32, k)
	// }
	// DistancesDataset := make([][]float32, 1)
	// for i := range DistancesDataset {
	// 	DistancesDataset[i] = make([]float32, k)
	// }

	// neighbors, err := cuvs.NewTensor(true, NeighborsDataset, false)

	// neighborsDuration := time.Since(neighborsStart)
	// fmt.Printf("NewTensorOnDevice for neighbors duration: %v\n", neighborsDuration)

	// distancesStart := time.Now()
	// distances, err := cuvs.NewTensor(true, DistancesDataset, false)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// distancesDuration := time.Since(distancesStart)
	// fmt.Printf("NewTensorOnDevice for distances duration: %v\n", distancesDuration)

	// queriesToDeviceStart := time.Now()
	_, err = queries.ToDevice(index.cuvsResource)
	if err != nil {
		return nil, nil, err
	}

	// _, err = neighbors.ToDevice(index.cuvsResource)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// _, err = distances.ToDevice(index.cuvsResource)
	// if err != nil {
	// 	return nil, nil, err
	// }

	// queriesToDeviceDuration := time.Since(queriesToDeviceStart)
	// fmt.Printf("Queries ToDevice duration: %v\n", queriesToDeviceDuration)
	// dummyTensor, err := cuvs.NewTensorOnDevice[float32](index.cuvsResource, []int64{int64(1), int64(1)}, true)

	// paramsStart := time.Now()
	params, err := cagra.CreateSearchParams()
	params.SetAlgo(cagra.SearchAlgoMultiCta)
	params.SetItopkSize(256)

	// params.SetHashmapMode(cagra.HashmapModeSmall)
	// params.SetMaxQueries(1)
	// paramsDuration := time.Since(paramsStart)
	// fmt.Printf("CreateSearchParams duration: %v\n", paramsDuration)
	// index.cuvsResource.Sync()

	// dummyTensor.ToHost(index.cuvsResource)
	// searchStart := time.Now()
	// index.cuvsResource.Sync()
	cagra.SearchIndex(*index.cuvsResource, params, index.cuvsIndex, &queries, &neighbors, &distances)

	// index.cuvsResource.Sync()
	// searchDuration := time.Since(searchStart)
	// fmt.Printf("SearchIndex duration: %v\n", searchDuration)
	// wait 200 microseconds
	// time.Sleep(200 * time.Microsecond)
	// toHostStart := time.Now()
	_, err = neighbors.ToHost(index.cuvsResource)

	if err != nil {
		return nil, nil, err
	}

	_, err = distances.ToHost(index.cuvsResource)

	if err != nil {
		return nil, nil, err
	}

	// index.cuvsResource.Sync()

	// toHostDuration := time.Since(toHostStart)
	// fmt.Printf("ToHost duration: %v\n", toHostDuration)
	// getArrayStart := time.Now()
	neighborsSlice, err := neighbors.GetArray()
	if err != nil {
		return nil, nil, err
	}
	distancesSlice, err := distances.GetArray()
	if err != nil {
		return nil, nil, err
	}
	// getArrayDuration := time.Since(getArrayStart)
	// fmt.Printf("GetArray duration: %v\n", getArrayDuration)

	// resultProcessingStart := time.Now()
	neighborsResultSlice := make([]uint64, k)
	// index.Lock()
	for i := range neighborsSlice[0] {
		neighborsResultSlice[i] = index.idCuvsIdMap[neighborsSlice[0][i]]
	}

	// distances.Close()
	// neighbors.Close()
	// queries.Close()

	// index.Unlock()
	// resultProcessingDuration := time.Since(resultProcessingStart)
	// fmt.Printf("Result processing duration: %v\n", resultProcessingDuration)

	// totalDuration := time.Since(start)
	// fmt.Printf("Total SearchByVector duration: %v\n", totalDuration)

	// for i := range neighborsResultSlice {
	// 	fmt.Printf("neighborsResultSlice[i]: %v\n", neighborsResultSlice[i])
	// }

	return neighborsResultSlice, distancesSlice[0], nil
}

func (index *cuvs_index) SearchByVectorBatch(vector [][]float32, k int, allow helpers.AllowList) ([][]uint64, [][]float32, error) {
	index.Lock()
	defer index.Unlock()

	for v := range vector {
		vector[v] = Normalize_Temp(vector[v])
	}

	queries, err := cuvs.NewTensor(true, vector, false)

	if err != nil {
		return nil, nil, err
	}

	neighbors, err := cuvs.NewTensorOnDevice[uint32](index.cuvsResource, []int64{int64(len(vector)), int64(k)}, false)
	if err != nil {
		return nil, nil, err
	}
	distances, err := cuvs.NewTensorOnDevice[float32](index.cuvsResource, []int64{int64(len(vector)), int64(k)}, false)
	if err != nil {
		return nil, nil, err
	}

	_, err = queries.ToDevice(index.cuvsResource)

	if err != nil {
		return nil, nil, err
	}

	params, err := cagra.CreateSearchParams()

	// index.cuvsResource.Sync()
	cagra.SearchIndex(*index.cuvsResource, params, index.cuvsIndex, &queries, &neighbors, &distances)

	// index.cuvsResource.Sync()
	neighbors.ToHost(index.cuvsResource)
	distances.ToHost(index.cuvsResource)
	// index.cuvsResource.Sync()

	neighborsSlice, err := neighbors.GetArray()

	if err != nil {
		return nil, nil, err
	}

	distancesSlice, err := distances.GetArray()

	if err != nil {
		return nil, nil, err
	}

	neighborsResultSlice := make([][]uint64, len(vector))
	// index.Lock()
	for j := range neighborsSlice {
		neighborsResultSlice[j] = make([]uint64, k)
		for i := range neighborsSlice[j] {
			neighborsResultSlice[j][i] = index.idCuvsIdMap[neighborsSlice[j][i]]
		}
	}
	// index.Unlock()

	return neighborsResultSlice, distancesSlice, nil

}

func (index *cuvs_index) initBuckets(ctx context.Context) error {
	if err := index.store.CreateOrLoadBucket(ctx, index.getBucketName(),
		// lsmkv.WithForceCompation(forceCompaction),
		lsmkv.WithUseBloomFilter(false),
		lsmkv.WithCalcCountNetAdditions(false),
	); err != nil {
		return fmt.Errorf("Create or load flat vectors bucket: %w", err)
	}

	return nil
}

func (index *cuvs_index) getBucketName() string {
	if index.targetVector != "" {
		return fmt.Sprintf("%s_%s", helpers.VectorsBucketLSM, index.targetVector)
	}
	return helpers.VectorsBucketLSM
}
func float32SliceFromByteSlice(vector []byte, slice []float32) []float32 {
	for i := range slice {
		slice[i] = math.Float32frombits(binary.LittleEndian.Uint32(vector[i*4:]))
	}
	return slice
}

func (index *cuvs_index) AlreadyIndexed() uint64 {
	return index.count
}

func (index *cuvs_index) PostStartup() {

	println("cuvs index post startup")

	err := cuvs.ResetMemoryResource()

	if err != nil {
		panic(err)
	}

	err = cuvs.EnablePoolMemoryResource(80, 100, false)

	if err != nil {
		panic(err)
	}

	// cursor := index.store.Bucket(index.getBucketName()).Cursor()
	// defer cursor.Close()

	// // The initial size of 10k is chosen fairly arbitrarily. The cost of growing
	// // this slice dynamically should be quite cheap compared to other operations
	// // involved here, e.g. disk reads.
	// ids := make([]uint64, 0, 10_000)
	// vectors := make([][]float32, 0, 10_000)
	// maxID := uint64(0)

	// for key, v := cursor.First(); key != nil; key, v = cursor.Next() {
	// 	id := binary.BigEndian.Uint64(key)
	// 	// vecs = append(vecs, vec{
	// 	// 	id:  id,
	// 	// 	vec: uint64SliceFromByteSlice(v, make([]uint64, len(v)/8)),
	// 	// })
	// 	ids = append(ids, id)
	// 	vectors = append(vectors, float32SliceFromByteSlice(v, make([]float32, len(v)/4)))
	// 	if id > maxID {
	// 		maxID = id
	// 	}
	// }

	// index.AddBatch(context.Background(), ids, vectors)
}

func (index *cuvs_index) Dump(labels ...string) {

}

// searchbyvectordistance
func (index *cuvs_index) SearchByVectorDistance(vector []float32, dist float32, maxLimit int64, allowList helpers.AllowList) ([]uint64, []float32, error) {
	return []uint64{}, []float32{}, nil
}

func (index *cuvs_index) maxLimit() int64 {
	return 1_000_000_000
}

func (index *cuvs_index) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	return nil
}

func (index *cuvs_index) Drop(ctx context.Context) error {
	return nil
}

func (index *cuvs_index) Shutdown(ctx context.Context) error {
	return nil
}

func (index *cuvs_index) Flush() error {
	return nil
}

func (index *cuvs_index) SwitchCommitLogs(ctx context.Context) error {
	return nil
}

func (index *cuvs_index) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return []string{}, nil
}

func (index *cuvs_index) Compressed() bool {
	return false
}

func (index *cuvs_index) ValidateBeforeInsert(vector []float32) error {
	return nil
}

func (index *cuvs_index) DistanceBetweenVectors(x, y []float32) (float32, error) {
	return 0, nil
}

func (index *cuvs_index) ContainsNode(id uint64) bool {
	return false
}

func (index *cuvs_index) DistancerProvider() distancer.Provider {
	return distancer.NewL2SquaredProvider()
}

func (index *cuvs_index) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	distFunc := func(nodeID uint64) (float32, error) {

		return 0, nil
	}
	return common.QueryVectorDistancer{DistanceFunc: distFunc}
}
