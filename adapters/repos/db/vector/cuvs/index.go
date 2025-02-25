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
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	// "github.com/boltdb/bolt"
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
	bolt "go.etcd.io/bbolt"
)

type cuvs_internals struct {
	cuvsIndex        *cagra.CagraIndex
	cuvsIndexParams  *cagra.IndexParams
	cuvsSearchParams *cagra.SearchParams
	dlpackTensor     *cuvs.Tensor[float32]
	cuvsResource     *cuvs.Resource
	cuvsMemory       *cuvs.CuvsPoolMemory
}

type cuvs_index struct {
	sync.Mutex
	cuvs_internals
	id                  string
	targetVector        string
	dims                int32
	store               *lsmkv.Store
	logger              logrus.FieldLogger
	distanceMetric      cuvs.Distance
	trackDimensionsOnce sync.Once

	metadata *bolt.DB
	rootPath string

	idCuvsIdMap    BiMap
	cuvsPoolMemory int

	cuvsExtendCount uint64
	cuvsNumExtends  uint64
	cuvsNumFiltered uint64
	cuvsFilter      []uint32
	maxFiltered     uint64
	nextCuvsId      uint32
	count           uint64
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

	if cfg.CuvsIndexParams == nil {
		cuvsIndexParams, err := cagra.CreateIndexParams()

		cuvsIndexParams.SetGraphDegree(32)
		cuvsIndexParams.SetIntermediateGraphDegree(32)
		cuvsIndexParams.SetBuildAlgo(cagra.NnDescent)

		if err != nil {
			return nil, err
		}

		cfg.CuvsIndexParams = cuvsIndexParams

	}

	if cfg.CuvsSearchParams == nil {

		cuvsSearchParams, err := cagra.CreateSearchParams()
		if err != nil {
			return nil, err
		}

		cuvsSearchParams.SetAlgo(cagra.SearchAlgoMultiCta)
		cuvsSearchParams.SetItopkSize(256)
		cuvsSearchParams.SetSearchWidth(1)

		cfg.CuvsSearchParams = cuvsSearchParams

	}

	if err != nil {
		return nil, err
	}

	cuvsIndex, err := cagra.CreateIndex()
	if err != nil {
		return nil, fmt.Errorf("create cuvs index: %w", err)
	}

	internals := cuvs_internals{
		cuvsIndex:        cuvsIndex,
		cuvsIndexParams:  cfg.CuvsIndexParams,
		cuvsSearchParams: cfg.CuvsSearchParams,
		cuvsResource:     &res,
		dlpackTensor:     nil,
	}

	index := &cuvs_index{
		cuvs_internals: internals,
		id:             cfg.ID,
		targetVector:   cfg.TargetVector,
		logger:         logger,
		distanceMetric: cuvs.DistanceL2,
		store:          store,
		rootPath:       cfg.RootPath,

		idCuvsIdMap:    *NewBiMap(),
		cuvsPoolMemory: cfg.CuvsPoolMemory,
	}

	index.maxFiltered = 30
	index.nextCuvsId = 0

	if err := index.initBuckets(context.Background()); err != nil {
		return nil, fmt.Errorf("init cuvs index buckets: %w", err)
	}

	if err := index.initMetadata(); err != nil {
		return nil, err
	}

	return index, nil
}

func byteSliceFromFloat32Slice(vector []float32, slice []byte) []byte {
	for i := range vector {
		binary.LittleEndian.PutUint32(slice[i*4:], math.Float32bits(vector[i]))
	}
	return slice
}

func ValidateUserConfigUpdate(initial, updated schemaConfig.VectorIndexConfig) error {
	_, ok := initial.(cuvsEnt.UserConfig)
	if !ok {
		return errors.Errorf("initial is not UserConfig, but %T", initial)
	}

	_, ok = updated.(cuvsEnt.UserConfig)
	if !ok {
		return errors.Errorf("updated is not UserConfig, but %T", updated)
	}

	return nil
}

func (index *cuvs_index) AddMulti(ctx context.Context, docId uint64, vector [][]float32) error {
	return errors.Errorf("AddMulti is not supported for cuvs index")
}

func (index *cuvs_index) AddMultiBatch(ctx context.Context, docIds []uint64, vectors [][][]float32) error {
	return errors.Errorf("AddMultiBatch is not supported for cuvs index")
}

func (index *cuvs_index) DeleteMulti(id ...uint64) error {
	return errors.Errorf("DeleteMulti is not supported for cuvs index")
}

func (index *cuvs_index) SearchByMultiVector(ctx context.Context, vector [][]float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, errors.Errorf("SearchByMultiVector is not supported for cuvs index")
}

func (index *cuvs_index) ValidateMultiBeforeInsert(vector [][]float32) error {
	return errors.Errorf("ValidateMultiBeforeInsert is not supported for cuvs index")
}

func (index *cuvs_index) GetKeys(id uint64) (uint64, uint64, error) {
	return 0, 0, errors.Errorf("GetKeys is not supported for cuvs index")
}

func (index *cuvs_index) Multivector() bool {
	return false
}

func shouldExtend(index *cuvs_index, num_new uint64) bool {
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

	// why 20? https://weaviate-org.slack.com/archives/C05V3MGDGQY/p1722897390825229?thread_ts=1722894509.398509&cid=C05V3MGDGQY
	if percentNewVectors > 20 {
		println("percentNewVectors is greater than 20; rebuilding")
		return false
	}

	println("extending")

	return true
}

func (index *cuvs_index) Add(ctx context.Context, id uint64, vector []float32) error {
	return index.AddBatch(ctx, []uint64{id}, [][]float32{vector})
}

func (index *cuvs_index) AddBatch(ctx context.Context, id []uint64, vectors [][]float32) error {
	index.Lock()
	defer index.Unlock()

	if index.count+uint64(len(id)) < 32 {
		return errors.Errorf("minimum number of vectors in the dataset must be 32")
	}

	if len(id) != len(vectors) {
		return errors.Errorf("ids and vectors sizes does not match")
	}
	if len(id) == 0 {
		return errors.Errorf("insertBatch called with empty lists")
	}

	index.trackDimensionsOnce.Do(func() {
		size := int32(len(vectors[0]))
		atomic.StoreInt32(&index.dims, size)
		err := index.setDimensions(size)
		if err != nil {
			index.logger.WithError(err).Error("could not set dimensions")
		}
	})
	for i := range vectors {
		if len(vectors[i]) != int(index.dims) {
			return errors.Errorf("insert called with a vector of the wrong size")
		}
	}

	if index == nil {
		return errors.New("cuvs index is nil")
	}

	// store in bucket
	for i := range id {
		slice := make([]byte, len(vectors[i])*4)
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, id[i])
		index.store.Bucket(index.getBucketName()).Put(idBytes, byteSliceFromFloat32Slice(vectors[i], slice))
	}

	if shouldExtend(index, uint64(len(id))) {
		index.cuvsExtendCount += uint64(len(id))
		index.cuvsNumExtends += 1
		return AddWithExtend(index, id, vectors)
	} else {
		index.cuvsExtendCount = 0
		return AddWithRebuild(index, id, vectors)
	}
}

func (index *cuvs_index) storeVector(id uint64, vector []byte) {
	index.storeGenericVector(id, vector, index.getBucketName())
}

func (index *cuvs_index) storeGenericVector(id uint64, vector []byte, bucket string) {
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	index.store.Bucket(bucket).Put(idBytes, vector)
}

func AddWithRebuild(index *cuvs_index, id []uint64, vector [][]float32) error {
	index.logger.Debug("adding batch, batch size: ", len(id))
	index.logger.Debug("adding batch, batch dimension: ", len(vector[0]))

	if index.cuvsIndex == nil {
		return errors.New("cuvs index is nil")
	}

	for i := range id {
		// index.idCuvsIdMap[uint32(index.count)] = id[i]
		index.idCuvsIdMap.Insert(index.nextCuvsId, id[i])
		index.nextCuvsId += 1
		index.count += 1
	}

	if index.dlpackTensor == nil {
		tensor, err := cuvs.NewTensor(vector)
		if err != nil {
			return err
		}
		_, err = tensor.ToDevice(index.cuvsResource)
		if err != nil {
			return err
		}
		index.dlpackTensor = &tensor
	} else {
		_, err := index.dlpackTensor.Expand(index.cuvsResource, vector)
		if err != nil {
			return err
		}
	}

	err := cagra.BuildIndex(*index.cuvsResource, index.cuvsIndexParams, index.dlpackTensor, index.cuvsIndex)
	if err != nil {
		return err
	}
	println("done")

	return nil
}

func AddWithExtend(index *cuvs_index, id []uint64, vector [][]float32) error {
	tensor, err := cuvs.NewTensor(vector)
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
	println(index.count)
	println(index.count + uint64(len(id)))
	ReturnDataset := make([][]float32, index.count+uint64(len(id)))
	for i := range ReturnDataset {
		ReturnDataset[i] = make([]float32, len(vector[0]))
	}

	returnTensor, err := cuvs.NewTensor(ReturnDataset)
	if err != nil {
		return err
	}
	_, err = returnTensor.ToDevice(index.cuvsResource)
	if err != nil {
		return err
	}

	for i := range id {
		index.idCuvsIdMap.Insert(index.nextCuvsId, id[i])
		index.count += 1
		index.nextCuvsId += 1
	}

	err = cagra.ExtendIndex(*index.cuvsResource, ExtendParams, &tensor, &returnTensor, index.cuvsIndex)
	// return nil
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	index.dlpackTensor = &returnTensor

	return nil
}

func (index *cuvs_index) ContainsDoc(docID uint64) bool {
	_, exists := index.idCuvsIdMap.GetCuvsId(docID)

	return exists
}

func (index *cuvs_index) QueryMultiVectorDistancer(queryVector [][]float32) common.QueryVectorDistancer {
	return common.QueryVectorDistancer{}
}

func (index *cuvs_index) SearchByMultiVectorDistance(ctx context.Context, vector [][]float32,
	targetDistance float32, maxLimit int64, allow helpers.AllowList,
) ([]uint64, []float32, error) {
	return nil, nil, errors.Errorf("SearchByMultiVectorDistance is not supported for cuvs index")
}

func (index *cuvs_index) Delete(ids ...uint64) error {
	index.Lock()
	defer index.Unlock()
	if index.count-uint64(len(ids)) < 32 {
		return errors.Errorf("minimum number of vectors in the dataset must be 32")
	}
	for i := range ids {
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, ids[i])

		if err := index.store.Bucket(index.getBucketName()).Delete(idBytes); err != nil {
			return err
		}
	}
	if index.shouldUseFilter(uint64(len(ids))) {
		return index.deleteWithFilter(ids)
	}

	return index.deleteWithRebuild(ids)
}

func (index *cuvs_index) shouldUseFilter(additionalElements uint64) bool {
	if (index.cuvsNumFiltered + additionalElements) > index.maxFiltered {
		return false
	}
	return true
}

func (index *cuvs_index) deleteWithFilter(ids []uint64) error {
	for i := range ids {
		cuvsId, exists := index.idCuvsIdMap.GetCuvsId(ids[i])
		if !exists {
			return errors.Errorf("idCuvsIdMap does not contain id %d", ids[i])
		}
		if !slices.Contains(index.cuvsFilter, cuvsId) {
			index.cuvsFilter = append(index.cuvsFilter, cuvsId)
			index.cuvsNumFiltered++
		}
		index.count--
	}
	return nil
}

func (index *cuvs_index) deleteWithRebuild(ids []uint64) error {
	index.logger.Debug("deleting with rebuild")

	if index.cuvsIndex == nil {
		return errors.New("cuvs index is nil")
	}

	if index.dlpackTensor == nil {
		return errors.New("dlpack tensor is nil")
	}

	_, err := index.dlpackTensor.ToHost(index.cuvsResource)
	slice, err := index.dlpackTensor.Slice()
	if err != nil {
		return err
	}

	newSlice := [][]float32{}

	newIndex := uint32(0)

	ids_cuvsIds := make([]uint32, len(ids))

	for i := range ids {
		var exists bool
		ids_cuvsIds[i], exists = index.idCuvsIdMap.GetCuvsId(ids[i])
		if !exists {
			return errors.Errorf("idCuvsIdMap does not contain id %d", ids[i])
		}
	}

	for i := range slice {
		if !slices.Contains(index.cuvsFilter, uint32(i)) && !slices.Contains(ids_cuvsIds, uint32(i)) {
			newSlice = append(newSlice, slice[i])

			// remap
			WeaviateId, _ := index.idCuvsIdMap.GetWeaviateId(uint32(i))

			index.idCuvsIdMap.DeleteByCuvsId(uint32(i))

			index.idCuvsIdMap.Insert(newIndex, WeaviateId)
			newIndex++
		} else {
			index.idCuvsIdMap.DeleteByCuvsId(uint32(i))
		}
	}

	index.cuvsNumFiltered = 0
	index.cuvsFilter = []uint32{}

	index.dlpackTensor.Close()

	index.count = uint64(len(newSlice))
	index.nextCuvsId = uint32(len(newSlice))

	newTensor, err := cuvs.NewTensor(newSlice)
	if err != nil {
		return err
	}

	_, err = newTensor.ToDevice(index.cuvsResource)
	if err != nil {
		return err
	}

	index.dlpackTensor = &newTensor

	err = cagra.BuildIndex(*index.cuvsResource, index.cuvsIndexParams, index.dlpackTensor, index.cuvsIndex)
	if err != nil {
		return err
	}
	println("done delete with rebuild")

	return nil
}

func (index *cuvs_index) createAllowList() []uint32 {
	list := []uint32{}

	for i := 0; i < int(index.nextCuvsId); i++ {
		if !slices.Contains(index.cuvsFilter, uint32(i)) {
			list = append(list, uint32(i))
		}
	}

	return list
}

func (index *cuvs_index) SearchByVector(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	index.Lock()
	defer index.Unlock()
	queries, err := cuvs.NewTensor([][]float32{vector})
	if err != nil {
		return nil, nil, err
	}
	neighbors, err := cuvs.NewTensorOnDevice[uint32](index.cuvsResource, []int64{int64(1), int64(k)})
	if err != nil {
		return nil, nil, err
	}
	distances, err := cuvs.NewTensorOnDevice[float32](index.cuvsResource, []int64{int64(1), int64(k)})
	if err != nil {
		return nil, nil, err
	}

	_, err = queries.ToDevice(index.cuvsResource)
	if err != nil {
		return nil, nil, err
	}

	cagra.SearchIndex(*index.cuvsResource, index.cuvsSearchParams, index.cuvsIndex, &queries, &neighbors, &distances, index.createAllowList())

	_, err = neighbors.ToHost(index.cuvsResource)
	if err != nil {
		return nil, nil, err
	}
	_, err = distances.ToHost(index.cuvsResource)
	if err != nil {
		return nil, nil, err
	}

	neighborsSlice, err := neighbors.Slice()
	if err != nil {
		return nil, nil, err
	}

	distancesSlice, err := distances.Slice()
	if err != nil {
		return nil, nil, err
	}

	neighborsResultSlice := make([]uint64, k)

	for i := range neighborsSlice[0] {
		// neighborsResultSlice[i] = index.idCuvsIdMap[neighborsSlice[0][i]]
		exists := false
		// println("neighborsSlice[0][i]:", neighborsSlice[0][i])
		// t_id, _ := index.idCuvsIdMap.GetWeaviateId(neighborsSlice[0][i])
		// println("index.idCuvsIdMap.GetWeaviateId(neighborsSlice[0][i]):", t_id)
		neighborsResultSlice[i], exists = index.idCuvsIdMap.GetWeaviateId(neighborsSlice[0][i])
		if !exists {
			return nil, nil, errors.Errorf("idCuvsIdMap does not contain id %d", neighborsSlice[0][i])
		}
	}

	err = distances.Close()
	if err != nil {
		return nil, nil, err
	}

	err = neighbors.Close()
	if err != nil {
		return nil, nil, err
	}

	err = queries.Close()
	if err != nil {
		return nil, nil, err
	}

	return neighborsResultSlice, distancesSlice[0], nil
}

func (index *cuvs_index) SearchByVectorBatch(vector [][]float32, k int, allow helpers.AllowList) ([][]uint64, [][]float32, error) {
	index.Lock()
	defer index.Unlock()

	queries, err := cuvs.NewTensor(vector)
	if err != nil {
		return nil, nil, err
	}

	neighbors, err := cuvs.NewTensorOnDevice[uint32](index.cuvsResource, []int64{int64(len(vector)), int64(k)})
	if err != nil {
		return nil, nil, err
	}

	distances, err := cuvs.NewTensorOnDevice[float32](index.cuvsResource, []int64{int64(len(vector)), int64(k)})
	if err != nil {
		return nil, nil, err
	}

	_, err = queries.ToDevice(index.cuvsResource)
	if err != nil {
		return nil, nil, err
	}

	err = cagra.SearchIndex(*index.cuvsResource, index.cuvsSearchParams, index.cuvsIndex, &queries, &neighbors, &distances, nil)
	if err != nil {
		return nil, nil, err
	}

	_, err = neighbors.ToHost(index.cuvsResource)
	if err != nil {
		return nil, nil, err
	}
	_, err = distances.ToHost(index.cuvsResource)
	if err != nil {
		return nil, nil, err
	}

	neighborsSlice, err := neighbors.Slice()
	if err != nil {
		return nil, nil, err
	}

	distancesSlice, err := distances.Slice()
	if err != nil {
		return nil, nil, err
	}

	neighborsResultSlice := make([][]uint64, len(vector))

	for j := range neighborsSlice {
		neighborsResultSlice[j] = make([]uint64, k)
		for i := range neighborsSlice[j] {
			// neighborsResultSlice[j][i] = index.idCuvsIdMap[neighborsSlice[j][i]]
			neighborsResultSlice[j][i], _ = index.idCuvsIdMap.GetWeaviateId(neighborsSlice[j][i])
		}
	}

	err = distances.Close()
	if err != nil {
		return nil, nil, err
	}
	err = neighbors.Close()
	if err != nil {
		return nil, nil, err
	}
	err = queries.Close()
	if err != nil {
		return nil, nil, err
	}

	return neighborsResultSlice, distancesSlice, nil
}

func (index *cuvs_index) initBuckets(ctx context.Context) error {
	if err := index.store.CreateOrLoadBucket(ctx, index.getBucketName(),
		lsmkv.WithUseBloomFilter(false),
		lsmkv.WithCalcCountNetAdditions(false),
		lsmkv.WithPread(false), // Add this critical flag
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
	if index.cuvsPoolMemory != 0 {
		mem, _ := cuvs.NewCuvsPoolMemory(index.cuvsPoolMemory, 100, false)

		index.cuvsMemory = mem

	}

	cursor := index.store.Bucket(index.getBucketName()).Cursor()
	defer cursor.Close()

	// The initial size of 10k is chosen fairly arbitrarily. The cost of growing
	// this slice dynamically should be quite cheap compared to other operations
	// involved here, e.g. disk reads.
	ids := make([]uint64, 0, 10_000)
	vectors := make([][]float32, 0, 10_000)
	maxID := uint64(0)

	for key, v := cursor.First(); key != nil; key, v = cursor.Next() {
		id := binary.BigEndian.Uint64(key)
		ids = append(ids, id)
		vectors = append(vectors, float32SliceFromByteSlice(v, make([]float32, len(v)/4)))
		if id > maxID {
			maxID = id
		}
	}

	if vectors == nil || len(vectors) == 0 {
		return
	}

	index.AddBatch(context.Background(), ids, vectors)
}

func (index *cuvs_index) Dump(labels ...string) {
	if len(labels) > 0 {
		fmt.Printf("--------------------------------------------------\n")
		fmt.Printf("--  %s\n", strings.Join(labels, ", "))
	}
	fmt.Printf("--------------------------------------------------\n")
	fmt.Printf("ID: %s\n", index.id)
	fmt.Printf("--------------------------------------------------\n")
}

// searchbyvectordistance
func (index *cuvs_index) SearchByVectorDistance(ctx context.Context, vector []float32, dist float32, maxLimit int64, allowList helpers.AllowList) ([]uint64, []float32, error) {
	return []uint64{}, []float32{}, nil
}

func (index *cuvs_index) maxLimit() int64 {
	return 1_000_000_000
}

func (index *cuvs_index) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	return nil
}

func (index *cuvs_index) Drop(ctx context.Context) error {
	if index.metadata != nil {
		if err := index.metadata.Close(); err != nil {
			return errors.Wrap(err, "close metadata")
		}
		if err := index.removeMetadataFile(); err != nil {
			return err
		}
	}
	// Shard::drop will take care of handling store's buckets
	return nil
}

func CheckGpuMemory() error {
	// run nvidia-smi
	cmd := exec.Command("nvidia-smi")
	out, err := cmd.Output()
	println("nvidia-smi output (CheckGpuMemory()): ", string(out))
	if err != nil {
		return err
	}
	return nil
}

func (index *cuvs_index) Stats() (common.IndexStats, error) {
	return &CuvsStats{}, errors.New("Stats() is not implemented for cuvs index")
}

func (s *CuvsStats) IndexType() common.IndexType {
	return common.IndexTypeCUVS
}

type CuvsStats struct{}

func (index *cuvs_index) Shutdown(ctx context.Context) error {
	err := index.cuvs_internals.cuvsIndex.Close()
	if err != nil {
		return err
	}

	err = index.cuvs_internals.cuvsIndexParams.Close()
	if err != nil {
		return err
	}

	err = index.cuvs_internals.cuvsSearchParams.Close()
	if err != nil {
		return err
	}

	err = index.cuvs_internals.cuvsResource.Close()
	if err != nil {
		return err
	}

	err = index.cuvs_internals.dlpackTensor.Close()
	if err != nil {
		return err
	}

	if index.cuvsMemory != nil {
		err = index.cuvsMemory.Close()
		if err != nil {
			return err
		}
	}

	index.cuvs_internals.cuvsResource.Sync()

	if index.metadata != nil {
		if err := index.metadata.Close(); err != nil {
			return errors.Wrap(err, "close metadata")
		}
	}

	return nil
}

func (index *cuvs_index) Flush() error {
	return nil
}

func (index *cuvs_index) SwitchCommitLogs(ctx context.Context) error {
	return nil
}

func (index *cuvs_index) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	var files []string

	if index.metadata != nil {
		metadataFile := index.getMetadataFile()
		fullPath := filepath.Join(index.rootPath, metadataFile)

		if _, err := os.Stat(fullPath); err == nil {
			relPath, err := filepath.Rel(basePath, fullPath)
			if err != nil {
				return nil, fmt.Errorf("failed to get relative path: %w", err)
			}
			files = append(files, relPath)
		}
		// If the file doesn't exist, we simply don't add it to the list
	}

	return files, nil
}

func (index *cuvs_index) Compressed() bool {
	return false
}

func (index *cuvs_index) ValidateBeforeInsert(vector []float32) error {
	return nil
}

func (index *cuvs_index) Iterate(fn func(id uint64) bool) {
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
