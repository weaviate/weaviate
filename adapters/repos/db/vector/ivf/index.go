package ivf

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

const (
	TrucAt    = 64
	MaxLevels = 18  // 12 // or 12+1500
	rescore   = 300 // 150
	size      = 1_000_000
)

type heapPool struct {
	pool *sync.Pool
}

func (p *heapPool) Get() *[]uint64 {
	slice := p.pool.Get().(*[]uint64)
	*slice = (*slice)[:0]
	return slice
}

func (p *heapPool) Put(slice *[]uint64) {
	p.pool.Put(slice)
}

// RegionalIVF implementa el algoritmo IVF basado en regiones
type RegionalIVF struct {
	sync.RWMutex

	// Configuración del algoritmo
	dims int32 // dimensiones de los vectores

	// Proveedor de distancias
	distancerProvider distancer.Provider

	// Configuración y estado
	id       string
	rootPath string
	logger   logrus.FieldLogger // logrus.FieldLogger en el contexto real

	// Control de acceso concurrente
	trackDimensionsOnce sync.Once

	// Métricas y estadísticas
	totalVectors uint64

	vectors [][]float32 // [nivel][índice_vector]Vector
	codes   [][]uint64

	bencoder compressionhelpers.BinaryQuantizer
	pool     *heapPool

	rate float64

	initList []uint64
}

// Config contiene la configuración para RegionalIVF
type Config struct {
	ID               string
	RootPath         string
	DistanceProvider distancer.Provider
	VectorForIDThunk common.VectorForID[float32]
	Logger           logrus.FieldLogger
	AllocChecker     memwatch.AllocChecker
}

// Validate valida la configuración
func (c Config) Validate() error {
	return nil
}

// New crea una nueva instancia de RegionalIVF
func New(cfg Config) (*RegionalIVF, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	index := &RegionalIVF{
		distancerProvider: cfg.DistanceProvider,
		id:                cfg.ID,
		rootPath:          cfg.RootPath,
		logger:            cfg.Logger,
		bencoder:          compressionhelpers.NewBinaryQuantizer(cfg.DistanceProvider),
		codes:             make([][]uint64, MaxLevels),
		vectors:           make([][]float32, size),
		rate:              math.Pow(float64(rescore)/float64(size), 1/float64(MaxLevels)),
		pool: &heapPool{
			pool: &sync.Pool{
				New: func() interface{} {
					slice := make([]uint64, 0, 1_000_000)
					return &slice
				},
			},
		},
		initList: make([]uint64, 0, size),
	}

	for l := range index.codes {
		index.codes[l] = make([]uint64, size)
	}

	for i := 0; i < size; i++ {
		index.initList = append(index.initList, uint64(i))
	}

	return index, nil
}

// ValidateBeforeInsert valida un vector antes de insertarlo
func (r *RegionalIVF) ValidateBeforeInsert(vector []float32) error {
	dims := int(atomic.LoadInt32(&r.dims))

	if dims == 0 {
		return nil
	}

	if dims != len(vector) {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", dims, len(vector))
	}

	return nil
}

// Add añade un vector al índice
func (r *RegionalIVF) Add(ctx context.Context, id uint64, vector []float32) error {
	return r.AddBatch(ctx, []uint64{id}, [][]float32{vector})
}

// AddBatch añade múltiples vectores al índice
func (r *RegionalIVF) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	if len(ids) != len(vectors) {
		return fmt.Errorf("ids and vectors length mismatch")
	}

	if len(ids) == 0 {
		return nil
	}

	// Establecer dimensiones en la primera inserción
	r.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&r.dims, int32(len(vectors[0])))
	})

	for i, vector := range vectors {
		r.vectors[ids[i]] = vector
		for l := 0; l < MaxLevels; l++ {
			r.codes[l][ids[i]] = r.bencoder.Encode(vector[l*TrucAt : (l+1)*TrucAt])[0]
		}
	}

	// Actualizar contador total
	atomic.AddUint64(&r.totalVectors, uint64(len(ids)))

	return nil
}

/***************************************************************
 ** New
 ** x32 top10  qps=20.464682821781924 recall=0.5935999999999995
 ** x32 top100 qps=19.567820964836983 recall=0.9539999999999991
 ** x32 top300 qps=20.2118514303 recall=0.9895999999999995
 ***************************************************************
 ** BQ
 ** top10  qps=1250.1278787060326 recall=0.6479999999999997
 ** top100 qps=671.8411125652276 recall=0.9805999999999989
 ** top300 qps=660.1741113097229 recall=0.9965999999999997
 ***************************************************************/

// SearchByVector busca los k vectores más similares
func (r *RegionalIVF) SearchByVector(ctx context.Context, searchVector []float32,
	k int, allowList helpers.AllowList) ([]uint64, []float32, error) {

	searchCode := r.bencoder.Encode(searchVector[:MaxLevels*64])
	total := float64(size)
	oldHeap := make([][]uint64, 1)
	oldHeap[0] = r.initList
	maxDist := 0
	for i := 0; i < MaxLevels; i++ {
		part := int(total * math.Pow(r.rate, float64(i)))
		heap := make([][]uint64, maxDist+64)
		soFar := 0
		for j := 0; soFar < part; j++ {
			soFar += len(oldHeap[j])
			for _, curr := range oldHeap[j] {
				distance := float32(bits.OnesCount64(r.codes[i][curr] ^ searchCode[i]))
				correctedDistance := j + int(distance)
				if heap[correctedDistance] == nil {
					slice := r.pool.Get()
					defer r.pool.Put(slice)
					heap[correctedDistance] = *slice
				}
				heap[correctedDistance] = append(heap[correctedDistance], curr)
				if correctedDistance > maxDist {
					maxDist = correctedDistance
				}
			}
		}
		oldHeap = heap
	}

	heap := priorityqueue.NewMax[any](k)
	for j := 0; j < len(oldHeap); j++ {
		for _, curr := range oldHeap[j] {
			distance, _ := r.distancerProvider.SingleDist(r.vectors[curr], searchVector)
			heap.Insert(curr, distance)
			if heap.Len() > k {
				heap.Pop()
			}
		}
	}

	dists := make([]float32, k)
	ids := make([]uint64, k)

	for i := k - 1; i >= 0; i-- {
		elem := heap.Pop()
		dists[i] = elem.Dist
		ids[i] = elem.ID
	}
	return ids, dists, nil
}

/*func (r *RegionalIVF) SearchByVector(ctx context.Context, searchVector []float32,
	k int, allowList helpers.AllowList) ([]uint64, []float32, error) {

	searchCode := r.bencoder.Encode(searchVector)
	size := len(r.vectors)

	// Use a single slice that we progressively shrink
	candidates := make([]uint64, size)
	distances := make([]float32, size)

	// Initialize with all candidate IDs
	for i := 0; i < size; i++ {
		candidates[i] = uint64(i)
	}

	candidateCount := size

	// Progressive filtering
	for level := 0; level < MaxLevels; level++ {
		keepCount := int(float64(size) * math.Pow(rate, float64(level+1)))
		if keepCount < k {
			keepCount = k
		}
		if keepCount >= candidateCount {
			continue
		}

		// Compute distances for current candidates
		for i := 0; i < candidateCount; i++ {
			distances[i] = float32(bits.OnesCount64(r.codes[level][candidates[i]] ^ searchCode[level]))
		}

		// Partial sort to find keepCount best candidates
		r.partialSortInPlace(candidates, distances, candidateCount, keepCount)
		candidateCount = keepCount

		if candidateCount <= k*2 {
			break
		}
	}

	// Final distance computation and sorting
	for i := 0; i < candidateCount; i++ {
		distance, _ := r.distancerProvider.SingleDist(r.vectors[candidates[i]], searchVector)
		distances[i] = distance
	}

	// Final sort
	finalCount := candidateCount
	if finalCount > k {
		r.partialSortInPlace(candidates, distances, candidateCount, k)
		finalCount = k
	} else {
		r.fullSortInPlace(candidates, distances, finalCount)
	}

	// Return results (slices of the working arrays)
	return candidates[:finalCount], distances[:finalCount], nil
}

// partialSortInPlace sorts in-place and keeps only the first keepCount elements
func (r *RegionalIVF) partialSortInPlace(candidates []uint64, distances []float32, length, keepCount int) {
	// Simple quickselect implementation
	r.quickSelectInPlace(candidates, distances, 0, length-1, keepCount-1)
}

// fullSortInPlace performs a complete sort on the first 'length' elements
func (r *RegionalIVF) fullSortInPlace(candidates []uint64, distances []float32, length int) {
	// Insertion sort for small arrays (very efficient for small k)
	if length <= 32 {
		for i := 1; i < length; i++ {
			candKey := candidates[i]
			distKey := distances[i]
			j := i - 1

			for j >= 0 && distances[j] > distKey {
				candidates[j+1] = candidates[j]
				distances[j+1] = distances[j]
				j--
			}

			candidates[j+1] = candKey
			distances[j+1] = distKey
		}
	} else {
		// Use Go's built-in sort for larger arrays
		type pair struct {
			id   uint64
			dist float32
		}

		pairs := make([]pair, length)
		for i := 0; i < length; i++ {
			pairs[i] = pair{candidates[i], distances[i]}
		}

		sort.Slice(pairs, func(i, j int) bool {
			return pairs[i].dist < pairs[j].dist
		})

		for i := 0; i < length; i++ {
			candidates[i] = pairs[i].id
			distances[i] = pairs[i].dist
		}
	}
}

// quickSelectInPlace performs quickselect with in-place swapping
func (r *RegionalIVF) quickSelectInPlace(candidates []uint64, distances []float32, left, right, k int) {
	for left < right {
		pivotIdx := r.partitionInPlace(candidates, distances, left, right)

		if pivotIdx == k {
			return
		} else if pivotIdx < k {
			left = pivotIdx + 1
		} else {
			right = pivotIdx - 1
		}
	}
}

func (r *RegionalIVF) partitionInPlace(candidates []uint64, distances []float32, left, right int) int {
	// Simple pivot selection (middle element)
	mid := left + (right-left)/2
	pivot := distances[mid]

	// Move pivot to end
	candidates[mid], candidates[right] = candidates[right], candidates[mid]
	distances[mid], distances[right] = distances[right], distances[mid]

	storeIdx := left
	for i := left; i < right; i++ {
		if distances[i] < pivot {
			candidates[i], candidates[storeIdx] = candidates[storeIdx], candidates[i]
			distances[i], distances[storeIdx] = distances[storeIdx], distances[i]
			storeIdx++
		}
	}

	// Move pivot to its final position
	candidates[storeIdx], candidates[right] = candidates[right], candidates[storeIdx]
	distances[storeIdx], distances[right] = distances[right], distances[storeIdx]

	return storeIdx
}*/

// Resto de implementaciones stub para satisfacer la interfaz VectorIndex

func (r *RegionalIVF) SearchByVectorDistance(ctx context.Context, vector []float32, dist float32,
	maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, fmt.Errorf("SearchByVectorDistance not implemented yet")
}

func (r *RegionalIVF) Delete(ids ...uint64) error {
	return nil
}

func (r *RegionalIVF) UpdateUserConfig(updated config.VectorIndexConfig, callback func()) error {
	return nil
}

func (r *RegionalIVF) Drop(ctx context.Context) error {
	return nil
}

func (r *RegionalIVF) Shutdown(ctx context.Context) error {
	return nil
}

func (r *RegionalIVF) Flush() error {
	return nil
}

func (r *RegionalIVF) SwitchCommitLogs(ctx context.Context) error {
	return nil
}

func (r *RegionalIVF) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return []string{}, nil
}

func (r *RegionalIVF) PostStartup() {
	// No-op
}

func (r *RegionalIVF) Compressed() bool {
	return false
}

func (r *RegionalIVF) DistanceBetweenVectors(x, y []float32) (float32, error) {
	return r.distancerProvider.SingleDist(x, y)
}

func (r *RegionalIVF) ContainsNode(id uint64) bool {
	return false
}

func (r *RegionalIVF) AlreadyIndexed() uint64 {
	return atomic.LoadUint64(&r.totalVectors)
}

func (r *RegionalIVF) Iterate(fn func(id uint64) bool) {
}

func (r *RegionalIVF) DistancerProvider() distancer.Provider {
	return r.distancerProvider
}

func (r *RegionalIVF) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	distancer := r.distancerProvider.New(queryVector)

	f := func(nodeID uint64) (float32, error) {
		return distancer.Distance(r.vectors[nodeID])
	}

	return common.QueryVectorDistancer{DistanceFunc: f}
}

func (r *RegionalIVF) Stats() (common.IndexStats, error) {
	return nil, fmt.Errorf("Stats not implemented yet")
}

func (r *RegionalIVF) Dump(labels ...string) {
}
