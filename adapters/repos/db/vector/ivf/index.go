package ivf

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

const (
	DefaultCentroids = 256 // 2^8 para usar 1 byte
	DefaultRotations = 10  // número de rotaciones
	TrucAt           = 384
)

type Vector []float32

type VectorIndexEncoder struct {
	vectors         [][]Vector         // [nivel][índice_vector]Vector
	vectorLength    int                // longitud fija de cada vector
	vectorsPerLevel int                // número de vectores por nivel (máx 256)
	numLevels       int                // número de niveles
	provider        distancer.Provider // provider para cálculo de distancias
	rng             *rand.Rand         // generador de números aleatorios
}

func NewVectorIndex(vectorLength, vectorsPerLevel, numLevels int, provider distancer.Provider) (*VectorIndexEncoder, error) {
	if vectorsPerLevel > 256 {
		return nil, fmt.Errorf("vectorsPerLevel no puede ser mayor a 256, recibido: %d", vectorsPerLevel)
	}
	if vectorLength <= 0 {
		return nil, fmt.Errorf("vectorLength debe ser positivo, recibido: %d", vectorLength)
	}
	if numLevels <= 0 {
		return nil, fmt.Errorf("numLevels debe ser positivo, recibido: %d", numLevels)
	}

	vi := &VectorIndexEncoder{
		vectorLength:    vectorLength,
		vectorsPerLevel: vectorsPerLevel,
		numLevels:       numLevels,
		provider:        provider,
		rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	return vi, nil
}

// GenerateVectors genera todos los vectores aleatorios normalizados
func (vi *VectorIndexEncoder) GenerateVectors() {
	vi.vectors = make([][]Vector, vi.numLevels)

	for level := 0; level < vi.numLevels; level++ {
		vi.vectors[level] = make([]Vector, vi.vectorsPerLevel)

		for i := 0; i < vi.vectorsPerLevel; i++ {
			vi.vectors[level][i] = vi.generateRandomNormalizedVector()
		}
	}
}

// generateRandomNormalizedVector genera un vector aleatorio normalizado de longitud 1
func (vi *VectorIndexEncoder) generateRandomNormalizedVector() Vector {
	vector := make(Vector, vi.vectorLength)

	// Generar valores aleatorios usando distribución normal
	// Esto da una distribución uniforme en la esfera unitaria
	var sumSquares float32 = 0

	for i := 0; i < vi.vectorLength; i++ {
		// Generar número aleatorio con distribución normal
		val := float32(vi.rng.NormFloat64())
		vector[i] = val
		sumSquares += val * val
	}

	// Normalizar el vector (hacer que su longitud sea 1)
	magnitude := float32(math.Sqrt(float64(sumSquares)))
	if magnitude > 0 {
		for i := 0; i < vi.vectorLength; i++ {
			vector[i] /= magnitude
		}
	}

	return vector
}

// FindClosestVectors encuentra el vector más cercano en cada nivel para un vector dado
func (vi *VectorIndexEncoder) FindClosestVectors(queryVector []float32) ([]byte, error) {
	if len(queryVector) != vi.vectorLength {
		return nil, fmt.Errorf("el vector de consulta debe tener longitud %d, recibido: %d",
			vi.vectorLength, len(queryVector))
	}

	if vi.vectors == nil {
		return nil, fmt.Errorf("los vectores no han sido generados. Llama GenerateVectors() primero")
	}

	result := make([]byte, vi.numLevels)

	for level := 0; level < vi.numLevels; level++ {
		closestIndex := 0
		minDistance := float32(math.Inf(1))

		for i := 0; i < vi.vectorsPerLevel; i++ {
			distance, err := vi.provider.SingleDist(queryVector, []float32(vi.vectors[level][i]))
			if err != nil {
				return nil, fmt.Errorf("error calculando distancia en nivel %d, índice %d: %v",
					level, i, err)
			}

			if distance < minDistance {
				minDistance = distance
				closestIndex = i
			}
		}

		result[level] = byte(closestIndex)
	}

	return result, nil
}

type vectorDistance struct {
	index    int
	distance float32
}

func (vi *VectorIndexEncoder) FindOrderedVectorsByDistance(queryVector []float32) ([][]byte, error) {
	if len(queryVector) != vi.vectorLength {
		return nil, fmt.Errorf("el vector de consulta debe tener longitud %d, recibido: %d",
			vi.vectorLength, len(queryVector))
	}

	if vi.vectors == nil {
		return nil, fmt.Errorf("los vectores no han sido generados. Llama GenerateVectors() primero")
	}

	result := make([][]byte, vi.numLevels)

	for level := 0; level < vi.numLevels; level++ {
		// Calcular distancias para todos los vectores en este nivel
		distances := make([]vectorDistance, vi.vectorsPerLevel)

		for i := 0; i < vi.vectorsPerLevel; i++ {
			distance, err := vi.provider.SingleDist(queryVector, []float32(vi.vectors[level][i]))
			if err != nil {
				return nil, fmt.Errorf("error calculando distancia en nivel %d, índice %d: %v",
					level, i, err)
			}

			distances[i] = vectorDistance{
				index:    i,
				distance: distance,
			}
		}

		// Ordenar por distancia (menor a mayor)
		vi.quickSortDistances(distances, 0, len(distances)-1)

		// Crear el arreglo de bytes con los índices ordenados
		result[level] = make([]byte, vi.vectorsPerLevel)
		for i, vd := range distances {
			result[level][i] = byte(vd.index)
		}
	}

	return result, nil
}

// quickSortDistances implementa quicksort para ordenar vectorDistance por distancia
func (vi *VectorIndexEncoder) quickSortDistances(arr []vectorDistance, low, high int) {
	if low < high {
		pi := vi.partitionDistances(arr, low, high)
		vi.quickSortDistances(arr, low, pi-1)
		vi.quickSortDistances(arr, pi+1, high)
	}
}

// partitionDistances función auxiliar para quicksort
func (vi *VectorIndexEncoder) partitionDistances(arr []vectorDistance, low, high int) int {
	pivot := arr[high].distance
	i := low - 1

	for j := low; j < high; j++ {
		if arr[j].distance <= pivot {
			i++
			arr[i], arr[j] = arr[j], arr[i]
		}
	}

	arr[i+1], arr[high] = arr[high], arr[i+1]
	return i + 1
}

// RegionalIVF implementa el algoritmo IVF basado en regiones
type RegionalIVF struct {
	sync.RWMutex

	// Configuración del algoritmo
	numCentroids int   // número de centroides (256 por defecto)
	numRotations int   // número de rotaciones
	dims         int32 // dimensiones de los vectores

	// Centroides para cada rotación
	encoder *VectorIndexEncoder

	// Proveedor de distancias
	distancerProvider distancer.Provider

	// Configuración y estado
	id       string
	rootPath string
	logger   interface{} // logrus.FieldLogger en el contexto real

	// Control de acceso concurrente
	trackDimensionsOnce sync.Once

	// Métricas y estadísticas
	totalVectors uint64

	cache  cache.Cache[float32]
	codes  cache.Cache[byte]
	bcodes cache.Cache[uint64]

	bencoder compressionhelpers.BinaryQuantizer
}

// Config contiene la configuración para RegionalIVF
type Config struct {
	ID               string
	RootPath         string
	DistanceProvider distancer.Provider
	VectorForIDThunk common.VectorForID[float32]
	Logger           interface{}
	NumCentroids     int
	NumRotations     int
	AllocChecker     memwatch.AllocChecker
}

// UserConfig contiene la configuración del usuario
type UserConfig struct {
	NumCentroids int `json:"numCentroids"`
	NumRotations int `json:"numRotations"`
}

// Validate valida la configuración
func (c Config) Validate() error {
	if c.NumCentroids <= 0 {
		return errors.New("numCentroids must be positive")
	}
	if c.NumRotations <= 0 {
		return errors.New("numRotations must be positive")
	}
	if c.NumCentroids > 256 {
		return errors.New("numCentroids cannot exceed 256 for single-byte encoding")
	}
	return nil
}

// New crea una nueva instancia de RegionalIVF
func New(cfg Config, uc UserConfig) (*RegionalIVF, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	numCentroids := uc.NumCentroids
	if numCentroids == 0 {
		numCentroids = DefaultCentroids
	}

	numRotations := uc.NumRotations
	if numRotations == 0 {
		numRotations = DefaultRotations
	}
	logger := logrus.New()

	index := &RegionalIVF{
		numCentroids:      numCentroids,
		numRotations:      numRotations,
		distancerProvider: cfg.DistanceProvider,
		id:                cfg.ID,
		rootPath:          cfg.RootPath,
		logger:            cfg.Logger,
		cache:             cache.NewShardedFloat32LockCache(cfg.VectorForIDThunk, 1000000, logger, false, 0, cfg.AllocChecker),
		codes:             cache.NewShardedByteLockCache(func(ctx context.Context, id uint64) ([]byte, error) { return nil, nil }, 1000000, logger, 0, cfg.AllocChecker),
		bcodes:            cache.NewShardedUInt64LockCache(func(ctx context.Context, id uint64) ([]uint64, error) { return nil, nil }, 1000000, logger, 0, cfg.AllocChecker),
		bencoder:          compressionhelpers.NewBinaryQuantizer(cfg.DistanceProvider),
	}
	index.cache.Grow(1000000)
	index.codes.Grow(1000000)
	index.bcodes.Grow(1000000)

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
		r.encoder, _ = NewVectorIndex(len(vectors[0]), r.numCentroids, r.numRotations, r.distancerProvider)
		r.encoder.GenerateVectors()
	})

	for i, vector := range vectors {
		code, err := r.encoder.FindClosestVectors(vector)
		if err != nil {
			return errors.Wrapf(err, "encode vector %d", ids[i])
		}

		r.cache.Preload(ids[i], vector)
		r.codes.Preload(ids[i], code)
		r.bcodes.Preload(ids[i], r.bencoder.Encode(vector[:TrucAt]))
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

	//qCode, _ := r.encoder.FindOrderedVectorsByDistance(searchVector)
	distancer := r.bencoder.NewDistancer(searchVector[:TrucAt])
	codes := r.bcodes.All()

	kExt := k * 100
	heap := priorityqueue.NewMax[any](kExt)

	for id, code := range codes {
		if len(code) == 0 {
			continue
		}

		/*pos := 0
		rank := 0
		for rot := 0; rot < len(qCode); rot++ {
			for pos = 0; pos < len(qCode[rot]); pos++ {
				if qCode[rot][pos] == code[rot] {
					break
				}
			}
			rank += pos
		}*/
		rank, _ := distancer.Distance(code)
		heap.Insert(uint64(id), float32(rank))
		if heap.Len() > kExt {
			heap.Pop()
		}
	}

	heap2 := priorityqueue.NewMax[any](k)
	for heap.Len() > 0 {
		elem := heap.Pop()
		vec, _ := r.cache.Get(context.Background(), elem.ID)
		d, _ := r.distancerProvider.SingleDist(vec, searchVector)
		heap2.Insert(elem.ID, d)
		if heap2.Len() > k {
			heap2.Pop()
		}
	}

	dists := make([]float32, k)
	ids := make([]uint64, k)

	for i := heap2.Len() - 1; i >= 0; i-- {
		elem := heap2.Pop()
		dists[i] = elem.Dist
		ids[i] = elem.ID
	}
	return ids, dists, nil
}

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
		vector, err := r.cache.Get(context.Background(), nodeID)
		if err != nil {
			return 0, err
		}
		return distancer.Distance(vector)
	}

	return common.QueryVectorDistancer{DistanceFunc: f}
}

func (r *RegionalIVF) Stats() (common.IndexStats, error) {
	return nil, fmt.Errorf("Stats not implemented yet")
}

func (r *RegionalIVF) Dump(labels ...string) {
}
