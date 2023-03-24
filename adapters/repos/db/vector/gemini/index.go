//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package gemini

import (
	"context"
	"fmt"
    "sync"
    "os"
    //"log"
    //goruntime "runtime"

    "github.com/go-openapi/strfmt"
    "github.com/google/uuid"
    gemmod "example.com/gemini"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/schema"

    //GW
    ent "github.com/weaviate/weaviate/entities/vectorindex/gemini"
    //GW
)

//GW type Index struct{}
type gemini struct{

    // global lock to prevent concurrent map read/write, etc.
    sync.RWMutex
    idxLock     *sync.RWMutex

    // a globally unique name for this index
    name        strfmt.UUID

    // the file backing store for this index
    db_path     string

    // a boolean which indicates the first add to the index
    first_add   bool

    // the floating point array dimensions of this index
    dim         int

    // the current record size of this index
    count       int

    // the allocation_id required by gemini
    allocation_id   string
    
    // the gemini dataset_id 
    dataset_id   string

    // the local filesystem directory required by gemini
    data_dir  string

    // indicates that the index needs retraining
    stale       bool

    // indicates the current training status from FVS
    last_fvs_status string

    // fvs server
    fvs_server  string

    // verbose printing for debugging
    verbose     bool

    // determines if we do a min records count
    min_records_check   bool

}
//GW


// New creates a new HNSW index, the commit logger is provided through a thunk
// (a function which can be deferred). This is because creating a commit logger
// opens files for writing. However, checking whether a file is present, is a
// criterium for the index to see if it has to recover from disk or if its a
// truly new index. So instead the index is initialized, with un-biased disk
// checks first and only then is the commit logger created
func New(cfg Config, uc ent.UserConfig) (*gemini, error) {
    //GW
    fmt.Println("HNSW New!")
    //GW

    fmt.Println("stuff", cfg, uc)

    index := &gemini{}
    return index, nil

    //return NewIndex(), nil

    /*
    if err := cfg.Validate(); err != nil {
        return nil, errors.Wrap(err, "invalid config")
    }

    if cfg.Logger == nil {
        logger := logrus.New()
        logger.Out = io.Discard
        cfg.Logger = logger
    }

    normalizeOnRead := false
    if cfg.DistanceProvider.Type() == "cosine-dot" {
        normalizeOnRead = true
    }

    vectorCache := newShardedLockCache(cfg.VectorForIDThunk, uc.VectorCacheMaxObjects,
        cfg.Logger, normalizeOnRead, defaultDeletionInterval)

    resetCtx, resetCtxCancel := context.WithCancel(context.Background())
    index := &hnsw{
        maximumConnections: uc.MaxConnections,

        // inspired by original paper and other implementations
        maximumConnectionsLayerZero: 2 * uc.MaxConnections,

        // inspired by c++ implementation
        levelNormalizer:   1 / math.Log(float64(uc.MaxConnections)),
        efConstruction:    uc.EFConstruction,
        flatSearchCutoff:  int64(uc.FlatSearchCutoff),
        nodes:             make([]*vertex, initialSize),
        cache:             vectorCache,
        vectorForID:       vectorCache.get,
        multiVectorForID:  vectorCache.multiGet,
        id:                cfg.ID,
        rootPath:          cfg.RootPath,
        tombstones:        map[uint64]struct{}{},
        logger:            cfg.Logger,
        distancerProvider: cfg.DistanceProvider,
        deleteLock:        &sync.Mutex{},
        tombstoneLock:     &sync.RWMutex{},
        resetLock:         &sync.Mutex{},
        resetCtx:          resetCtx,
        resetCtxCancel:    resetCtxCancel,
        initialInsertOnce: &sync.Once{},
        cleanupInterval:   time.Duration(uc.CleanupIntervalSeconds) * time.Second,

        ef:       int64(uc.EF),
        efMin:    int64(uc.DynamicEFMin),
        efMax:    int64(uc.DynamicEFMax),
        efFactor: int64(uc.DynamicEFFactor),

        metrics: NewMetrics(cfg.PrometheusMetrics, cfg.ClassName, cfg.ShardName),

        randFunc: rand.Float64,
    }

    index.tombstoneCleanupCycle = cyclemanager.New(index.cleanupInterval, index.tombstoneCleanup)
    index.insertMetrics = newInsertMetrics(index.metrics)

    if err := index.init(cfg); err != nil {
        return nil, errors.Wrapf(err, "init index %q", index.id)
    }

    return index, nil
    */
}


//GW func NewIndex() *Index {
func NewIndex() *gemini {
	//GW return &Index{}

    //goruntime.Breakpoint()
   
    // get special verbose/debug flag if present 
    gemini_verbose := false
    gemini_debug_flag := os.Getenv("GEMINI_DEBUG")
    if gemini_debug_flag == "true" {
        gemini_verbose = true
    }

    // a valid gemini_allocation_id is required
    allocation_id := os.Getenv("GEMINI_ALLOCATION_ID")
    if allocation_id == "" {
        fmt.Println("ERROR: Could not find GEMINI_ALLOCATIONID env var.") 
        return nil
    }
    //TODO: Check valid GUID format 
    
    // a valid gemini_fvs_server is required
    fvs_server := os.Getenv("GEMINI_FVS_SERVER")
    if fvs_server == "" {
        fmt.Println("ERROR: Could not find GEMINI_FVS_SERVER env var.") 
        return nil
    }
    //TODO: Validate the server connection here

    // a valid data_dir is required for gemini files
    data_dir := os.Getenv("GEMINI_DATA_DIRECTORY")
    fmt.Println("NOOP NewIndex", data_dir)
    if data_dir == "" {
        fmt.Println("ERROR: Could not find GEMINI_DATA_DIRECTORY env var.") 
        return nil
    }
    _, err := os.Stat(data_dir)
    if os.IsNotExist(err) {
        fmt.Println("The GEMINI_DATA_DIRECTORY %s is not valid (%v)", data_dir, err)
        return nil
    }

    // possibly override min records check 
    min_records_check := true
    min_records_check_flag := os.Getenv("GEMINI_MIN_RECORDS_CHECK")
    if min_records_check_flag == "false" {
        min_records_check = false
    }

    //idx := &Index{}
    idx := &gemini{}
    idx.name        = strfmt.UUID(uuid.New().String())
    idx.db_path     = fmt.Sprintf("%s/dataset_%s.npy", data_dir, idx.name.String())
    idx.first_add   = true
    idx.dim         = 0
    idx.count       = 0
    idx.allocation_id = allocation_id
    idx.data_dir    = data_dir
    idx.idxLock     = &sync.RWMutex{}
    idx.verbose     = gemini_verbose
    idx.stale       = true
    idx.last_fvs_status = ""
    idx.fvs_server  = fvs_server
    idx.min_records_check   = min_records_check

    if idx.verbose {
        fmt.Println("Gemini Index Contructor db_path=", idx.db_path)
    }

    return idx

}

// func (i *Index) Add(id uint64, vector []float32) error {
func (i *gemini) Add(id uint64, vector []float32) error {

    // sychronize this function
    i.idxLock.Lock()
    defer i.idxLock.Unlock()

    if i.verbose {
        fmt.Println("Gemini Add: Start")
    }

    if i.last_fvs_status != "" {
        // TODO: This means that an async index build is in progress.
        // TODO: In the future We should consider cacheing the adds for a deferred
        // TODO: index build when the current one is complete.
    
        return errors.Errorf("Async index build is in progress.  Cannot add new items while this is in progress.")
    }

    // Get the float vector dimensions
    dim := int( len(vector) )
    // TODO: check any dim constraints

    if i.verbose {
        fmt.Println("Gemini Add: dimension=", dim)
    }
   
    if (i.first_add) {
        // First time adding to this index

        // Prepare the float array for saving
        farr := make([][]float32, 1)
        farr[0] = vector

        // Append vector to the numpy file store via memmapping 
        new_count, _, err := gemmod.Numpy_append_float32_array( i.db_path, farr, int64(dim), int64(1) )
        if err!=nil {
            return errors.Errorf("There was a problem adding to the index backing store file.")
        }
        if new_count!=1 {
            return errors.Errorf("Appending array to local file store did not yield expected result.")
        }

        i.first_add = false
        i.count = 1
        i.dim = dim

    } else {
        // Not the first time adding to this index

        if dim != i.dim {
            return errors.Errorf("Vector has dim=%d but expected %d", dim, i.dim )
        }

        // Prepare the float array for saving
        farr := make([][]float32, 1)
        farr[0] = vector

        // Append vector to the numpy file store via memmapping 
        new_count, _, err := gemmod.Numpy_append_float32_array( i.db_path, farr, int64(dim), int64(1) )
        if err!=nil {
            return errors.Errorf("There was a problem adding to the index backing store file.")
        }

        if (new_count!=(i.count+1)) {
            errs := fmt.Sprintf("Add size mismatch - expected %d but got %d", i.count+1, new_count )
            return errors.Errorf(errs)
        } 
            
        i.count = new_count
    }

    // Always set the current index (if any) to stale so
    // that we immediately initiate building it on demand
    i.stale = true
	
    return nil

	//GW // silently ignore
	//GW return nil
}

//GW func (i *Index) Delete(id uint64) error {
func (i *gemini) Delete(id uint64) error {

    if i.verbose {
        fmt.Println("Gemini SearchByVector: Delete")
    }

    return errors.New("Delete is not supported.")

	//GW // silently ignore
	//GW return nil
}

//GW func (i *Index) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
func (i *gemini) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {

    // sychronize this function
    i.idxLock.Lock()
    defer i.idxLock.Unlock()

    if i.verbose {
        fmt.Println("Gemini SearchByVector: Start")
    }

    if (i.count==0) {
	    return nil, nil, errors.Errorf("No items in the gemini index.")

    } else {

        if  i.stale  {
            // Build/rebuild the index or check on an async build status

            if  i.last_fvs_status == ""  {
                // Initiate the index build asynchronously
               
                if i.verbose { 
                    fmt.Println("Gemini SearchByVector: About to import dataset with dataset_id=", i.dataset_id )
                }

		// TODO: Check that we have enough data.
		// TODO: Note that his arbitrary number of 4001 was surfaced
		// TODO: because we encountered a specific FVS error which indicated
		// TODO: this constraint.  
		if (i.min_records_check && i.count<4001) { 
		    return nil, nil, fmt.Errorf("FVS requires a mininum of 4001 vectors in the dataset.")
		}

                //dataset_id, err := gemmod.Fvs_import_dataset( i.fvs_server, 7761, i.allocation_id, "/home/public/deep-1M.npy", 768, i.verbose );
                dataset_id, err := gemmod.Fvs_import_dataset( i.fvs_server, 7761, i.allocation_id, i.db_path, 768, i.verbose );
                if err!=nil {
                    return nil, nil, errors.Wrap(err, "Gemini dataset import failed.")

                } else {
                    i.dataset_id = dataset_id
                
                    if i.verbose {            
                        fmt.Println("Gemini SearchByVector: Got dataset_id=", i.dataset_id)
                    }
                }
            }
               
            if i.verbose { 
                fmt.Println("Gemini SearchByVector: about to get train status for dataet_id=", i.dataset_id)
            }

            // Query the training status to populate the 'last_fvs_status' field
            status, err := gemmod.Fvs_train_status( i.fvs_server, 7761, i.allocation_id, i.dataset_id, i.verbose )
            if err!=nil {
                return nil, nil, errors.Wrap(err, "Could not get gemini index training status.")
            } else if status == "error" {
                return nil, nil, fmt.Errorf("Gemini training status returned 'error'.")
            } else if status=="" {
                return nil, nil, fmt.Errorf("Gemini training status is not valid.")
            }
            i.last_fvs_status = status; 
           
            if i.verbose { 
                fmt.Println("Gemini SearchByVector: After train status=", i.last_fvs_status)
            }

            // At this point, we have an updated training status
            if ( i.last_fvs_status == "completed" ) {

                if i.verbose {
                    fmt.Println("Gemini SearchByVector: Training done, status=", i.last_fvs_status)
                }

                // TODO: Now load the dataset
                // TODO: Consider doing this asynchronously
                status, err := gemmod.Fvs_load_dataset( i.fvs_server, 7761, i.allocation_id, i.dataset_id, i.verbose )
                if err!=nil {
                    return nil, nil, errors.Wrap(err, "Load dataset failed.")
                }

                if i.verbose {
                    fmt.Println("Gemini SearchByVector: Load done, status=",status)
                }
               
                // Everything is now set for an actual search when this function is called next time
                i.stale = false;
                i.last_fvs_status = ""
                return nil, nil, fmt.Errorf("Async index build completed.  Next API call will run the actual search.")

            } else {
                return nil, nil, fmt.Errorf("Async index build is in progress.  Please try again later.")

            }
        }
           
        //goruntime.Breakpoint()

        if i.verbose { 
            fmt.Println("Gemini SearchByVector: About to perform Fvs_search", i.last_fvs_status, i.dataset_id)
        }
        
        //
        // If we got here, we can proceed with the actual search
        //

        // check dimensions match expected
        dim := int( len(vector) )
        if dim != i.dim {
            return nil, nil, fmt.Errorf("Gemini SearchByVector: Got vector of dim=%d but expected %d", dim, i.dim )
        }

        // Create a unique filename for the numpy query file
        query_id := strfmt.UUID(uuid.New().String())
        query_path := fmt.Sprintf("%s/queries_%s.npy", i.data_dir, query_id.String())
        if i.verbose {
            fmt.Println("Gemini SearchByVector: Before Numpy_append, temp query path=",query_path)
        }

        // Prepare the float array for saving to the file
        farr := make([][]float32, 1)
        farr[0] = vector

        // TODO: Create a temp numpy file for the query array.
        // TODO: Convert code to memory array when FVS api supports it.
        query_count, _, err := gemmod.Numpy_append_float32_array( query_path, farr, int64(dim), int64(1) )
        if err!=nil {
            return nil, nil, errors.Errorf("There was a problem adding to the query backing store file.")
        }           
        if query_count!=1 {
            return nil, nil, errors.Errorf("Appending array to local file store did not yield expected result.")
        }       
        
        //dist, inds, timing, s_err := gemmod.Fvs_search( i.fvs_server, 7761, i.allocation_id, i.dataset_id, "/home/public/deep-queries-10.npy", 10, i.verbose );
        dist, inds, timing, s_err := gemmod.Fvs_search( i.fvs_server, 7761, i.allocation_id, i.dataset_id, query_path, 10, i.verbose );
        if s_err!= nil {
            return nil, nil, errors.Wrap(s_err, "Gemini index search failed.")
        }
        if i.verbose {
            fmt.Println("Gemini SearchByVector: search timing=",timing)
        }

        // Remove the temp file
        rErr := os.Remove( query_path )
        if rErr != nil {
            return nil, nil, errors.Wrap(rErr, "Could not remove the temp queries file")
        }

        return inds[0], dist[0], nil

    }

    //GW
	//GWreturn nil, nil, errors.Errorf("cannot vector-search on a class not vector-indexed")
    //GW
}

//GW func (i *Index) SearchByVectorDistance(vector []float32, dist float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
func (i *gemini) SearchByVectorDistance(vector []float32, dist float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {

    if i.verbose {
        fmt.Println("Gemini SearchByVectorDistance: Start")
    }
	return nil, nil, errors.Errorf("cannot vector-search on a class not vector-indexed")
}

//GW func (i *Index) UpdateUserConfig(updated schema.VectorIndexConfig) error {
func (i *gemini) UpdateUserConfig(updated schema.VectorIndexConfig) error {

    if i.verbose {	
        fmt.Println("Gemini UpdateUserConfig: Start")
    }	
	return errors.Errorf("cannot update vector index config on a non-indexed class. Delete and re-create without skip property")
}

//GW func (i *Index) Drop(context.Context) error {
func (i *gemini) Drop(context.Context) error {

    // sychronize this function
    i.idxLock.Lock()
    defer i.idxLock.Unlock()

    if i.verbose {
        fmt.Println("Gemini Drop: Start")
    }

    err := os.Remove( i.db_path )
    if (err!=nil) {
        return errors.Errorf("Could not remove the file backing store of this index.")
    } else {
	    return nil
    }
}

//func (i *Index) Flush() error {
func (i *gemini) Flush() error {

    if i.verbose {
        fmt.Println("Gemini Flush: Start")
    }

	return nil
}

//GW func (i *Index) Shutdown(context.Context) error {
func (i *gemini) Shutdown(context.Context) error {

    if i.verbose {
        fmt.Println("Gemini Shutdown: Start")
    }

	return nil
}

//GW func (i *Index) PauseMaintenance(context.Context) error {
func (i *gemini) PauseMaintenance(context.Context) error {

    if i.verbose {
        fmt.Println("Gemini PauseMaintenance: Start")
    }

	return nil
}

//GW func (i *Index) SwitchCommitLogs(context.Context) error {
func (i *gemini) SwitchCommitLogs(context.Context) error {
    
    if i.verbose {
        fmt.Println("Gemini SwitchCommitLogs: Start")
    }

	return nil
}

//GW func (i *Index) ListFiles(context.Context) ([]string, error) {
func (i *gemini) ListFiles(context.Context) ([]string, error) {

    if i.verbose {
        fmt.Println("Gemini ListFiles: Start")
    }

	return nil, nil
}

//GW func (i *Index) ResumeMaintenance(context.Context) error {
func (i *gemini) ResumeMaintenance(context.Context) error {
    
    if i.verbose {
        fmt.Println("Gemini ResumeMaintenance: Start")
    }

	return nil
}

//GWfunc (i *Index) ValidateBeforeInsert(vector []float32) error {
func (i *gemini) ValidateBeforeInsert(vector []float32) error {

    if i.verbose {
        fmt.Println("Gemini ValidateBeforeInsert: Start")
    }

	return nil
}

//GW func (i *Index) PostStartup() {
func (i *gemini) PostStartup() {
    
    if i.verbose {
        fmt.Println("Gemini PostStartup: Start")
    }
}

//GW func (i *Index) Dump(labels ...string) {
func (i *gemini) Dump(labels ...string) {
    
    if i.verbose {
        fmt.Println("Gemini Dump: Start")
    }

}
