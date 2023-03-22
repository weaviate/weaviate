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

package noop

import (
	"context"
	"fmt"
    "sync"
    "os"
    //"log"
    //goruntime "runtime"

    "github.com/go-openapi/strfmt"
    "github.com/google/uuid"
    "example.com/gemini"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/schema"
)

//GW type Index struct{}
type Index struct{

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

    // verbose printing for debugging
    verbose     bool


}
//GW

func NewIndex() *Index {
	//GW return &Index{}

    //goruntime.Breakpoint()
   
    // get special verbose/debug flag if present 
    gemini_verbose := false
    gemini_debug_flag := os.Getenv("GEMINI_DEBUG")
    if gemini_debug_flag == "" {
        gemini_verbose = false
    } else if gemini_debug_flag == "false" {
        gemini_verbose = false
    } else {
        gemini_verbose = true
    }

    // a valid gemini_allocation_id is required
    allocation_id := os.Getenv("GEMINI_ALLOCATION_ID")
    if allocation_id == "" {
        fmt.Println("ERROR: Could not find GEMINI_ALLOCATIONID env var.") 
        return nil
    }

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

    idx := &Index{}
    idx.name        = strfmt.UUID(uuid.New().String())
    idx.db_path     = fmt.Sprintf("/var/lib/weaviate/%s.npy", idx.name.String())
    idx.first_add   = true
    idx.dim         = 0
    idx.count       = 0
    idx.allocation_id = allocation_id
    idx.data_dir    = data_dir
    idx.idxLock     = &sync.RWMutex{}
    idx.verbose     = gemini_verbose
    idx.stale       = true
    idx.last_fvs_status = ""
    fmt.Println("NOOP GEMINI NewIndex path=", idx.db_path)
    return idx

}

func (i *Index) Add(id uint64, vector []float32) error {

    i.idxLock.Lock()
    defer i.idxLock.Unlock()

    if i.verbose {
        fmt.Println("Gemini Add: Start")
    }

    if i.last_fvs_status != "" {
        // TODO: This means that an index build is in progress.
        // TODO: We should consider cacheing the adds for a deferred
        // TODO: index build when the current one is complete.
    
        return errors.Errorf("Async index build is in progress.  Cannot add new items while this is in progress.")
    }

    // Get the float vector dimensions
    dim := int( len(vector) )
    // TODO: check any dim constraints
    if i.verbose {
        fmt.Println("Gemini Add: dimension=", dim)
    }
   
    if i.verbose {
        fmt.Println("Gemini Add: After Numpy_append")
    }

    if (i.first_add) {
        // First time adding to this index

        // Prepare the float array for saving
        farr := make([][]float32, 1)
        farr[0] = vector

        // Append vector to the numpy file store via memmapping 
        new_count, _, err := gemini.Numpy_append_float32_array( i.db_path, farr, int64(dim), int64(1) )
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
        new_count, _, err := gemini.Numpy_append_float32_array( i.db_path, farr, int64(dim), int64(1) )
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

func (i *Index) Delete(id uint64) error {

    if i.verbose {
        fmt.Println("Gemini SearchByVector: Delete")
    }

    return errors.New("Delete is not supported.")

	//GW // silently ignore
	//GW return nil
}

func (i *Index) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {

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

                dataset_id, err := gemini.Fvs_import_dataset( "localhost", 7761, i.allocation_id, "/home/public/deep-1M.npy", 768, i.verbose );
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
            status, err := gemini.Fvs_train_status( "localhost", 7761, i.allocation_id, i.dataset_id, i.verbose )
            if err!=nil {
                return nil, nil, errors.Wrap(err, "Could not get gemini index training status.")
            }
            if status=="" {
                return nil, nil, errors.Wrap(err, "Gemini training status is not valid.")
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
                status, err := gemini.Fvs_load_dataset( "localhost", 7761, i.allocation_id, i.dataset_id, true )
                if err!=nil {
                    return nil, nil, errors.Wrap(err, "Load dataset failed.")
                }

                if i.verbose {
                    fmt.Println("Gemini SearchByVector: Load done, status=",status)
                }
               
                // Everything is now set for an actual search when this function is called next time
                i.stale = false;
                i.last_fvs_status = ""
                return nil, nil, fmt.Errorf("Async index build completed.  Next call will run an actual search.")

            } else {
                return nil, nil, fmt.Errorf("Async index build is in progress.  Please try again later.")

            }
        }
           
        //goruntime.Breakpoint()

        if i.verbose { 
            fmt.Println("Gemini: SearchByVector: About to perform Fvs_search", i.last_fvs_status, i.dataset_id)
        }

        // If we got here, we can proceed with the actual search
        dist, inds, _, s_err := gemini.Fvs_search( "localhost", 7761, i.allocation_id, i.dataset_id, "/home/public/deep-queries-10.npy", 10, i.verbose );
        if s_err!= nil {
            return nil, nil, errors.Wrap(s_err, "Gemini index search failed.")
        }

        return inds[0], dist[0], nil

    }

    //GW
	//GWreturn nil, nil, errors.Errorf("cannot vector-search on a class not vector-indexed")
    //GW
}

func (i *Index) SearchByVectorDistance(vector []float32, dist float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {

    if i.verbose {
        fmt.Println("Gemini SearchByVectorDistance: Start")
    }
	return nil, nil, errors.Errorf("cannot vector-search on a class not vector-indexed")
}

func (i *Index) UpdateUserConfig(updated schema.VectorIndexConfig) error {

    if i.verbose {	
        fmt.Println("Gemini UpdateUserConfig: Start")
    }	
	return errors.Errorf("cannot update vector index config on a non-indexed class. Delete and re-create without skip property")
}

func (i *Index) Drop(context.Context) error {

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

func (i *Index) Flush() error {

    if i.verbose {
        fmt.Println("Gemini Flush: Start")
    }

	return nil
}

func (i *Index) Shutdown(context.Context) error {

    if i.verbose {
        fmt.Println("Gemini Shutdown: Start")
    }

	return nil
}

func (i *Index) PauseMaintenance(context.Context) error {

    if i.verbose {
        fmt.Println("Gemini PauseMaintenance: Start")
    }

	return nil
}

func (i *Index) SwitchCommitLogs(context.Context) error {
    
    if i.verbose {
        fmt.Println("Gemini SwitchCommitLogs: Start")
    }

	return nil
}

func (i *Index) ListFiles(context.Context) ([]string, error) {

    if i.verbose {
        fmt.Println("Gemini ListFiles: Start")
    }

	return nil, nil
}

func (i *Index) ResumeMaintenance(context.Context) error {
    
    if i.verbose {
        fmt.Println("Gemini ResumeMaintenance: Start")
    }

	return nil
}

func (i *Index) ValidateBeforeInsert(vector []float32) error {

    if i.verbose {
        fmt.Println("Gemini ValidateBeforeInsert: Start")
    }

	return nil
}

func (i *Index) PostStartup() {
    
    if i.verbose {
        fmt.Println("Gemini PostStartup: Start")
    }
}

func (i *Index) Dump(labels ...string) {
    
    if i.verbose {
        fmt.Println("Gemini Dump: Start")
    }

}
