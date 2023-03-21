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
	//GW
	"fmt"
    //"golang.org/x/exp/mmap"
    "sync"
    "os"
    "log"
    //goruntime "runtime"
	//GW

    //GW
    "github.com/go-openapi/strfmt"
    "github.com/google/uuid"
    "example.com/gemini"
    //GW

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

    //GW

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
    fmt.Println("NOOP NewIndex", allocation_id)
    if allocation_id == "" {
        log.Fatalf("Could not find GEMINI_ALLOCATIONID env var.") 
        return nil
    }

    // a valid data_dir is required for gemini files
    data_dir := os.Getenv("GEMINI_DATA_DIRECTORY")
    fmt.Println("NOOP NewIndex", data_dir)
    if data_dir == "" {
        log.Fatalf("Could not find GEMINI_DATA_DIRECTORY env var.") 
        return nil
    }
    _, err := os.Stat(data_dir)
    if os.IsNotExist(err) {
        log.Fatalf("The GEMINI_DATA_DIRECTORY %s is not valid (%v)", data_dir, err)
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
    //GW    

}

func (i *Index) Add(id uint64, vector []float32) error {

	//GW
    i.idxLock.Lock()
    defer i.idxLock.Unlock()

    fmt.Println("NOOP Add Before!", i.db_path)
    //GW

    if i.last_fvs_status != "" {
        // This means import/training/loading is in progress
    
        return errors.Errorf("Import/training/load is in progress.  Cannot add new items while this is in progress.")
    }

    farr := make([][]float32, 1)
    farr[0] = vector
    dim := int64( len(vector) )
    fmt.Println("Add Dim", dim)
    new_count, _, err := gemini.Numpy_append_float32_array( i.db_path, farr, int64(dim), int64(1) )
    if err!=nil {
        return errors.Errorf("There was a problem adding to the index backing store file.")
    }
    
    //GW
    fmt.Println("NOOP Add After!", new_count, dim, err)
    //GW

    if (i.first_add) {
        if (new_count!=1) {
            errs := fmt.Sprintf("First add size mismatch - expected %d but got %d", 1, new_count )
            return errors.Errorf(errs)
        } else {
            i.first_add = false
            i.count = new_count
            i.dim = int(dim)
        }
    } else {
        if (new_count!=(i.count+1)) {
            errs := fmt.Sprintf("Add size mismatch - expected %d but got %d", i.count+1, new_count )
            return errors.Errorf(errs)
        } else {
            i.count = new_count
        }
    }

    i.stale = true
	
    return nil
	//GW // silently ignore
	//GW return nil
}

func (i *Index) Delete(id uint64) error {

    //GW
    return errors.New("Delete is not supported.")
    //GW

	// silently ignore
	return nil
}

func (i *Index) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {

	//GW
    i.idxLock.Lock()
    defer i.idxLock.Unlock()

    fmt.Println("NOOP SearchByVector!")
    //GW

    if (i.count==0) {
	    return nil, nil, errors.Errorf("No items in the gemini index.")

    } else {

        // Do we need to initiate a train/re-train ?
        if ( i.stale ) {
            if ( i.last_fvs_status == "" ) {
                // Initiate gemini index training/re-training asynchronously

                fmt.Println("About to import dataset")

                dataset_id, err := gemini.Fvs_import_dataset( "localhost", 7761, i.allocation_id, "/home/public/deep-1M.npy", 768, true ) //i.verbose );
                if err!=nil {
                    return nil, nil, errors.Wrap(err, "Gemini dataset import failed.")
                } else {
                    i.dataset_id = dataset_id
                    fmt.Println("Got dataset_id=", i.dataset_id)
                }
            }
                
            fmt.Println("About to get train status", i.dataset_id)


            // Query the training status to get the 'last_fvs_status' field
            status, err := gemini.Fvs_train_status( "localhost", 7761, i.allocation_id, i.dataset_id, i.verbose )
            if err!=nil {
                return nil, nil, errors.New("Could not get gemini index training status.")
            }
            i.last_fvs_status = status;
            
            fmt.Println("After train status", i.last_fvs_status)

            // At this point, we have an updated training status
            if ( i.last_fvs_status == "completed" ) {
                fmt.Println("Training done!", i.last_fvs_status)

                // Now load the dataset
                status, err := gemini.Fvs_load_dataset( "localhost", 7761, i.allocation_id, i.dataset_id, true )
                if err!=nil {
                    return nil, nil, errors.New("Load dataset failed.")
                }
                fmt.Println("Load done!",status)
                
                i.stale = false;
                i.last_fvs_status = ""
                return nil, nil, errors.New("Async index build completed.  Next call should complete.")

            } else {
                return nil, nil, errors.New("Asyc index build is in progress.  Please try again later.")
            }
        }
            
        fmt.Println("Before Fvs_search", i.last_fvs_status, i.dataset_id)

        // If we got here, we can proceed with the search
        dist, inds, _, s_err := gemini.Fvs_search( "localhost", 7761, i.allocation_id, i.dataset_id, "/home/public/deep-queries-10.npy", 10, true) //i.verbose );
        if s_err!= nil {
            return nil, nil, errors.New("Gemini index search failed.")
        }

        return inds[0], dist[0], nil

        /*
        f, _ := mmap.Open( i.db_path)
        defer f.Close()

        // allocate array for the index read
        farr := make([][]float32, 1)
        farr[0] = make([]float32, i.dim)

        // read the index at 0th record 
        _, err := gemini.Read_float32_array( f, farr, int64(i.dim), 0, 1, 128 )
        if (err!=nil) {
	        return nil, nil, errors.Errorf("Error reading index.")
        }
   
        // allocate index array for return 
        iarr := make([]uint64,1)
        iarr[0] = 0

        return iarr, farr[0], nil
        */

    }
    //GW
	//GWreturn nil, nil, errors.Errorf("cannot vector-search on a class not vector-indexed")
    //GW
}

func (i *Index) SearchByVectorDistance(vector []float32, dist float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	//GW
        fmt.Println("NOOP SearchByVectorDistance!")
        //GW
	return nil, nil, errors.Errorf("cannot vector-search on a class not vector-indexed")
}

func (i *Index) UpdateUserConfig(updated schema.VectorIndexConfig) error {
	//GW
        fmt.Println("NOOP UpdateUserConfig!")
	//GW
	return errors.Errorf("cannot update vector index config on a non-indexed class. Delete and re-create without skip property")
}

func (i *Index) Drop(context.Context) error {

    //GW
    i.idxLock.Lock()
    defer i.idxLock.Unlock()

    fmt.Println("NOOP DROP!")
    //GW

    err := os.Remove( i.db_path )
    if (err!=nil) {
        return errors.Errorf("Could not remove the file backing store of this index.")
    } else {
	    return nil
    }
}

func (i *Index) Flush() error {
	return nil
}

func (i *Index) Shutdown(context.Context) error {
	return nil
}

func (i *Index) PauseMaintenance(context.Context) error {
	return nil
}

func (i *Index) SwitchCommitLogs(context.Context) error {
	return nil
}

func (i *Index) ListFiles(context.Context) ([]string, error) {
	return nil, nil
}

func (i *Index) ResumeMaintenance(context.Context) error {
	return nil
}

func (i *Index) ValidateBeforeInsert(vector []float32) error {
	//GW
        fmt.Println("NOOP ValidateBeforeInsert")
	//GW
	return nil
}

func (i *Index) PostStartup() {
}

func (i *Index) Dump(labels ...string) {
}
