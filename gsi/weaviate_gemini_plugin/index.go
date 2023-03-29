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

    "github.com/go-openapi/strfmt"
    "github.com/google/uuid"
	"github.com/pkg/errors"
)

type Gemini struct{

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

func New( centroidsHammingK int, centroidsRerank int, hammingK int, nbits int, searchtype string ) (*Gemini, error) {

    // get special verbose/debug flag if present 
    gemini_verbose := false
    gemini_debug_flag := os.Getenv("GEMINI_DEBUG")
    if gemini_debug_flag == "true" {
        gemini_verbose = true
    }

    // a valid gemini_allocation_id is required
    allocation_id := os.Getenv("GEMINI_ALLOCATION_ID")
    if allocation_id == "" {
        fmt.Println("ERROR: Could not find GEMINI_ALLOCATION_ID env var.") 
        return nil, fmt.Errorf("Could not find GEMINI_ALLOCATION_ID env var." )
    }
    //TODO: Check valid allocation id (GUID) format 
    
    // a valid gemini_fvs_server is required
    fvs_server := os.Getenv("GEMINI_FVS_SERVER")
    if fvs_server == "" {
        fmt.Println("ERROR: Could not find GEMINI_FVS_SERVER env var.") 
        return nil, fmt.Errorf("Could not find GEMINI_FVS_SERVER env var." )
    }

    //TODO: Validate the server connection here

    // a valid data_dir is required for gemini files
    data_dir := os.Getenv("GEMINI_DATA_DIRECTORY")
    if data_dir == "" {
        fmt.Println("ERROR: Could not find GEMINI_DATA_DIRECTORY env var.") 
        return nil, fmt.Errorf("Could not find GEMINI_DATA_DIRECTORY env var." )
    }
    _, err := os.Stat(data_dir)
    if os.IsNotExist(err) {
        fmt.Println("The GEMINI_DATA_DIRECTORY %s is not valid (%v)", data_dir, err)
        return nil, errors.Wrapf(err, "The GEMINI_DATA_DIRECTORY %s is not valid", data_dir )
    }

    // possibly override min records check 
    min_records_check := true
    min_records_check_flag := os.Getenv("GEMINI_MIN_RECORDS_CHECK")
    if min_records_check_flag == "false" {
        min_records_check = false
    }

    idx := &Gemini{}
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

    return idx, nil

}

func (i *Gemini) Add(id uint64, vector []float32) error {

    // sychronize this function
    i.idxLock.Lock()
    defer i.idxLock.Unlock()

    if i.verbose {
        fmt.Println("Gemini Add: Start")
    }

    if i.last_fvs_status != "" {
        // TODO: This means that an async index build is in progress.
        // TODO: In the future We should consider cacheiing the adds for a deferred
        // TODO: index build which starts after the current one is complete.
        // TODO: For now, we return an error with a suitable error string.
    
        return errors.Errorf("Async index build is in progress.  Cannot add new items while this is in progress.")
    }

    // Get the float vector dimensions
    dim := int( len(vector) )
    // TODO: We should eventually check dimension constraints.

    if i.verbose {
        fmt.Println("Gemini Add: dimension=", dim)
    }
   
    if (i.first_add) {
        // First time adding to this index

        // Prepare the float array for saving
        farr := make([][]float32, 1)
        farr[0] = vector

        // Append vector to the numpy file store via memmapping 
        new_count, _, err := Numpy_append_float32_array( i.db_path, farr, int64(dim), int64(1) )
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
        new_count, _, err := Numpy_append_float32_array( i.db_path, farr, int64(dim), int64(1) )
        if err!=nil {
            return errors.Errorf("There was a problem adding to the index backing store file.")
        }

        if (new_count!=(i.count+1)) {
            errs := fmt.Sprintf("Add size mismatch - expected %d but got %d", i.count+1, new_count )
            return errors.Errorf(errs)
        } 
            
        i.count = new_count
    }

    // By setting to true, next search kicks off an async index build/train
    i.stale = true
	
    return nil

}

func (i *Gemini) Delete(id uint64) error {

    if i.verbose {
        fmt.Println("Gemini SearchByVector: Delete")
    }

    // TODO: We need to eventually implement the 'D' in CRUD semantics provided by native HNSW.
    return errors.New("Delete is not supported.")

}

func (i *Gemini) SearchByVector(vector []float32, k int) ([]uint64, []float32, error) {

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

		if (i.min_records_check && i.count<4001) { 
		    return nil, nil, fmt.Errorf("FVS requires a mininum of 4001 vectors in the dataset.")
		}

                dataset_id, err := Import_dataset( i.fvs_server, DefaultFVSPort, i.allocation_id, i.db_path, 768, i.verbose );
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
            status, err := Train_status( i.fvs_server, DefaultFVSPort, i.allocation_id, i.dataset_id, i.verbose )
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
                status, err := Load_dataset( i.fvs_server, DefaultFVSPort, i.allocation_id, i.dataset_id, i.verbose )
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
           
        if i.verbose { 
            fmt.Println("Gemini SearchByVector: About to perform Fvs search", i.last_fvs_status, i.dataset_id)
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
        // TODO: We should eventually convert the code to support memory array when the
        // TODO: FVS supports it.
        query_count, _, err := Numpy_append_float32_array( query_path, farr, int64(dim), int64(1) )
        if err!=nil {
            return nil, nil, errors.Errorf("There was a problem adding to the query backing store file.")
        }           
        if query_count!=1 {
            return nil, nil, errors.Errorf("Appending array to local file store did not yield expected result.")
        }       
        
        //dist, inds, timing, s_err := Search( i.fvs_server, DefaultFVSPort, i.allocation_id, i.dataset_id, "/home/public/deep-queries-10.npy", 10, i.verbose );
        dist, inds, timing, s_err := Search( i.fvs_server, DefaultFVSPort, i.allocation_id, i.dataset_id, query_path, 10, i.verbose );
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
}

func (i *Gemini) SearchByVectorDistance(vector []float32, dist float32, maxLimit int64) ([]uint64, []float32, error) {

    if i.verbose {
        fmt.Println("Gemini SearchByVectorDistance: Start")
    }
	return nil, nil, errors.Errorf("cannot vector-search on a class not vector-indexed")
}

func (i *Gemini) UpdateUserConfig() error {

    if i.verbose {	
        fmt.Println("Gemini UpdateUserConfig: Start")
    }	
	return errors.Errorf("cannot update vector index config on a non-indexed class. Delete and re-create without skip property")
}


func (i *Gemini) Drop(context.Context) error {

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

func (i *Gemini) Flush() error {

    if i.verbose {
        fmt.Println("Gemini Flush: Start")
    }

	return nil
}

func (i *Gemini) Shutdown(context.Context) error {

    if i.verbose {
        fmt.Println("Gemini Shutdown: Start")
    }

	return nil
}

func (i *Gemini) PauseMaintenance(context.Context) error {

    if i.verbose {
        fmt.Println("Gemini PauseMaintenance: Start")
    }

	return nil
}

func (i *Gemini) SwitchCommitLogs(context.Context) error {
    
    if i.verbose {
        fmt.Println("Gemini SwitchCommitLogs: Start")
    }

	return nil
}

func (i *Gemini) ListFiles(context.Context) ([]string, error) {

    if i.verbose {
        fmt.Println("Gemini ListFiles: Start")
    }

	return nil, nil
}

func (i *Gemini) ResumeMaintenance(context.Context) error {
    
    if i.verbose {
        fmt.Println("Gemini ResumeMaintenance: Start")
    }

	return nil
}

func (i *Gemini) ValidateBeforeInsert(vector []float32) error {

    if i.verbose {
        fmt.Println("Gemini ValidateBeforeInsert: Start")
    }

	return nil
}

func (i *Gemini) PostStartup() {
    
    if i.verbose {
        fmt.Println("Gemini PostStartup: Start")
    }
}

func (i *Gemini) Dump(labels ...string) {
    
    if i.verbose {
        fmt.Println("Gemini Dump: Start")
    }
}
