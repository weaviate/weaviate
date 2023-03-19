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
    "golang.org/x/exp/mmap"
    "sync"
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

    addLock     *sync.RWMutex

    name        strfmt.UUID
    db_path     string
    first_add   bool
    dim         int
    count       int
}
//GW

func NewIndex() *Index {
	//GW return &Index{}

    //GW
    idx := &Index{}
    idx.name        = strfmt.UUID(uuid.New().String())
    idx.db_path     = fmt.Sprintf("/var/lib/weaviate/%s.npy", idx.name.String())
    idx.first_add   = true
    idx.dim         = 0
    idx.count       = 0
    idx.addLock     = &sync.RWMutex{}
    fmt.Println("NOOP GEMINI NewIndex path=", idx.db_path)
    return idx
    //GW    

}

func (i *Index) Add(id uint64, vector []float32) error {

	//GW
    i.addLock.Lock()
    defer i.addLock.Unlock()

    fmt.Println("NOOP Add Before!", i.db_path)
    //GW

    //GW
    //func Append_float32_array(fname string, arr [][]float32, dim int64, count int64) {

    farr := make([][]float32, 1)
    farr[0] = vector
    dim := int64( len(vector) )
    fmt.Println("Add Dim", dim)
    new_count, _, err := gemini.Append_float32_array( i.db_path, farr, int64(dim), int64(1) )
    
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
	
    //GW
    //farr := make([][]float32, 1)
    //dim := int64( len(vector) )
    //fmt.Println("Add Dim", dim)
    //farr[0] := make([]float32, dim)
    //for i := 0; i< 1; i++ {
    //    farr[i] = vector
    //}
    //gemini.Read_float32_array( f, farr, dim, 0, 1000, 128 )
    //
    //gemini.Append_float32_array( "/Users/gwilliams/Projects/GSI/Weaviate/github.fork/weaviate/gsi/tests/dbase.npy", farr, dim, 1 )
    //GW

    return nil
	//GW // silently ignore
	//GW return nil
}

func (i *Index) Delete(id uint64) error {
	// silently ignore
	return nil
}

func (i *Index) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	//GW
    fmt.Println("NOOP SearchByVector!")
    //GW

    if (i.count==0) {
	    return nil, nil, errors.Errorf("No items in the index.")

    } else {

        //func Read_float32_array(f *mmap.ReaderAt, arr [][]float32, dim int64, index int64, count int64, offset int64) (int64,error) {

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
        fmt.Println("NOOP DROP!")
        //GW

	// silently ignore
	return nil
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
