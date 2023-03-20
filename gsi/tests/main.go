
package main

import (
    "fmt"
    "time"

    //"golang.org/x/exp/mmap"

    //"example.com/greetings"
    "example.com/gemini"
)

func main() {

    verbose := false
    allocation_id := "0b391a1a-b916-11ed-afcb-0242ac1c0002"
    dataset_id := "" //"d6b6bbf9-daf2-4884-8120-c6ab57d3e8bc"

    if (dataset_id=="") {
        fmt.Println("Importing new dataset...")
        did, ierr := gemini.Fvs_import_dataset( "localhost", 7761, allocation_id, "/home/public/deep-1M.npy", 768, verbose )
        if ierr!=nil {
            fmt.Println("Problem importing dataset", ierr)
            return
        }
        dataset_id = did
        fmt.Println("Got new dataset_id", dataset_id)
    } 
        
    fmt.Println("Using dataset_id=", dataset_id)

    already_loaded := false
    fmt.Println("Checking training status...")
    for {
        status, terr := gemini.Fvs_train_status( "localhost", 7761, allocation_id, dataset_id, verbose )
        if terr != nil {
            fmt.Println("Error retrieving training status", terr)
            return  
        }

        time.Sleep(time.Second)

        if status == "completed" {
            fmt.Println("Training completed.")
            break
        } else if status == "loaded" {
            fmt.Println("Training completed and dataset loaded.")
            already_loaded = true
            break
        } else {
            fmt.Println("Training not completed, status=", status)
        } 
    }
   
    if !already_loaded { 
        fmt.Println("Loading dataset", dataset_id, "...")
        status, ld_err := gemini.Fvs_load_dataset( "localhost", 7761, allocation_id, dataset_id, verbose );
        if ld_err != nil {
            fmt.Println("Problem loading dataset", ld_err)
            return
        }
        fmt.Println("Fvs_load_dataset: status=", status)
        if status != "ok" {
            fmt.Println("Load dataset status is not ok", status)
            return
        }
        already_loaded = true
    }
    
    queries_id := ""
    fmt.Println("Importing queries...")
    qid, iq_err := gemini.Fvs_import_queries( "localhost", 7761, allocation_id, "/home/public/deep-queries-10.npy", verbose )
    if iq_err != nil {
        fmt.Println("Problem importing queries", iq_err)
        return
    }
    fmt.Println("Fvs_import_queries results=", qid)
    queries_id = qid

    fmt.Println("Searching...")
    dist, inds, timing, s_err := gemini.Fvs_search( "localhost", 7761, allocation_id, dataset_id, "/home/public/deep-queries-10.npy", 10, verbose );
    if s_err != nil {
        fmt.Println("Problem searching", s_err)
        return
    }
    fmt.Println("Fvs_search results=", dist, inds, timing, s_err)
   
    fmt.Println("Unloading dataset", dataset_id, "...")
    status, uld_err := gemini.Fvs_unload_dataset( "localhost", 7761, allocation_id, dataset_id, verbose );
    if uld_err != nil {
        fmt.Println("Problem unloading dataset", uld_err)
        return
    }
    if status != "ok" {
        fmt.Println("Unload dataset status is not ok", status)
        return
    }
    fmt.Println("Fvs_unload_dataset: status=", status)

    fmt.Println("Deleting queries", queries_id, "...")
    status, dq_err := gemini.Fvs_delete_queries( "localhost", 7761, allocation_id, queries_id, verbose );
    if dq_err != nil {
        fmt.Println("Problem deleting queries", dq_err)
        return
    }
    if status != "ok" {
        fmt.Println("Delete queries status is not ok", status)
        return
    }
    fmt.Println("Fvs_delete_queries: status=", status)

    fmt.Println("Deleting dataset", dataset_id, "...")
    status, dd_err := gemini.Fvs_delete_dataset( "localhost", 7761, allocation_id, dataset_id, verbose );
    if dd_err != nil {
        fmt.Println("Problem deleting dataset", dd_err)
        return
    }
    if status != "ok" {
        fmt.Println("Delete dataset status is not ok", status)
        return
    }
    fmt.Println("Fvs_delete_dataset: status=", status)
 
    /*
    f, _ := mmap.Open("./deep-1M.npy")
    defer f.Close()

    dim := int64(96)
    farr := make([][]float32, 1000)
    for i := 0; i<1000; i++ {
        farr[i] = make([]float32, dim)
    }
    
    for i := 0; i< 1000; i++ {
        gemini.Read_float32_array( f, farr, dim, int64(i*1000), 1000, 128 )
        gemini.Append_float32_array( "./copy-data.npy", farr, dim, 1000 )
        fmt.Printf("copy progress %d\n", i)
    }
    */

    fmt.Println("Done.")
}
