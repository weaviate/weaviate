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
	//GW

    //GW
    "github.com/go-openapi/strfmt"
    "github.com/google/uuid"
    //"example.com/gemini"
    //GW

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/schema"
)

//GW type Index struct{}
type Index struct{
    name strfmt.UUID
    db_path string
    
}
//GW

func NewIndex() *Index {
	//GW return &Index{}

    //GW
    idx := &Index{}
    idx.name = strfmt.UUID(uuid.New().String())
    idx.db_path = fmt.Sprintf("/var/lib/weaviate/%s.npy", idx.name.String())
    fmt.Println("NOOP GEMINI NewIndex path=", idx.db_path)
    return idx
    //GW    

}

func (i *Index) Add(id uint64, vector []float32) error {
	//GW
    fmt.Println("NOOP Add!")
    //GW

    //GW
    farr := make([][]float32, 1)
    dim := int64( len(vector) )
    fmt.Println("Add Dim", dim)
    //farr[0] := make([]float32, dim)

    for i := 0; i< 1; i++ {
        farr[i] = vector
    }
    //gemini.Read_float32_array( f, farr, dim, 0, 1000, 128 )

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

	return nil, nil, errors.Errorf("cannot vector-search on a class not vector-indexed")
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
