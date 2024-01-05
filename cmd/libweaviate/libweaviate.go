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

package main

// build with:  go build -buildmode c-shared -o libweaviate.so .

// The function signature must not include neither Go struct nor Go interface nor Go array nor variadic argument.

import (
	"C"
	"context"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/usecases/config"
)

var started = false

//export dumpBucket
func dumpBucket(storeDir, propName string) {
	if !started {
		fmt.Println("Weaviate has not been started!  Call startWeaviate() before any other function")
		os.Exit(1)
	}

	kvstore, err := lsmkv.New(storeDir, storeDir, &logrus.Logger{}, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		panic(err)
	}

	fmt.Printf("propName: %s\n", propName)
	bucketName := helpers.BucketFromPropNameLSM(propName)
	fmt.Printf("bucketName: %s\n", bucketName)
	err = kvstore.CreateOrLoadBucket(context.Background(), bucketName, lsmkv.WithStrategy(lsmkv.StrategyMapCollection))
	if err != nil {
		panic(err)
	}

	bucket := kvstore.Bucket(bucketName)
	fmt.Printf("Dir: %v, bucket %v\n", storeDir, bucketName)
	bucket.IterateMapObjects(context.Background(), func(k1, k2, v []byte, tombstone bool) error {
		fmt.Printf("k1: %s\n", k1)
		fmt.Printf("k2: %x\n", k2)
		fmt.Printf("v: %x\n", v)
		fmt.Printf("tombstone: %v\n", tombstone)
		fmt.Println("-----")
		return nil
	})
	fmt.Printf("Dir: %v, bucket %v\n", storeDir, bucketName)
}

//export startWeaviate
func startWeaviate() {
	config := config.GetConfigOptionGroup()
	rest.MakeAppState(context.Background(), config)
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s <storeDir> <propName>\n", os.Args[0])
		os.Exit(1)
	}
	started = true
}

func main() {
}
