package main

import (
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

func dumpBucket(storeDir, propName string) {
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

func main() {
	config := config.GetConfigOptionGroup()
	rest.MakeAppState(context.Background(), config)
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s <storeDir> <propName>\n", os.Args[0])
		os.Exit(1)
	}
	storeDir := os.Args[1]
	propName := os.Args[2]
	dumpBucket(storeDir, propName)

}
