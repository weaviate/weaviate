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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/weaviate/weaviate/adapters/handlers/rest"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/config"
)

func main() {
	dataPath := flag.String("data-path", "./data", "path to the data directory")
	origin := flag.String("origin", "http://localhost:8080", "listen address")
	clusterHostname := flag.String("cluster-hostname", "node1", "cluster hostname")
	defaultVectoriser := flag.String("default-vectoriser", "none", "default vectoriser")
	dumpAll := flag.Bool("dump-all", false, "dump all data")
	dumpLimit := flag.Int("dump-limit", 10, "per-bucket limit of rows to dump")
	checkCorruption := flag.Bool("check-corruption", false, "check for index corruption")
	flag.Parse()
	// set environment variables
	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("LOG_FORMAT", "text")
	os.Setenv("PROMETHEUS_MONITORING_ENABLED", "true")
	os.Setenv("GO_BLOCK_PROFILE_RATE", "20")
	os.Setenv("GO_MUTEX_PROFILE_FRACTION", "20")
	os.Setenv("PERSISTENCE_DATA_PATH", *dataPath)
	os.Setenv("ORIGIN", *origin)
	os.Setenv("QUERY_DEFAULTS_LIMIT", "20")
	os.Setenv("QUERY_MAXIMUM_RESULTS", "10000")
	os.Setenv("CLUSTER_HOSTNAME", *clusterHostname)
	os.Setenv("TRACK_VECTOR_DIMENSIONS", "true")
	os.Setenv("CLUSTER_GOSSIP_BIND_PORT", "7100")
	os.Setenv("CLUSTER_DATA_BIND_PORT", "7101")
	os.Setenv("AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED", "true")
	os.Setenv("DEFAULT_VECTORIZER_MODULE", *defaultVectoriser)

	config := config.GetConfigOptionGroup()
	appState := rest.MakeAppState(context.Background(), config)

	for _, index := range appState.DB.GetIndices() {
		fmt.Printf("index: %v\n", index.ID())
		for _, shard := range index.GetShards() {
			fmt.Printf("	shard: %v\n", shard.ID())
			store := shard.GetStore()
			buckets := store.GetBucketsByName()

			for name, bucket := range buckets {
				fmt.Printf("		bucket: %v\n", name)
				fmt.Printf("			dir: %v\n", bucket.GetDir())
				fmt.Printf("			strategy: %v\n", bucket.GetStrategy())
				if *dumpAll {
					if bucket.GetStrategy() == "mapcollection" {

						count := 0
						bucket.IterateMapObjects(context.Background(), func(k1, k2, v []byte, tombstone bool) error {
							count += 1
							fmt.Printf("				k1: %s\n", k1)
							_, err := bucket.MapList(k1)
							if err != nil {
								fmt.Printf("%v in lsm but not accessable!", k1)
							}
							if count < *dumpLimit {
								return nil
							} else {
								return fmt.Errorf("done")
							}
						})
					}
				}

				if *checkCorruption {
					if bucket.GetStrategy() == "mapcollection" {
						fmt.Printf("\n\n\nTesting bucket %v for index corruption\n", name)
						// Get a bucket by name to test

						// Now iterate over every key in the bucket and attempt to get it with the get function
						bucket.IterateMapObjects(context.Background(), func(k1, k2, v []byte, tombstone bool) error {
							mps, err := bucket.MapList(k1)
							if err != nil {
								fmt.Printf("%v in lsm but not accessable!\n", k1)
								fmt.Printf("Bucket is corrupted: %v\n", err)
								os.Exit(1)
							}
							if len(mps) == 0 {
								fmt.Printf("No map entries for %v, %v has a corrupted index\n\n", string(k1), name)
								os.Exit(1)
							}
							return nil
						})
					}
				}

				if bucket.GetStrategy() == "replace" {
					if *dumpAll {
						count := 0
						bucket.IterateObjects(context.Background(), func(obj *storobj.Object) error {
							count += 1
							fmt.Printf("				id: %v\n", obj.ID())
							properties := obj.Properties().(map[string]interface{})
							for k, v := range properties {
								_, err := bucket.Get([]byte(k))
								if err != nil {
									fmt.Printf("%v in lsm but not accessable!", k)
								}
								val := fmt.Sprintf("%v", v)
								if len(val) > 100 {
									val = val[:100]
								}
								val = strings.ReplaceAll(val, "\n", "")
								fmt.Printf("				%v: %v\n", k, val)
							}
							if count < *dumpLimit {
								return nil
							} else {
								return fmt.Errorf("done")
							}
						})
					}
				}
			}

		}
	}

	/* TODO dump vector repo
	vectorRepo := appState.DB
	fmt.Printf("vectorRepo: %+v\n", vectorRepo)
	*/
	os.Exit(0)
}
