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

// build with:  go build -buildmode c-shared -o libweaviate.so .

// The function signature must not include neither Go struct nor Go interface nor Go array nor variadic argument.

/*
#include<stdbool.h>
typedef const char cchar_t;
*/
import "C"

import (
	//"encoding/json"
	"context"
	"fmt"
	"github.com/vitaminwater/cgo.wchar"
	"os"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/usecases/config"
)
import "github.com/weaviate/weaviate/adapters/handlers/rest/state"

var started = false
var appState *state.State

//export dumpBucket
func dumpBucket(storeDir_c, propName_c *C.cchar_t) {
	storeDir := wstr2go(storeDir_c)
	propName := wstr2go(propName_c)

	if started {
		fmt.Println("Dump bucket must only be run while weaviate is shut down.  Do not start weaviate when doing a direct bucket dump")
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

//export appClasses
func appClasses() *C.cchar_t {
	if !started {
		fmt.Println("Weaviate must be started before calling functions.")
		os.Exit(1)
	}
	schemaManager := appState.SchemaManager
	if schemaManager == nil {
		msg := fmt.Sprintf("Error: schemaManager is nil")
		cstr := C.CString(msg)
		return cstr
	}
	scheme := schemaManager.GetSchemaSkipAuth()
	classes := scheme.Objects.Classes
	
	out := ""
	for _, class := range classes {
		out = out + fmt.Sprintf("%v\n", class.Class)
		/*if err != nil {
			msg := fmt.Sprintf("Error: %v", err)
			cstr := C.CString(msg)
			return cstr
		}
		*/
	}

	cstr := C.CString(string(out))
	return cstr
}

//export classProperties
func classProperties(className_c *C.cchar_t) *C.cchar_t {
	if !started {
		fmt.Println("Weaviate must be started before calling functions.")
		os.Exit(1)
	}
	className := wstr2go(className_c)
	schemaManager := appState.SchemaManager
	if schemaManager == nil {
		msg := fmt.Sprintf("Error: schemaManager is nil")
		cstr := C.CString(msg)
		return cstr
	}
	scheme := schemaManager.GetSchemaSkipAuth()
	classes := scheme.Objects.Classes
	
	out := ""
	for _, class := range classes {
		if class.Class == className {
			for _, prop := range class.Properties {
				out = out + fmt.Sprintf("%v\n", prop.Name)
			}
		}
	}

	cstr := C.CString(string(out))
	return cstr
}

func wstr2go(cstr *C.cchar_t) string {
	val, err := wchar.WcharStringPtrToGoString(unsafe.Pointer(cstr))
	if err != nil {
		panic(err)
	}
	return val
}

//export startWeaviate
func startWeaviate(dataPath_c, origin_c, clusterHostname_c, defaultVectoriser_c *C.cchar_t) {
	dataPath := wstr2go(dataPath_c)

	if dataPath == "" {
		dataPath = "./data"
	}

	origin := wstr2go(origin_c)
	if origin == "" {
		origin = "http://localhost:8080"
	}

	clusterHostname := wstr2go(clusterHostname_c)
	if clusterHostname == "" {
		clusterHostname = "node1"
	}

	defaultVectoriser := wstr2go(defaultVectoriser_c)
	if defaultVectoriser == "" {
		defaultVectoriser = "none"
	}

	// set environment variables
	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("LOG_FORMAT", "text")
	os.Setenv("PROMETHEUS_MONITORING_ENABLED", "true")
	os.Setenv("GO_BLOCK_PROFILE_RATE", "20")
	os.Setenv("GO_MUTEX_PROFILE_FRACTION", "20")
	os.Setenv("PERSISTENCE_DATA_PATH", dataPath)
	os.Setenv("ORIGIN", origin)
	os.Setenv("QUERY_DEFAULTS_LIMIT", "20")
	os.Setenv("QUERY_MAXIMUM_RESULTS", "10000")
	os.Setenv("CLUSTER_HOSTNAME", clusterHostname)
	os.Setenv("TRACK_VECTOR_DIMENSIONS", "true")
	os.Setenv("CLUSTER_GOSSIP_BIND_PORT", "7100")
	os.Setenv("CLUSTER_DATA_BIND_PORT", "7101")
	os.Setenv("AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED", "true")
	os.Setenv("DEFAULT_VECTORIZER_MODULE", defaultVectoriser)

	config := config.GetConfigOptionGroup()

	fmt.Printf("Starting Weaviate with dataPath: %v, origin: %v, clusterHostname: %v, defaultVectoriser: %v\n", dataPath, origin, clusterHostname, defaultVectoriser)
	appState = rest.MakeAppState(context.Background(), config)

	started = true
}

func main() {
}
