//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
)

const (
	size = 1e6
)

const (
	keySize   = 4
	valueSize = 256
)

type obj struct {
	Key   []byte
	Value []byte
}

func genData() []obj {
	out := make([]obj, size)

	for i := range out {
		out[i] = randomObj()
	}

	return out
}

func randomObj() obj {
	key := make([]byte, keySize)
	rand.Read(key)

	value := make([]byte, valueSize)
	rand.Read(value)

	return obj{Key: key, Value: value}
}

func do() error {
	bucket, err := lsmkv.NewBucket("./my-bucket")
	if err != nil {
		return err
	}

	data := genData()

	beforeInsert := time.Now()
	bucket.Put([]byte("key1"), []byte("some data"))
	bucket.Put([]byte("key2"), []byte("other data"))
	for _, obj := range data {
		bucket.Put(obj.Key, obj.Value)
	}
	bucket.Put([]byte("key3"), []byte("third data"))
	bucket.Put([]byte("key4"), []byte("fourth data"))

	tookInsert := time.Since(beforeInsert)
	fmt.Printf("inserting took %s\n", tookInsert)

	for _, key := range [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
	} {
		beforeQuery := time.Now()
		res, err := bucket.Get([]byte(key))
		if err != nil {
			fmt.Printf("err: %s\n", err)
		}
		tookQuery := time.Since(beforeQuery)

		fmt.Printf("%s - %s - query took %s\n", string(key), string(res), tookQuery)
	}

	fmt.Printf("waiting for shutdown")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := bucket.Shutdown(ctx); err != nil {
		return err
	}

	return nil
}

func main() {
	rand.Seed(time.Now().Unix())
	if err := do(); err != nil {
		log.Fatal(err.Error())
	}
}
