package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
)

const (
	size = 1e5
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
		time.Sleep(50 * time.Microsecond)
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
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

func main() {
	rand.Seed(time.Now().Unix())
	if err := do(); err != nil {
		log.Fatal(err.Error())
	}
}
