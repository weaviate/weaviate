//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package main

import (
	"context"
	"fmt"
	"log"

	"github.com/coreos/etcd/clientv3"
)

func main2() {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"http://localhost:2379"}})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	fmt.Println("accessing a key that does not exist")
	res, err := cli.Get(context.TODO(), "test_foo1")
	fmt.Printf("res: %#v\nerro: %s\n\n", res, err)

	fmt.Println("creating the key")
	createRes, err := cli.Put(context.TODO(), "test_foo1", "now it has some value")
	fmt.Printf("res: %#v\nerro: %s\n\n", createRes, err)

	fmt.Println("accessing the key again (it should exist now)")
	res, err = cli.Get(context.TODO(), "test_foo1")
	fmt.Printf("res: %#v\nerro: %s\n\n", res, err)

	fmt.Println("deleting the key")
	delRes, err := cli.Delete(context.TODO(), "test_foo1")
	fmt.Printf("res: %#v\nerro: %s\n\n", delRes, err)
}
