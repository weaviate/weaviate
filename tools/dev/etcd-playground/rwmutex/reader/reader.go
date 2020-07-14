//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package main

import (
	"fmt"
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	recipe "github.com/coreos/etcd/contrib/recipes"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"http://localhost:2379"}})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// create two separate sessions for lock competition
	s1, err := concurrency.NewSession(cli)
	if err != nil {
		log.Fatal(err)
	}
	defer s1.Close()

	m1 := recipe.NewRWMutex(s1, "my-rw-lock")

	// acquire lock for s1
	log.Println("about to aquire lock")
	if err := m1.RLock(); err != nil {
		log.Fatal(err)
	}
	log.Println("now reading for 30s")

	time.Sleep(30 * time.Second)
	log.Println("30s are over, about to release lock")

	if err := m1.Unlock(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("released rlock - exiting")
}
