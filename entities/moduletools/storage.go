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

package moduletools

type StorageProvider interface {
	Storage(name string) (Storage, error)
	DataPath() string
}

type ScanFn func(k, v []byte) (bool, error)

type Storage interface {
	Get(key []byte) ([]byte, error)
	Scan(scan ScanFn) error
	Put(key, value []byte) error
}
