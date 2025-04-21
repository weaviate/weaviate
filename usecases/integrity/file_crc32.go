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

package integrity

import (
	"hash/crc32"
	"io"
	"os"
)

func CRC32(path string) (n int64, checksum uint32, err error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	h := crc32.NewIEEE()

	n, err = io.Copy(h, file)
	if err != nil {
		return 0, 0, err
	}

	return n, h.Sum32(), nil
}
