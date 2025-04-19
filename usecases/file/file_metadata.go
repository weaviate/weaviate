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

package file

import (
	"os"
	"syscall"

	"github.com/weaviate/weaviate/usecases/integrity"
)

type FileMetadata struct {
	Name  string `json:"name"`
	Size  int64  `json:"size"`
	CRC32 uint32 `json:"crc32"`
}

func GetFileMetadata(filePath string) (FileMetadata, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return FileMetadata{}, err
	}

	if fi.IsDir() {
		return FileMetadata{}, syscall.EISDIR
	}

	size, crc32, err := integrity.CRC32(filePath)
	if err != nil {
		return FileMetadata{}, err
	}

	return FileMetadata{
		Name:  fi.Name(),
		Size:  size,
		CRC32: crc32,
	}, nil
}
