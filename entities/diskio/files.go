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

package diskio

import (
	"os"
	"path/filepath"
)

func FileExists(file string) (bool, error) {
	_, err := os.Stat(file)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func Fsync(path string) error {
	tmpFile := filepath.Join(path, ".sync.tmp")
	if err := os.WriteFile(tmpFile, []byte{}, 0o66); err != nil {
		return err
	}
	defer os.Remove(tmpFile)

	file, err := os.OpenFile(tmpFile, os.O_RDWR, 0o666)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := file.Sync(); err != nil {
		return err
	}
	return nil
}
