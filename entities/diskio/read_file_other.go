//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build !unix

package diskio

// ReadFileExact reads the regular file at path into a slice sized by its stat,
// so it suits only files nobody writes during the read.
func ReadFileExact(path string) ([]byte, error) {
	return readFileExactPortable(path)
}
