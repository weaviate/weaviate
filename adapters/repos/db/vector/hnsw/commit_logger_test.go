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

package hnsw

import (
	_ "fmt"
	"os"
	"testing"
)

type MockDirEntry struct {
	name  string
	isDir bool
}

func (d MockDirEntry) Name() string {
	return d.name
}

func (d MockDirEntry) IsDir() bool {
	return d.isDir
}

func (d MockDirEntry) Type() os.FileMode {
	return os.ModePerm
}

func (d MockDirEntry) Info() (os.FileInfo, error) {
	return nil, nil
}

func TestRemoveTmpScratchOrHiddenFiles(t *testing.T) {
	entries := []os.DirEntry{
		MockDirEntry{name: "1682473161", isDir: false},
		MockDirEntry{name: ".nfs6b46801cd962afbc00000005", isDir: false},
		MockDirEntry{name: ".mystery-folder", isDir: false},
		MockDirEntry{name: "1682473161.condensed", isDir: false},
		MockDirEntry{name: "1682473161.scratch.tmp", isDir: false},
	}

	expected := []os.DirEntry{
		MockDirEntry{name: "1682473161", isDir: false},
		MockDirEntry{name: "1682473161.condensed", isDir: false},
	}

	result := removeTmpScratchOrHiddenFiles(entries)

	if len(result) != len(expected) {
		t.Errorf("Expected %d entries, got %d", len(expected), len(result))
	}

	for i, entry := range result {
		if entry.Name() != expected[i].Name() {
			t.Errorf("Expected entry %d to be %s, got %s", i, expected[i].Name(), entry.Name())
		}
	}
}
