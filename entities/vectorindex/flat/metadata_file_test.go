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

package flat

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadataFileName(t *testing.T) {
	tests := []struct {
		name         string
		targetVector string
		want         string
	}{
		{name: "unnamed vector", targetVector: "", want: "meta.db"},
		{name: "named vector", targetVector: "foo", want: "meta_foo.db"},
		{name: "named vector containing an underscore", targetVector: "foo_bar", want: "meta_foo_bar.db"},
		{name: "a target vector that would escape the index root", targetVector: "./../foo", want: "meta_foo.db"},
		{name: "a target vector that is only a separator", targetVector: "/", want: "meta.db"},
		{name: "a target vector that is only a dot", targetVector: ".", want: "meta.db"},
		{name: "a target vector that is only a parent reference", targetVector: "..", want: "meta.db"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, MetadataFileName(test.targetVector))
		})
	}
}

func TestIsMetadataFile(t *testing.T) {
	tests := []struct {
		name string
		file string
		want bool
	}{
		{name: "unnamed vector", file: "meta.db", want: true},
		{name: "named vector", file: "meta_foo.db", want: true},
		{name: "named vector containing an underscore", file: "meta_foo_bar.db", want: true},
		{name: "dynamic index state db", file: "index.db", want: false},
		{name: "prefix without the target vector separator", file: "metadata.db", want: false},
		{name: "lsm segment", file: "segment-0001.db", want: false},
		{name: "no extension", file: "meta", want: false},
		{name: "staged copy still being written", file: "meta.db.tmp", want: false},
		{name: "empty", file: "", want: false},
		{name: "relative path", file: "myclass/tenant1/meta.db", want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, IsMetadataFile(test.file))
		})
	}

	// Checks IsMetadataFile against MetadataFileName's output, so a change to one
	// alone fails here rather than leaving the file out of a backup.
	t.Run("recognizes every name the index writes", func(t *testing.T) {
		for _, targetVector := range []string{"", "foo", "foo_bar", "/", ".", "..", "./../foo"} {
			require.True(t, IsMetadataFile(MetadataFileName(targetVector)),
				"target vector %q", targetVector)
		}
	})
}
