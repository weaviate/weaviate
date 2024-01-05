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

package lsmkv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetDecoder(t *testing.T) {
	type test struct {
		name string
		in   []value
		out  [][]byte
	}

	tests := []test{
		{
			name: "single value",
			in: []value{
				{
					value: []byte("foo"),
				},
			},
			out: [][]byte{
				[]byte("foo"),
			},
		},

		{
			name: "single value with tombstone",
			in: []value{
				{
					value:     []byte("foo"),
					tombstone: true,
				},
			},
			out: [][]byte{},
		},
		{
			name: "single value, then a tombstone added",
			in: []value{
				{
					value: []byte("foo"),
				},
				{
					value:     []byte("foo"),
					tombstone: true,
				},
			},
			out: [][]byte{},
		},
		{
			name: "single value, then a tombstone added, then added again",
			in: []value{
				{
					value: []byte("foo"),
				},
				{
					value:     []byte("foo"),
					tombstone: true,
				},
				{
					value: []byte("foo"),
				},
			},
			out: [][]byte{
				[]byte("foo"),
			},
		},
		{
			name: "one value, repeating",
			in: []value{
				{
					value: []byte("foo"),
				},
				{
					value: []byte("foo"),
				},
			},
			out: [][]byte{
				[]byte("foo"),
			},
		},
		{
			name: "multiple values, some tombstones, ending in everything present",
			in: []value{
				{
					value: []byte("foo"),
				},
				{
					value: []byte("bar"),
				},
				{
					value:     []byte("foo"),
					tombstone: true,
				},
				{
					value:     []byte("foo"),
					tombstone: true,
				},
				{
					value:     []byte("foo"),
					tombstone: true,
				},
				{
					value: []byte("foo"),
				},
				{
					value: []byte("bar"),
				},
				{
					value:     []byte("bar"),
					tombstone: true,
				},
				{
					value: []byte("bar"),
				},
			},
			out: [][]byte{
				[]byte("foo"),
				[]byte("bar"),
			},
		},
		{
			name: "multiple values, some tombstones, ending in everything deleted",
			in: []value{
				{
					value: []byte("foo"),
				},
				{
					value: []byte("bar"),
				},
				{
					value:     []byte("foo"),
					tombstone: true,
				},
				{
					value:     []byte("foo"),
					tombstone: true,
				},
				{
					value:     []byte("foo"),
					tombstone: true,
				},
				{
					value: []byte("foo"),
				},
				{
					value: []byte("bar"),
				},
				{
					value:     []byte("bar"),
					tombstone: true,
				},
				{
					value:     []byte("foo"),
					tombstone: true,
				},
			},
			out: [][]byte{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.out, newSetDecoder().Do(test.in))
		})
	}
}
