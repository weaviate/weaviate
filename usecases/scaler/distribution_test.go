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

package scaler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDifference(t *testing.T) {
	tests := []struct {
		xs, ys, zs []string
	}{
		{
			zs: []string{},
		},
		{
			xs: []string{"1", "2"},
			ys: []string{},
			zs: []string{"1", "2"},
		},
		{
			xs: []string{"1", "2"},
			ys: []string{"1", "2"},
			zs: []string{},
		},
		{
			xs: []string{"1", "2", "3", "4"},
			ys: []string{"1", "3"},
			zs: []string{"2", "4"},
		},
		{
			xs: []string{"1", "2", "3", "4"},
			ys: []string{"2", "4"},
			zs: []string{"1", "3"},
		},
	}
	for _, c := range tests {
		assert.Equal(t, c.zs, difference(c.xs, c.ys))
	}
}
