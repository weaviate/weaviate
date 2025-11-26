//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

import (
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

type Centroid struct {
	Uncompressed []float32
	Compressed   []byte
	Deleted      bool
}

func (c *Centroid) Distance(distancer *Distancer, v Vector) (float32, error) {
	return v.DistanceWithRaw(distancer, c.Compressed)
}

type CentroidIndex interface {
	Insert(id uint64, centroid *Centroid) error
	Get(id uint64) *Centroid
	MarkAsDeleted(id uint64) error
	Exists(id uint64) bool
	Search(query []float32, k int) (*ResultSet, error)
	SetQuantizer(quantizer *compressionhelpers.RotationalQuantizer)
	GetMaxID() uint64
}
