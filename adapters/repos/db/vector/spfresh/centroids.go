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

type Centroid struct {
	Uncompressed []float32
	Compressed   []byte
	Deleted      bool
}

func (c *Centroid) Distance(distancer *Distancer, v Vector) (float32, error) {
	return v.DistanceWithRaw(distancer, c.Compressed)
}
