//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import "github.com/semi-technologies/weaviate/entities/storobj"

type sortObjsByDist struct {
	objects   []*storobj.Object
	distances []float32
}

func (sbd sortObjsByDist) Len() int {
	return len(sbd.objects)
}

func (sbd sortObjsByDist) Less(i, j int) bool {
	return sbd.distances[i] < sbd.distances[j]
}

func (sbd sortObjsByDist) Swap(i, j int) {
	sbd.distances[i], sbd.distances[j] = sbd.distances[j], sbd.distances[i]
	sbd.objects[i], sbd.objects[j] = sbd.objects[j], sbd.objects[i]
}
