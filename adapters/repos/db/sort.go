package db

import "github.com/semi-technologies/weaviate/adapters/repos/db/storobj"

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
