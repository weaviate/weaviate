//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lib

import (
	"fmt"
	"math"
	"strconv"
)

type Scores struct {
	NDCG       float64
	hitsAt1    int
	hitsAt5    int
	numQueries int
}

func (n *Scores) AddResult(matchingIds []int, resultIds []interface{}, propNameWithId string) ([]int, error) {
	IDCG := 0.
	for j := 0; j < len(matchingIds); j++ {
		IDCG += 1. / math.Log(float64(j+2.))
	}

	matched := []int{}

	DCG := 0.
	for rank, resultId := range resultIds {
		id, err := strconv.Atoi(resultId.(map[string]interface{})[propNameWithId].(string))
		if err != nil {
			return nil, err
		}
		for _, matchigId := range matchingIds {
			m := false
			if id == matchigId {
				if rank == 0 {
					n.hitsAt1 += 1
					m = true
				}
				if rank < 5 {
					n.hitsAt5 += 1
					m = true
				}
				if m {
					matched = append(matched, id)
				}
				DCG += 1 / math.Log(float64(rank+2))

			}
		}
	}
	n.NDCG += DCG / IDCG
	n.numQueries += 1
	return matched, nil
}

func (n *Scores) CurrentNDCG() float64 {
	return n.NDCG / float64(n.numQueries)
}

func (n *Scores) PrettyPrint() {
	fmt.Printf("nDCG score: %.04f, hits at 1: %d, hits at 5: %d\n", n.NDCG/float64(n.numQueries), n.hitsAt1, n.hitsAt5)
}
