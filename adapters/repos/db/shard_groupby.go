package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (s *Shard) groupResults(ctx context.Context, ids []uint64,
	dists []float32, params *dto.GroupByParams,
) ([]*storobj.Object, []float32, error) {
	objsBucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	err := newGrouper(ids, dists, params, objsBucket).Do(ctx)

	return nil, nil, err
}

type grouper struct {
	ids       []uint64
	dists     []float32
	params    *dto.GroupByParams
	objBucket *lsmkv.Bucket
}

func newGrouper(ids []uint64, dists []float32,
	params *dto.GroupByParams, objBucket *lsmkv.Bucket,
) *grouper {
	return &grouper{
		ids:       ids,
		dists:     dists,
		params:    params,
		objBucket: objBucket,
	}
}

func (g *grouper) Do(ctx context.Context) error {
	before := time.Now()
	rawObjectData := make([][]byte, len(g.ids))
	docIDBytes := make([]byte, 8)

	groups := map[float64][]uint64{}

	for i, docID := range g.ids {
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		objData, err := g.objBucket.GetBySecondary(0, docIDBytes)
		if err != nil {
			return errors.Wrapf(err, "lsm sorter - could not get obj by doc id %d", docID)
		}
		if objData == nil {
			continue
		}
		rawObjectData[i] = objData
		value, ok, _ := storobj.ParseAndExtractProperty(objData, g.params.Prop)
		if len(value) == 0 || !ok {
			// TODO: we need to explicitly handle null values
			continue
		}

		parsed := mustExtractNumber(value)
		// TODO: other lengths
		val := parsed[0]

		current, ok := groups[val]
		if !ok && len(groups) >= g.params.GroupsLimit {
			continue
		}

		if len(current) >= g.params.ObjectsPerGroup {
			continue
		}

		groups[val] = append(current, docID)
	}
	fmt.Printf("retrieve, partial parse, group objects took %s\n", time.Since(before))
	spew.Dump(groups)

	return fmt.Errorf("not implemented yet")
}

func mustExtractNumber(value []string) []float64 {
	numbers := make([]float64, len(value))
	for i := range value {
		number, err := strconv.ParseFloat(value[i], 64)
		if err != nil {
			panic("sorter: not a number")
		}
		numbers[i] = number
	}
	return numbers
}
