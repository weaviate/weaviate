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

package roaringsetrange

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
)

type InnerReader interface {
	Read(ctx context.Context, value uint64, operator filters.Operator) (roaringset.BitmapLayer, error)
}

type CombinedReader struct {
	logger         logrus.FieldLogger
	readers        []InnerReader
	releaseReaders func()
	concurrency    int
}

func NewCombinedReader(readers []InnerReader, releaseReaders func(), concurrency int,
	logger logrus.FieldLogger,
) *CombinedReader {
	return &CombinedReader{
		logger:         logger,
		readers:        readers,
		releaseReaders: releaseReaders,
		concurrency:    concurrency,
	}
}

func (r *CombinedReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (*sroar.Bitmap, error) {
	count := len(r.readers)

	switch count {
	case 0:
		return sroar.NewBitmap(), nil
	case 1:
		layer, err := r.readers[0].Read(ctx, value, operator)
		if err != nil {
			return nil, err
		}
		return layer.Additions, nil
	}

	// all readers but last one. it will be processed by current goroutine
	responseChans := make([]chan *readerResponse, count-1)
	for i := range responseChans {
		responseChans[i] = make(chan *readerResponse, 1)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errors.GoWrapper(func() {
		eg, gctx := errors.NewErrorGroupWithContextWrapper(r.logger, ctx)
		eg.SetLimit(r.concurrency)

		for i := 1; i < count; i++ {
			i := i
			eg.Go(func() error {
				s := time.Now()
				fmt.Printf(" ==> [%d] started reading\n", i)

				layer, err := r.readers[i].Read(gctx, value, operator)
				responseChans[i-1] <- &readerResponse{layer, err}

				fmt.Printf(" ==> [%d] finished reading, took [%s]\n", i, time.Since(s))

				return err
			})
		}
	}, r.logger)

	s := time.Now()
	fmt.Printf(" ==> [%d] started reading\n", 0)

	layer, err := r.readers[0].Read(ctx, value, operator)
	if err != nil {
		return nil, err
	}

	fmt.Printf(" ==> [%d] finished reading, took [%s]\n", 0, time.Since(s))
	var d_merge, d_merge_total time.Duration

	for i := 1; i < count; i++ {
		response := <-responseChans[i-1]
		if response.err != nil {
			return nil, response.err
		}

		s := time.Now()
		fmt.Printf(" ==> [%d/%d] started merging\n", i-1, i)

		layer.Additions.AndNot(response.layer.Deletions)
		layer.Additions.Or(response.layer.Additions)

		d_merge = time.Since(s)
		d_merge_total += d_merge
		fmt.Printf(" ==> [%d/%d] finished merging, took [%s]\n", i-1, i, d_merge)
	}

	fmt.Printf(" ==> MERGING segments took [%s]\n", d_merge_total)

	return roaringset.Condense(layer.Additions), nil
}

func (r *CombinedReader) Close() {
	r.releaseReaders()
}

type readerResponse struct {
	layer roaringset.BitmapLayer
	err   error
}
