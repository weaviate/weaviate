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

package docid

import (
	"context"
	"encoding/binary"
	"math"
	"runtime"
	"sync"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

func scanConcurrency() int { return 2 * runtime.GOMAXPROCS(0) }

// A worker carries its largest object between reads, so this bounds what one
// outsized object pins. A normal object with its vectors fits under it.
const maxRetainedBufferBytes = 1 << 20 // 1MB

// ObjectScanFn is called once per object with the context the scan runs under,
// which is cancelled whenever the caller's is. If an error is returned, the
// scanning will stop.
type ObjectScanFn func(ctx context.Context, prop *models.PropertySchema, docID uint64) error

// ScanObjectsLSM calls the provided scanFn on each object for the
// specified pointer. If a pointer does not resolve to an object-id, the item
// will be skipped. The number of times scanFn is called can therefore be
// smaller than the input length of pointers.
func ScanObjectsLSM(ctx context.Context, store *lsmkv.Store, pointers []uint64, scan ObjectScanFn, properties []string, logger logrus.FieldLogger) error {
	return newObjectScannerLSM(store, pointers, scan, properties, logger).Do(ctx)
}

type objectScannerLSM struct {
	store         *lsmkv.Store
	pointers      []uint64
	scanFn        ObjectScanFn
	objectsBucket *lsmkv.Bucket
	properties    []string
	logger        logrus.FieldLogger
}

func newObjectScannerLSM(store *lsmkv.Store, pointers []uint64,
	scan ObjectScanFn, properties []string, logger logrus.FieldLogger,
) *objectScannerLSM {
	return &objectScannerLSM{
		store:      store,
		pointers:   pointers,
		scanFn:     scan,
		properties: properties,
		logger:     logger,
	}
}

func (os *objectScannerLSM) Do(ctx context.Context) error {
	if err := os.init(); err != nil {
		return errors.Wrap(err, "init object scanner")
	}

	if err := os.scan(ctx); err != nil {
		return errors.Wrap(err, "scan")
	}

	return nil
}

func (os *objectScannerLSM) init() error {
	bucket := os.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return errors.Errorf("objects bucket not found")
	}
	os.objectsBucket = bucket

	return nil
}

func (os *objectScannerLSM) scan(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Preallocate property paths needed for json unmarshalling
	propertyPaths := make([][]string, len(os.properties))
	for i := range os.properties {
		propertyPaths[i] = []string{os.properties[i]}
	}

	lookup, release := os.objectsBucket.SecondaryViewLookup()
	defer release()

	lock := sync.Mutex{}
	eg, groupCtx := enterrors.NewErrorGroupWithContextWrapper(os.logger, ctx)
	concurrency := scanConcurrency()
	stride := int(math.Ceil(max(float64(len(os.pointers))/float64(concurrency), 1)))
	for i := 0; i < concurrency; i++ {
		start := i * stride
		end := min(start+stride, len(os.pointers))
		if start >= len(os.pointers) {
			break
		}
		f := func() error {
			// each object is scanned one after the other, so we can reuse the same memory allocations for all objects
			docIDBytes := make([]byte, 8)

			// Grown to the largest object this worker reads, up to maxRetainedBufferBytes.
			// Safe to reuse: UnmarshalPropertiesFromObject retains no slice into it.
			var objBuf []byte

			// The typed properties are needed for extraction from json
			var properties models.PropertySchema

			// checked per doc ID rather than every nth: a worker's range is only
			// len(pointers)/concurrency long, so an interval skips short ranges
			// outright. BenchmarkScanObjectsLSM sweeps what the check costs.
			for _, id := range os.pointers[start:end] {
				if err := groupCtx.Err(); err != nil {
					return err
				}
				binary.LittleEndian.PutUint64(docIDBytes, id)
				res, newBuf, err := lookup(groupCtx, 0, docIDBytes, objBuf)
				if err != nil {
					return err
				}
				objBuf = newBuf

				if res == nil {
					continue
				}

				propertiesTyped := map[string]interface{}{}
				if len(os.properties) > 0 {
					err = storobj.UnmarshalPropertiesFromObject(res, propertiesTyped, propertyPaths)
					if err != nil {
						return errors.Wrapf(err, "unmarshal data object")
					}
					properties = propertiesTyped
				}

				// majority of time is spend reading the objects => do the analyses sequentially to not cause races
				// when analysing the results
				if err := func() error {
					lock.Lock()
					defer lock.Unlock()
					if err := os.scanFn(groupCtx, &properties, id); err != nil {
						return errors.Wrapf(err, "scan object %d", id)
					}
					return nil
				}(); err != nil {
					return err
				}

				if cap(objBuf) > maxRetainedBufferBytes {
					objBuf = nil
				}
			}
			return nil
		}

		eg.Go(f)
	}

	return eg.Wait()
}
