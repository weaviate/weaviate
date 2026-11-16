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

package inverted

import (
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// TokenizationBucketPinner returns a property's tokenization and pins its bucket as one snapshot,
// until release runs exactly once (bucket may be nil): a retokenization's flip moves both.
type TokenizationBucketPinner func(propName, bucketName, schemaTokenization string) (tokenization string, bucket *lsmkv.Bucket, release func())

// queryPins holds one query's snapshots, so every clause reads the same pair and release frees them all.
type queryPins struct {
	mu      sync.Mutex
	pinned  []queryPin
	backing [2]queryPin
}

type queryPin struct {
	prop, bucketName, tokenization string
	bucket                         *lsmkv.Bucket
	release                        func()
}

func (p *queryPins) pin(pinner TokenizationBucketPinner, prop, bucketName, schemaTokenization string,
) (string, *lsmkv.Bucket) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pin := range p.pinned {
		if pin.prop == prop && pin.bucketName == bucketName {
			return pin.tokenization, pin.bucket
		}
	}
	if p.pinned == nil {
		p.pinned = p.backing[:0]
	}
	tokenization, bucket, release := pinner(prop, bucketName, schemaTokenization)
	p.pinned = append(p.pinned, queryPin{
		prop: prop, bucketName: bucketName, tokenization: tokenization, bucket: bucket, release: release,
	})
	return tokenization, bucket
}

func (p *queryPins) bucketFor(prop string) *lsmkv.Bucket {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pin := range p.pinned {
		if pin.prop == prop {
			return pin.bucket
		}
	}
	return nil
}

func (p *queryPins) release() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pin := range p.pinned {
		if pin.release != nil {
			pin.release()
		}
	}
	p.pinned = p.pinned[:0]
}
