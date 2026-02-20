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

package lsmkv

import (
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/lsmkv"
)

// SetPostings returns all Set entries for a given key.
//
// SetPostings is specific to the Set Strategy with HFresh postings
func (b *Bucket) SetPostings(key []byte) ([][]byte, error) {
	view := b.GetConsistentView()
	defer view.ReleaseView()

	return b.setPostingsFromConsistentView(view, key)
}

func (b *Bucket) setPostingsFromConsistentView(view BucketConsistentView, key []byte) ([][]byte, error) {
	var out [][]byte

	v, err := b.disk.getCollectionBytes(key, view.Disk)
	if err != nil && !errors.Is(err, lsmkv.NotFound) {
		return nil, err
	}
	out = v

	if view.Flushing != nil {
		v, err = view.Flushing.getCollectionBytes(key)
		if err != nil && !errors.Is(err, lsmkv.NotFound) {
			return nil, err
		}
		out = append(out, v...)

	}

	v, err = view.Active.getCollectionBytes(key)
	if err != nil && !errors.Is(err, lsmkv.NotFound) {
		return nil, err
	}
	if len(v) > 0 {
		// skip the expensive append operation if there was no memtable
		out = append(out, v...)
	}

	return out, nil
}
