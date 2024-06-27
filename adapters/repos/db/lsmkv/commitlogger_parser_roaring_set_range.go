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

package lsmkv

import (
	"encoding/binary"
	"fmt"

	"github.com/weaviate/sroar"
)

func (p *commitloggerParser) doRoaringSetRange() error {
	prs := &commitlogParserRoaringSet{
		parser: p,
		consume: func(key []byte, additions, deletions *sroar.Bitmap) error {
			if len(key) != 8 {
				return fmt.Errorf("commitloggerParser: invalid value length %d, should be 8 bytes", len(key))
			}

			return p.memtable.roaringSetRangeAddRemove(binary.BigEndian.Uint64(key),
				additions.ToArray(), deletions.ToArray())
		},
	}

	return prs.parse()
}
