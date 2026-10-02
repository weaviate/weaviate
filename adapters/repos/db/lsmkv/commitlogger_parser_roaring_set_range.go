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
	"encoding/binary"
	"fmt"
)

func (p *commitloggerParser) doRoaringSetRange() error {
	prs := &commitlogParserRoaringSet{
		parser: p,
		consume: func(mt *Memtable, key []byte, additions, deletions []uint64) error {
			if len(key) != 8 {
				return fmt.Errorf("commitloggerParser: invalid value length %d, should be 8 bytes", len(key))
			}

			return mt.roaringSetRangeAddRemove(binary.BigEndian.Uint64(key),
				additions, deletions)
		},
	}

	return prs.parse()
}
