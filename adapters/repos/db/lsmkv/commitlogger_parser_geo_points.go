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

// doGeoPoints replays a geopoints log, whose records are roaringset nodes as
// memtable_geo_points.go writes them.
func (p *commitloggerParser) doGeoPoints() error {
	prs := &commitlogParserRoaringSet{
		parser: p,
		consume: func(mt *Memtable, key []byte, additions, deletions []uint64) error {
			if len(key) != 8 {
				return fmt.Errorf("commitloggerParser: invalid key length %d, should be 8 bytes", len(key))
			}
			docID := binary.BigEndian.Uint64(key)

			switch {
			case len(additions) == 1 && len(deletions) == 0:
				lat, lon := unpackCoordinates(additions[0])
				return mt.geoPointsAdd(docID, lat, lon)
			case len(additions) == 0 && len(deletions) == 1:
				return mt.geoPointsRemove(docID)
			default:
				return fmt.Errorf("commitloggerParser: geopoints record for doc %d has %d additions and %d deletions",
					docID, len(additions), len(deletions))
			}
		},
	}

	return prs.parse()
}
