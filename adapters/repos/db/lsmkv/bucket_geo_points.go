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
	"context"
	"fmt"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/geopoints"
)

// GeoPointsAdd indexes docID at the coordinates. A doc ID is added once in its
// lifetime: an object update gets a new doc ID, and the old one is removed.
func (b *Bucket) GeoPointsAdd(docID uint64, lat, lon float32) error {
	if err := CheckStrategyGeoPoints(b.strategy); err != nil {
		return err
	}

	active, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer active.decWriterCount()

	return active.geoPointsAdd(docID, lat, lon)
}

// GeoPointsRemove drops docID's point, in whichever layer it sits.
func (b *Bucket) GeoPointsRemove(docID uint64) error {
	if err := CheckStrategyGeoPoints(b.strategy); err != nil {
		return err
	}

	active, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer active.decWriterCount()

	return active.geoPointsRemove(docID)
}

// GeoPointsWithinRange returns the doc IDs whose point is at most distance
// meters from (lat, lon). Every layer contributes its matches and its
// deletions; the deletions are subtracted at the end, which is exact because a
// doc ID is added once and removed at most once afterwards.
func (b *Bucket) GeoPointsWithinRange(ctx context.Context, lat, lon, distance float32) (*sroar.Bitmap, error) {
	if err := CheckStrategyGeoPoints(b.strategy); err != nil {
		return nil, err
	}
	circle, err := geopoints.NewCircle(lat, lon, distance)
	if err != nil {
		return nil, fmt.Errorf("geo range: %w", err)
	}

	view := b.GetConsistentView()
	defer view.ReleaseView()

	out, deleted := sroar.NewBitmap(), sroar.NewBitmap()
	for _, seg := range view.Disk {
		reader, err := seg.newGeoPointsReader()
		if err != nil {
			return nil, fmt.Errorf("segment %q: %w", seg.getPath(), err)
		}
		if err := reader.WithinRange(ctx, circle, out, deleted); err != nil {
			return nil, fmt.Errorf("segment %q: %w", seg.getPath(), err)
		}
	}
	if view.Flushing != nil {
		if err := view.Flushing.newGeoPointsReader().WithinRange(ctx, circle, out, deleted); err != nil {
			return nil, fmt.Errorf("flushing memtable: %w", err)
		}
	}
	if err := view.Active.newGeoPointsReader().WithinRange(ctx, circle, out, deleted); err != nil {
		return nil, fmt.Errorf("active memtable: %w", err)
	}

	if !deleted.IsEmpty() {
		out.AndNot(deleted)
	}
	return out, nil
}
