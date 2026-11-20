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

// GeoPointsAdd indexes docID at the coordinates. A doc ID is added once: an
// object update gets a new doc ID and the old one is removed.
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

// GeoPointsWithinRange returns the doc IDs within distance meters of (lat,
// lon).
func (b *Bucket) GeoPointsWithinRange(ctx context.Context, lat, lon, distance float32) (*sroar.Bitmap, error) {
	if err := CheckStrategyGeoPoints(b.strategy); err != nil {
		return nil, err
	}
	circle, err := geopoints.NewCircle(lat, lon, distance)
	if err != nil {
		return nil, fmt.Errorf("geo range: %w", err)
	}
	return b.geoPointsMatching(ctx, func(layer geopoints.InnerReader, out, deleted *sroar.Bitmap) error {
		return layer.WithinRange(ctx, circle, out, deleted)
	})
}

// GeoPointsWithinBox returns the doc IDs between the two latitudes whose
// longitude lies between west and east going east; a west past the east spans
// the antimeridian.
func (b *Bucket) GeoPointsWithinBox(ctx context.Context, south, north, west, east float32) (*sroar.Bitmap, error) {
	if err := CheckStrategyGeoPoints(b.strategy); err != nil {
		return nil, err
	}
	box, err := geopoints.NewBoxQuery(south, north, west, east)
	if err != nil {
		return nil, fmt.Errorf("geo box: %w", err)
	}
	return b.geoPointsMatching(ctx, func(layer geopoints.InnerReader, out, deleted *sroar.Bitmap) error {
		return layer.WithinBox(ctx, box, out, deleted)
	})
}

// geoPointsMatching runs a filter over every layer. Each adds its matches and
// its deletions, and the deletions are subtracted at the end, which is exact
// because a doc ID is added once and removed at most once.
func (b *Bucket) geoPointsMatching(ctx context.Context,
	query func(layer geopoints.InnerReader, out, deleted *sroar.Bitmap) error,
) (*sroar.Bitmap, error) {
	out, deleted := sroar.NewBitmap(), sroar.NewBitmap()
	err := b.geoPointsEachLayer(ctx, func(layer geopoints.InnerReader) error {
		return query(layer, out, deleted)
	})
	if err != nil {
		return nil, err
	}
	if !deleted.IsEmpty() {
		out.AndNot(deleted)
	}
	return out, nil
}

// GeoPointsNearest returns up to limit points ordered by their distance from
// (lat, lon), nearest first. allow restricts the candidates; nil allows all.
// The deletions of every layer are gathered before any point is read, since a
// point is only out of the result once its deletion is known. The memtables
// are read from snapshots, so the deletions and the points agree even while
// updates retire doc IDs and add their replacements.
func (b *Bucket) GeoPointsNearest(ctx context.Context, lat, lon float32, limit int,
	allow func(docID uint64) bool,
) ([]geopoints.Neighbor, error) {
	if err := CheckStrategyGeoPoints(b.strategy); err != nil {
		return nil, err
	}

	view := b.GetConsistentView()
	defer view.ReleaseView()

	// the memtables first: the search bounds the segments' trees with what it
	// found in them
	layers := make([]geopoints.NearestReader, 0, len(view.Disk)+2)
	layers = append(layers, view.Active.newGeoPointsSnapshot())
	if view.Flushing != nil {
		// writers that took it before the switch may still be adding to it
		layers = append(layers, view.Flushing.newGeoPointsSnapshot())
	}
	for _, seg := range view.Disk {
		reader, err := seg.newGeoPointsReader()
		if err != nil {
			return nil, fmt.Errorf("segment %q: %w", seg.getPath(), err)
		}
		layers = append(layers, reader)
	}

	deleted := sroar.NewBitmap()
	for _, layer := range layers {
		if err := layer.CollectDeletions(deleted); err != nil {
			return nil, fmt.Errorf("geo nearest: deletions: %w", err)
		}
	}

	keep := allow
	if !deleted.IsEmpty() {
		keep = func(docID uint64) bool {
			return !deleted.Contains(docID) && (allow == nil || allow(docID))
		}
	}
	nearest, err := geopoints.NewNearest(lat, lon, limit, keep)
	if err != nil {
		return nil, fmt.Errorf("geo nearest: %w", err)
	}
	for _, layer := range layers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := layer.Nearest(ctx, nearest); err != nil {
			return nil, fmt.Errorf("geo nearest: %w", err)
		}
	}
	return nearest.Results(), nil
}

// geoPointsEachLayer visits every layer of the bucket under one view.
func (b *Bucket) geoPointsEachLayer(ctx context.Context, fn func(layer geopoints.InnerReader) error) error {
	view := b.GetConsistentView()
	defer view.ReleaseView()

	for _, seg := range view.Disk {
		if err := ctx.Err(); err != nil {
			return err
		}
		reader, err := seg.newGeoPointsReader()
		if err != nil {
			return fmt.Errorf("segment %q: %w", seg.getPath(), err)
		}
		if err := fn(reader); err != nil {
			return fmt.Errorf("segment %q: %w", seg.getPath(), err)
		}
	}
	if view.Flushing != nil {
		if err := fn(view.Flushing.newGeoPointsReader()); err != nil {
			return fmt.Errorf("flushing memtable: %w", err)
		}
	}
	if err := fn(view.Active.newGeoPointsReader()); err != nil {
		return fmt.Errorf("active memtable: %w", err)
	}
	return nil
}
