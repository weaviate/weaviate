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

package propertyspecific

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/geo"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestShutdownGeoIndices(t *testing.T) {
	tests := []struct {
		name      string
		geoProps  []string
		otherProp bool
		// a cancelled context makes every geo index fail to shut down
		cancelled bool
		wantErrs  []string
	}{
		{
			name: "no properties",
		},
		{
			name:      "only a non-geo property is skipped",
			otherProp: true,
		},
		{
			name:     "all geo properties shut down",
			geoProps: []string{"location"},
		},
		{
			name:      "single geo property fails",
			geoProps:  []string{"location"},
			cancelled: true,
			wantErrs:  []string{"shutdown property location"},
		},
		{
			name:      "every geo property is reported, not just the first",
			geoProps:  []string{"location", "home", "work"},
			otherProp: true,
			cancelled: true,
			wantErrs: []string{
				"shutdown property location",
				"shutdown property home",
				"shutdown property work",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			indices := Indices{}
			for _, propName := range test.geoProps {
				indices[propName] = Index{
					Name:     propName,
					Type:     schema.DataTypeGeoCoordinates,
					GeoIndex: newGeoIndex(t, propName),
				}
			}
			if test.otherProp {
				indices["name"] = Index{Name: "name", Type: schema.DataTypeText}
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if test.cancelled {
				cancel()
			}

			err := indices.ShutdownGeoIndices(ctx)

			if len(test.wantErrs) == 0 {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, want := range test.wantErrs {
				require.ErrorContains(t, err, want)
			}
		})
	}
}

func newGeoIndex(t *testing.T, id string) *geo.Index {
	t.Helper()

	logger, _ := test.NewNullLogger()
	index, err := geo.NewIndex(geo.Config{
		ID:                 id,
		Logger:             logger,
		AllocChecker:       memwatch.NewDummyMonitor(),
		DisablePersistence: true,
		RootPath:           t.TempDir(),
		CoordinatesForID: func(ctx context.Context, id uint64) (*models.GeoCoordinates, error) {
			return nil, nil
		},
	}, cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroup("tombstone", logger, 1))
	require.NoError(t, err)

	return index
}
