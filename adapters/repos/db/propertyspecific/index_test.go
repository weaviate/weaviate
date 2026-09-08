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
			indices := newIndices(t, test.geoProps, test.otherProp)

			err := indices.ShutdownGeoIndices(testContext(t, test.cancelled))

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

func TestDropAll(t *testing.T) {
	tests := []struct {
		name      string
		geoProps  []string
		otherProp bool
		// a cancelled context makes every geo index fail to drop
		cancelled     bool
		repeat        int
		wantErrs      []string
		wantRemaining []string
	}{
		{
			name: "no properties",
		},
		{
			name:     "single geo property is dropped",
			geoProps: []string{"location"},
		},
		{
			name:     "every geo property is dropped",
			geoProps: []string{"location", "home", "work"},
		},
		{
			name:          "unsupported property type is reported and stays registered",
			otherProp:     true,
			wantErrs:      []string{"no implementation to delete property name"},
			wantRemaining: []string{"name"},
		},
		{
			// map iteration order is random, so a single run can pass even when
			// the loop aborts at the unsupported property
			name:          "unsupported property type does not strand the geo indices",
			geoProps:      []string{"location", "home", "work"},
			otherProp:     true,
			repeat:        20,
			wantErrs:      []string{"no implementation to delete property name"},
			wantRemaining: []string{"name"},
		},
		{
			name:          "single geo property fails and stays registered",
			geoProps:      []string{"location"},
			cancelled:     true,
			wantErrs:      []string{"drop property location"},
			wantRemaining: []string{"location"},
		},
		{
			name:      "every failing geo property is reported, not just the first",
			geoProps:  []string{"location", "home", "work"},
			cancelled: true,
			wantErrs: []string{
				"drop property location",
				"drop property home",
				"drop property work",
			},
			wantRemaining: []string{"location", "home", "work"},
		},
		{
			name:      "failing drops and an unsupported type are all reported",
			geoProps:  []string{"location", "home", "work"},
			otherProp: true,
			cancelled: true,
			wantErrs: []string{
				"drop property location",
				"drop property home",
				"drop property work",
				"no implementation to delete property name",
			},
			wantRemaining: []string{"location", "home", "work", "name"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for i := 0; i < max(test.repeat, 1); i++ {
				indices := newIndices(t, test.geoProps, test.otherProp)

				err := indices.DropAll(testContext(t, test.cancelled), false)

				if len(test.wantErrs) == 0 {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					for _, want := range test.wantErrs {
						require.ErrorContains(t, err, want)
					}
				}

				remaining := make([]string, 0, len(indices))
				for propName := range indices {
					remaining = append(remaining, propName)
				}
				require.ElementsMatch(t, test.wantRemaining, remaining)
			}
		})
	}
}

func newIndices(t *testing.T, geoProps []string, otherProp bool) Indices {
	t.Helper()

	indices := Indices{}
	for _, propName := range geoProps {
		indices[propName] = Index{
			Name:     propName,
			Type:     schema.DataTypeGeoCoordinates,
			GeoIndex: newGeoIndex(t, propName),
		}
	}
	if otherProp {
		indices["name"] = Index{Name: "name", Type: schema.DataTypeText}
	}
	return indices
}

func testContext(t *testing.T, cancelled bool) context.Context {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if cancelled {
		cancel()
	}
	return ctx
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
