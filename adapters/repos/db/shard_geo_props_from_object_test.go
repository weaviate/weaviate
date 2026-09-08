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

package db

import (
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

const geoFromObjectClass = "GeoPropScan"

func geoScanShard() *Shard {
	return &Shard{index: &Index{Config: IndexConfig{ClassName: schema.ClassName(geoFromObjectClass)}}}
}

func geoCoordinates(lat, lon float32) *models.GeoCoordinates {
	return &models.GeoCoordinates{Latitude: &lat, Longitude: &lon}
}

// marshalGeoObject produces object bytes exactly as the write path does.
// skipClassName mirrors LSM_SKIP_WRITE_CLASS_NAME, which leaves the class name
// off disk and is why the decoder takes it from the schema instead.
func marshalGeoObject(t *testing.T, props map[string]interface{}, skipClassName bool) []byte {
	t.Helper()

	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      geoFromObjectClass,
			Properties: props,
		},
		Vector: []float32{7, 8, 9},
	}
	data, err := obj.MarshalBinaryDisk(skipClassName)
	require.NoError(t, err)
	return data
}

func TestMakeCoordinatesFromObject(t *testing.T) {
	munich := geoCoordinates(48.13743, 11.57549)
	stuttgart := geoCoordinates(48.78232, 9.17702)
	fullProps := map[string]interface{}{
		"name":     "munich office",
		"sizes":    []float64{1.5, 2.5},
		"location": munich,
		"home":     stuttgart,
	}

	tests := []struct {
		name          string
		propName      string
		props         map[string]interface{}
		skipClassName bool
		objectBytes   func(t *testing.T) []byte
		want          *models.GeoCoordinates
		wantErr       string
	}{
		{
			name:     "geo prop next to other properties",
			propName: "location",
			props:    fullProps,
			want:     munich,
		},
		{
			// each geo prop has its own index, so a scan must not pick up its sibling
			name:     "second geo prop of the same object",
			propName: "home",
			props:    fullProps,
			want:     stuttgart,
		},
		{
			name:          "class name omitted on disk",
			propName:      "location",
			props:         fullProps,
			skipClassName: true,
			want:          munich,
		},
		{
			// a nil coordinate means "skip", not "this doc is gone" — unlike the
			// by-id reader, the scan has no doc to tombstone
			name:     "object without the requested prop",
			propName: "location",
			props:    map[string]interface{}{"name": "no coordinates here"},
		},
		{
			name:     "object with an empty property map",
			propName: "location",
			props:    map[string]interface{}{},
		},
		{
			name:     "object without any properties",
			propName: "location",
			props:    nil,
		},
		{
			name:        "payload of an unsupported marshaller version",
			propName:    "location",
			objectBytes: func(t *testing.T) []byte { return []byte{2, 0, 0, 0} },
			wantErr:     "unsupported binary marshaller version",
		},
		{
			name:     "prop that does not hold coordinates",
			propName: "location",
			props:    map[string]interface{}{"location": "48.1"},
			wantErr:  "expected property to be of type",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			objectBytes := test.objectBytes
			if objectBytes == nil {
				objectBytes = func(t *testing.T) []byte {
					return marshalGeoObject(t, test.props, test.skipClassName)
				}
			}

			coordinates, err := geoScanShard().makeCoordinatesFromObject(test.propName)(objectBytes(t))

			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				require.Nil(t, coordinates)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.want, coordinates)
		})
	}
}

// The scan runs one decoder across every cursor, so all of them share a single
// *storobj.PropertyExtraction.
func TestMakeCoordinatesFromObjectConcurrentDecodes(t *testing.T) {
	munich := geoCoordinates(48.13743, 11.57549)
	objectBytes := marshalGeoObject(t, map[string]interface{}{"location": munich}, false)
	fromObject := geoScanShard().makeCoordinatesFromObject("location")

	const decodes = 8
	coordinates := make([]*models.GeoCoordinates, decodes)
	errs := make([]error, decodes)

	var wg sync.WaitGroup
	for i := range decodes {
		wg.Go(func() {
			coordinates[i], errs[i] = fromObject(objectBytes)
		})
	}
	wg.Wait()

	for i := range decodes {
		require.NoError(t, errs[i])
		require.Equal(t, munich, coordinates[i])
	}
}
