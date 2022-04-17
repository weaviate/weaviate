//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package sorter

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func Test_sortBy_compare(t *testing.T) {
	type want struct {
		asc, desc bool
	}
	type args struct {
		i        interface{}
		j        interface{}
		dataType schema.DataType
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "two strings",
			args: args{i: "a", j: "b", dataType: schema.DataTypeString},
			want: want{asc: true, desc: false},
		},
		{
			name: "two string arrays",
			args: args{i: []string{"a", "b"}, j: []string{"b", "c"}, dataType: schema.DataTypeStringArray},
			want: want{asc: true, desc: false},
		},
		{
			name: "two numbers",
			args: args{i: float64(1), j: float64(2), dataType: schema.DataTypeNumber},
			want: want{asc: true, desc: false},
		},
		{
			name: "two number arrays",
			args: args{i: []float64{1, 1, 1}, j: []float64{2}, dataType: schema.DataTypeNumberArray},
			want: want{asc: false, desc: true},
		},
		{
			name: "two bools",
			args: args{i: true, j: true, dataType: schema.DataTypeBoolean},
			want: want{asc: false, desc: true},
		},
		{
			name: "two dates",
			args: args{i: "1980-01-01T00:00:00+02:00", j: "1980-01-02T00:00:00+02:00", dataType: schema.DataTypeDate},
			want: want{asc: true, desc: false},
		},
		{
			name: "two date arrays",
			args: args{
				i:        []string{"1980-01-01T00:00:00+02:00"},
				j:        []string{"1980-01-01T00:00:00+02:00", "1980-01-01T00:00:00+02:00"},
				dataType: schema.DataTypeDateArray,
			},
			want: want{asc: true, desc: false},
		},
		{
			name: "two phone numbers",
			args: args{
				i: &models.PhoneNumber{
					CountryCode:            49,
					DefaultCountry:         "DE",
					Input:                  "0171 1000000",
					Valid:                  true,
					InternationalFormatted: "+49 171 1000000",
					National:               1000000,
					NationalFormatted:      "0171 1000000",
				},
				j: &models.PhoneNumber{
					CountryCode:            49,
					DefaultCountry:         "DE",
					Input:                  "0171 1000000",
					Valid:                  true,
					InternationalFormatted: "+49 171 1000000",
					National:               1000001,
					NationalFormatted:      "0171 1000000",
				},
				dataType: schema.DataTypePhoneNumber,
			},
			want: want{asc: true, desc: false},
		},
		{
			name: "two geo coordinates",
			args: args{
				i: &models.GeoCoordinates{
					Longitude: pointerFloat32(1),
					Latitude:  pointerFloat32(0),
				},
				j: &models.GeoCoordinates{
					Longitude: pointerFloat32(2),
					Latitude:  pointerFloat32(0),
				},
				dataType: schema.DataTypeGeoCoordinates,
			},
			want: want{asc: true, desc: false},
		},
		{
			name: "two nils",
			args: args{dataType: schema.DataTypeString},
			want: want{asc: false, desc: false},
		},
		{
			name: "first one nil",
			args: args{i: nil, j: "a", dataType: schema.DataTypeString},
			want: want{asc: true, desc: false},
		},
		{
			name: "second one nil",
			args: args{i: "a", j: nil, dataType: schema.DataTypeString},
			want: want{asc: false, desc: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSortBy(newComparator("asc"))
			if got := s.compare(tt.args.i, tt.args.j, tt.args.dataType); got != tt.want.asc {
				t.Errorf("sortBy.compare(asc) = %v, want %v", got, tt.want.asc)
			}
			s = newSortBy(newComparator("desc"))
			if got := s.compare(tt.args.i, tt.args.j, tt.args.dataType); got != tt.want.desc {
				t.Errorf("sortBy.compare(desc) = %v, want %v", got, tt.want.desc)
			}
		})
	}
}
