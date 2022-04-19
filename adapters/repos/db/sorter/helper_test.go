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

	"github.com/semi-technologies/weaviate/entities/schema"
)

func Test_classHelper_isDataTypeSupported(t *testing.T) {
	tests := []struct {
		name     string
		dataType []string
		want     bool
	}{
		{
			name:     "empty",
			dataType: []string{""},
			want:     false,
		},
		{
			name:     "cref",
			dataType: []string{string(schema.DataTypeCRef)},
			want:     false,
		},
		{
			name:     "inProperty",
			dataType: []string{"Property"},
			want:     false,
		},
		{
			name:     "inProperties",
			dataType: []string{"Property1", "Property2"},
			want:     false,
		},
		{
			name:     "string",
			dataType: []string{string(schema.DataTypeString)},
			want:     true,
		},
		{
			name:     "text",
			dataType: []string{string(schema.DataTypeText)},
			want:     true,
		},
		{
			name:     "string[]",
			dataType: []string{string(schema.DataTypeStringArray)},
			want:     true,
		},
		{
			name:     "text[]",
			dataType: []string{string(schema.DataTypeTextArray)},
			want:     true,
		},
		{
			name:     "int",
			dataType: []string{string(schema.DataTypeInt)},
			want:     true,
		},
		{
			name:     "number",
			dataType: []string{string(schema.DataTypeNumber)},
			want:     true,
		},
		{
			name:     "int[]",
			dataType: []string{string(schema.DataTypeIntArray)},
			want:     true,
		},
		{
			name:     "number[]",
			dataType: []string{string(schema.DataTypeNumberArray)},
			want:     true,
		},
		{
			name:     "boolean",
			dataType: []string{string(schema.DataTypeBoolean)},
			want:     true,
		},
		{
			name:     "boolean[]",
			dataType: []string{string(schema.DataTypeBooleanArray)},
			want:     true,
		},
		{
			name:     "date",
			dataType: []string{string(schema.DataTypeDate)},
			want:     true,
		},
		{
			name:     "date[]",
			dataType: []string{string(schema.DataTypeDateArray)},
			want:     true,
		},
		{
			name:     "phoneNumber",
			dataType: []string{string(schema.DataTypePhoneNumber)},
			want:     true,
		},
		{
			name:     "geoCoordinates",
			dataType: []string{string(schema.DataTypeGeoCoordinates)},
			want:     true,
		},
		{
			name:     "blob",
			dataType: []string{string(schema.DataTypeBlob)},
			want:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newClassHelper(getMyFavoriteClassSchemaForTests())
			if got := s.isDataTypeSupported(tt.dataType); got != tt.want {
				t.Errorf("classHelper.isDataTypeSupported() %v, want %v", got, tt.want)
			}
		})
	}
}
