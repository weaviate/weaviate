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

package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestValidator_extractAndValidateProperty(t *testing.T) {
	type fields struct {
		schema schema.Schema
		exists exists
		config *config.WeaviateConfig
	}
	type args struct {
		ctx          context.Context
		propertyName string
		pv           interface{}
		className    string
		dataType     *schema.DataType
	}
	validatorFields := fields{
		schema: schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "BlobClass",
						Properties: []*models.Property{
							{
								DataType: []string{"blob"},
								Name:     "blobProperty",
							},
						},
					},
				},
			},
		},
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name:   "Validate blob - empty base64 encoded image string",
			fields: validatorFields,
			args: args{
				ctx:          context.Background(),
				propertyName: "blobProperty",
				pv:           "",
				className:    "BlobClass",
				dataType:     getDataType(schema.DataTypeBlob),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "Validate blob - invalid base64 encoded image string",
			fields: validatorFields,
			args: args{
				ctx:          context.Background(),
				propertyName: "blobProperty",
				pv:           "image",
				className:    "BlobClass",
				dataType:     getDataType(schema.DataTypeBlob),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "Validate blob - valid base64 encoded string but with type definition before image string",
			fields: validatorFields,
			args: args{
				ctx:          context.Background(),
				propertyName: "blobProperty",
				pv:           "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAGAAAAA/CAYAAAAfQM0aAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAyRpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMy1jMDExIDY2LjE0NTY2MSwgMjAxMi8wMi8wNi0xNDo1NjoyNyAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RSZWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZVJlZiMiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENTNiAoTWFjaW50b3NoKSIgeG1wTU06SW5zdGFuY2VJRD0ieG1wLmlpZDpCRjQ5NEM3RDI5QTkxMUUyOTc1NENCMzI4N0QwNDNCOSIgeG1wTU06RG9jdW1lbnRJRD0ieG1wLmRpZDpCRjQ5NEM3RTI5QTkxMUUyOTc1NENCMzI4N0QwNDNCOSI+IDx4bXBNTTpEZXJpdmVkRnJvbSBzdFJlZjppbnN0YW5jZUlEPSJ4bXAuaWlkOkJGNDk0QzdCMjlBOTExRTI5NzU0Q0IzMjg3RDA0M0I5IiBzdFJlZjpkb2N1bWVudElEPSJ4bXAuZGlkOkJGNDk0QzdDMjlBOTExRTI5NzU0Q0IzMjg3RDA0M0I5Ii8+IDwvcmRmOkRlc2NyaXB0aW9uPiA8L3JkZjpSREY+IDwveDp4bXBtZXRhPiA8P3hwYWNrZXQgZW5kPSJyIj8+WeGRxAAAB2hJREFUeNrUXFts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",
				className:    "BlobClass",
				dataType:     getDataType(schema.DataTypeBlob),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "Validate blob - valid base64 encoded string",
			fields: validatorFields,
			args: args{
				ctx:          context.Background(),
				propertyName: "blobProperty",
				pv:           "iVBORw0KGgoAAAANSUhEUgAAAGAAAAA/CAYAAAAfQM0aAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAyRpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMy1jMDExIDY2LjE0NTY2MSwgMjAxMi8wMi8wNi0xNDo1NjoyNyAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RSZWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZVJlZiMiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENTNiAoTWFjaW50b3NoKSIgeG1wTU06SW5zdGFuY2VJRD0ieG1wLmlpZDpCRjQ5NEM3RDI5QTkxMUUyOTc1NENCMzI4N0QwNDNCOSIgeG1wTU06RG9jdW1lbnRJRD0ieG1wLmRpZDpCRjQ5NEM3RTI5QTkxMUUyOTc1NENCMzI4N0QwNDNCOSI+IDx4bXBNTTpEZXJpdmVkRnJvbSBzdFJlZjppbnN0YW5jZUlEPSJ4bXAuaWlkOkJGNDk0QzdCMjlBOTExRTI5NzU0Q0IzMjg3RDA0M0I5IiBzdFJlZjpkb2N1bWVudElEPSJ4bXAuZGlkOkJGNDk0QzdDMjlBOTExRTI5NzU0Q0IzMjg3RDA0M0I5Ii8+IDwvcmRmOkRlc2NyaXB0aW9uPiA8L3JkZjpSREY+IDwveDp4bXBtZXRhPiA8P3hwYWNrZXQgZW5kPSJyIj8+WeGRxAAAB2hJREFUeNrUXFts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",
				className:    "BlobClass",
				dataType:     getDataType(schema.DataTypeBlob),
			},
			want:    "iVBORw0KGgoAAAANSUhEUgAAAGAAAAA/CAYAAAAfQM0aAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAyRpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMy1jMDExIDY2LjE0NTY2MSwgMjAxMi8wMi8wNi0xNDo1NjoyNyAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RSZWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZVJlZiMiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENTNiAoTWFjaW50b3NoKSIgeG1wTU06SW5zdGFuY2VJRD0ieG1wLmlpZDpCRjQ5NEM3RDI5QTkxMUUyOTc1NENCMzI4N0QwNDNCOSIgeG1wTU06RG9jdW1lbnRJRD0ieG1wLmRpZDpCRjQ5NEM3RTI5QTkxMUUyOTc1NENCMzI4N0QwNDNCOSI+IDx4bXBNTTpEZXJpdmVkRnJvbSBzdFJlZjppbnN0YW5jZUlEPSJ4bXAuaWlkOkJGNDk0QzdCMjlBOTExRTI5NzU0Q0IzMjg3RDA0M0I5IiBzdFJlZjpkb2N1bWVudElEPSJ4bXAuZGlkOkJGNDk0QzdDMjlBOTExRTI5NzU0Q0IzMjg3RDA0M0I5Ii8+IDwvcmRmOkRlc2NyaXB0aW9uPiA8L3JkZjpSREY+IDwveDp4bXBtZXRhPiA8P3hwYWNrZXQgZW5kPSJyIj8+WeGRxAAAB2hJREFUeNrUXFtslUUQ3hJCoQVEKy0k1qQgrRg0vaAJaq1tvJSgaLy8mKDF2IvxBY2Bgm8+iIoxvhB72tTUmKgPigbFKCEtxeKD9hZjAi3GJrYJtqRai7TQB+pMz/zwU/5zzsxe2u4kXwiwZ+bb/Xb/s7v/zEmrra1VTFsFeBRQCtgEuBWwkv5vHPAn4DdAB+B7wBjXcUNDQ8o2dXV1SmDzyhUtLS3tBPyxC9CdrN1ihi/swKuA7YD0BG1uJhQDngdcAnwDeJ86Ole2kLii+J2AFsA+wF9RjRalmEUHaZY8m6RDUYZtn6HPHiRfLm2hck0D7AScAdRH8UokwD2AnwA7UoiUyhaRD/S12dHg+8B1OWA/4BTgqVQCPEJL8haLBNDXEfJt03ziipYH+BJwHFAYJcAWwCeAZQ6CLyPfWyz584nrbCuj74eHwgKsddih2R1ba+jHJ65R1k6PuWNhAd4DZM/BTiWbdhwm5hPXsA0AngY8COgNP4JwSTyu4zE/P18VFhZKP7aNYuouXxFX5Ic8Nc2Ea2D/AfYCNgIORZ0DdusOfnFxcXDwUD09PZKP76alKDUR16KiIlVQUHDl7/39/Uozpg7Xac45YB0dGrQHHw07KVwJpRRbYiKuyCc8+MhXcyXocP2RnvMvJhr8QIBK08EPbGJiQuqq0mX7KD4GIohi4xVPTU0N6/BRamPwu7u7dZb3/RozkW3IB3lZEkGHayeI8FFVVdWaZAIUcD2Wl5fbHHy024XtC6QBkomA/XHIFb8X0Xamp6efASHqt27dGnkVkcNxVlFRoXJycmwOvuLGNmifVATsD/bLZezgKgKE2J+bm3sKHk3XXUWs4Mz87Oxs24OvOLEN26cUAfvFXAkrlKGBCDNXEbAajldXV1+5ijjP+KCrg855x+3nk2uy8SwDdIIIM1cRI6k+0NraqkZGRmzuKAIbFrYf0Q2UaPOA/Wpra3PBNfHhYHq6HbC5qanpGB7ETgPWc0TApTr7eyDolOaj6LRG+/W2Bn94eJg7+DpcowZ+AGb+642NjYfC3wEdXAdI1uK2Du2ksH2HrcHHfggGX4frNVcRMPh7BwcHN8ZiseuuIr4DvKXib29YX2bhmW+wEqYptsREXC2eWXS44oyfuYqYmpra19LSEnkaRgEG6Nj8gGRHESVCRkaG9Kg+IOyTiGtmZqatnZsOV/zMLnjcsF7KH5AIECVCX1+f6u3tlbg4oLmc2VyDy8HgPshg2yzmCo8aFsdAALzpw9dw23REwJkvHPwjSu92UcwVRcAnAd4LaQ6+CVe2AGivAe5WwhcdGp0aoVgmJuIqnBy2uSa18Buxs4AXAJMO401SjLOGfnziyhYg2GrtcNSxSfJ90pI/n7iyBUA7quKv/IYsxhmiZ/ZRy/x94soWAO1nwL0qnhVw2cD/ZfKBvjod9cEnrmwB0DBh9RUVfxHxhYrnUHLtEn2mlHyMOe6HT1wT7oISGSas4ntNzJmsVFczjnMBN1CbfwGD1BYPID8A/lFzbz5xZQsQnmWfExa6ecNVIsBKWuIlgA0qnjG2PLhsou0aZgF3qfil2fg89ssbrhwBNtB+GN/dLUnQ5kbCHYAnAFMAvGpsoY7OlS0krmOhxx7WLHwAeBLwVahN2uIUswgrPB5T8rRv7DxWqDwM+JaCjzue8b5wZe2C7gJ8quKVJqY599vJ1yZHffCJK0uA+wAfAtZYjIO+Gsi3TfOJK0sAfFP/jpKV+HBtKfkutOTPJ64sAVYD3qXgrmwpxVht6McnrmwBMAP4pjlYdRij3tCHT1xZAuDdermOA836gDKKqWNirob1ASZc2eeAl3QH36A+AGP+ohFWxNVSfYAuV9YKyKUTo/bgo2nUB5RQbImJuFqsD9DhyhbAuDgjMI36gFKX7S3XB5S6egSV2Bh8zYyDYjr4SGYi2yzmMIm5YnFGkFOLSQGNjY3X/BtaLBabWQF5XKcO6gOkZT950gAW6wPWuXoEZXEaOqoPyHLcPqkIwvqALFcCZHJmvqP6gEzH7VOKIKgPyHQlwIVUjRzWB1xw3H4+ubIFGE3VyGF9wKjj9ik3D4L6gFFXArCSTlEEzKe3LMIfwvYDNgcf+4P9csSVLUAXt7GD+oBuYfsuW4OvUR/Q7UoA/G2zaRvbOqEI0xRbYiKulusDTrgSYEg6sxKJIKwP6FLyjDYRV4v1ATpc2QKgNZtu6zTqA5o1ObM/h5eDyMvCtrlZObLgNhRv+jAHvkwqQjDzhYPfrvRvF0VcLdQHaHGNxWKrZv0d//hahcqr8Ccww1kRbwPuVMIXHRqd+ptimZiIq0F9gA2urEcQ2jkVf/tz0WG8ixTjnKEfn7iyBQi2WnuULLlV0qE9FrdzPnFlC4CGRQkvqyQ/MqRh6KtO2S948IkrWwC0XwHPAQ4r85z7w+TL1U8Y+8Q14S4oyjA9703AZ4AqFX8RvoTpN8i3/Bi/p+egHz5xZQsQGCasvqGuZhzj76DdpuIZx8FPuOAviWDG8e8qXl0yXxnHPnGdsf8FGAByGwC02iMZswAAAABJRU5ErkJggg==",
			wantErr: false,
		},
		{
			name:   "Validate blob - nil entry",
			fields: validatorFields,
			args: args{
				ctx:          context.Background(),
				propertyName: "blobProperty",
				pv:           nil,
				className:    "BlobClass",
				dataType:     getDataType(schema.DataTypeBlob),
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &Validator{
				exists: tt.fields.exists,
				config: tt.fields.config,
			}
			got, err := v.extractAndValidateProperty(tt.args.ctx, tt.args.propertyName, tt.args.pv, tt.args.className, tt.args.dataType, "")
			if (err != nil) != tt.wantErr {
				t.Errorf("Validator.extractAndValidateProperty() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Validator.extractAndValidateProperty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func getDataType(dataType schema.DataType) *schema.DataType {
	return &dataType
}

func TestProperties(t *testing.T) {
	const myBeacon = "weaviate://localhost/things/8e555f0d-8590-48c2-a9a6-70772ed14c0a"
	myJournalClass := &models.Class{
		Properties: []*models.Property{
			{Name: "inJournal", DataType: []string{"Journal"}},
			{
				Name:             "nested",
				DataType:         schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{{Name: "nested2", DataType: schema.DataTypeObject.PropString()}},
			},
		},
	}
	specs := map[string]struct {
		class     *models.Class
		obj       *models.Object
		expErr    bool
		expBeacon strfmt.URI
	}{
		"incorrect cref body - example from issue #1253": {
			class: myJournalClass,
			obj: &models.Object{
				Properties: map[string]any{"inJournal": []any{map[string]any{
					"beacon": map[string]any{"beacon": myBeacon},
				}}},
			},
			expErr: true,
		},
		"complete beacon": {
			class: myJournalClass,
			obj: &models.Object{
				Properties: map[string]any{"inJournal": []any{map[string]any{
					"beacon": myBeacon,
				}}},
			},
			expBeacon: myBeacon,
		},
		"beacon without class": {
			class: myJournalClass,
			obj: &models.Object{
				Properties: map[string]any{"inJournal": []any{map[string]any{
					"beacon": "weaviate://foo/8e555f0d-8590-48c2-a9a6-70772ed14c0a",
				}}},
			},
			expBeacon: "weaviate://localhost/Journal/8e555f0d-8590-48c2-a9a6-70772ed14c0a",
		},
		"nested with nil": {
			class: myJournalClass,
			obj: &models.Object{
				Properties: map[string]interface{}{"nested": map[string]interface{}{
					"nested2": nil,
				}},
			},
		},
	}
	for name, spec := range specs {
		t.Run(name, func(t *testing.T) {
			validator := &Validator{exists: func(_ context.Context, class string, _ strfmt.UUID, _ *additional.ReplicationProperties, _ string) (bool, error) {
				return true, nil
			}}
			gotErr := validator.properties(context.Background(), spec.class, spec.obj, nil)
			if spec.expErr {
				require.Error(t, gotErr)
				return
			}
			require.NoError(t, gotErr)
			props := spec.obj.Properties
			if spec.expBeacon != "" {
				gotBeacon := extractBeacon(t, props)
				assert.Equal(t, spec.expBeacon, gotBeacon)

			}
		})
	}
}

func extractBeacon(t *testing.T, props models.PropertySchema) strfmt.URI {
	require.IsType(t, map[string]any{}, props)
	require.Contains(t, props.(map[string]any), "inJournal")
	journalProp := props.(map[string]any)["inJournal"]
	require.IsType(t, models.MultipleRef{}, journalProp)
	require.Len(t, journalProp.(models.MultipleRef), 1)
	gotBeacon := journalProp.(models.MultipleRef)[0].Beacon
	return gotBeacon
}

func TestValidator_ValuesCasting(t *testing.T) {
	t.Run("int(s)", func(t *testing.T) {
		type testCase struct {
			value         interface{}
			expectedValue float64
			expectedErr   bool
		}

		testCases := []testCase{
			{
				value:         json.Number("123"),
				expectedValue: float64(123),
				expectedErr:   false,
			},
			{
				value:         int64(123),
				expectedValue: float64(123),
				expectedErr:   false,
			},
			{
				value:         float64(123),
				expectedValue: float64(123),
				expectedErr:   false,
			},

			{
				value:         json.Number("123.5"),
				expectedValue: float64(0),
				expectedErr:   true,
			},
			{
				value:         float64(123.5),
				expectedValue: float64(0),
				expectedErr:   true,
			},
			{
				value:         "123.5",
				expectedValue: float64(0),
				expectedErr:   true,
			},
			{
				value:         "something",
				expectedValue: float64(0),
				expectedErr:   true,
			},
			{
				value:         true,
				expectedValue: float64(0),
				expectedErr:   true,
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("int #%d", i), func(t *testing.T) {
				value, err := intVal(tc.value)

				if tc.expectedErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, tc.expectedValue, value)
			})
		}
		for i, tc := range testCases {
			t.Run(fmt.Sprintf("ints (single) #%d", i), func(t *testing.T) {
				value, err := intArrayVal(tc.value)

				assert.Error(t, err)
				assert.Nil(t, value)
			})
			t.Run(fmt.Sprintf("ints (array) #%d", i), func(t *testing.T) {
				value, err := intArrayVal([]interface{}{tc.value})

				if tc.expectedErr {
					assert.Error(t, err)
					assert.Nil(t, value)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, []float64{tc.expectedValue}, value)
				}
			})
		}
	})

	t.Run("number(s)", func(t *testing.T) {
		type testCase struct {
			value         interface{}
			expectedValue float64
			expectedErr   bool
		}

		testCases := []testCase{
			{
				value:         json.Number("123"),
				expectedValue: float64(123),
				expectedErr:   false,
			},
			{
				value:         int64(123),
				expectedValue: float64(123),
				expectedErr:   false,
			},
			{
				value:         float64(123),
				expectedValue: float64(123),
				expectedErr:   false,
			},
			{
				value:         json.Number("123.5"),
				expectedValue: float64(123.5),
				expectedErr:   false,
			},
			{
				value:         float64(123.5),
				expectedValue: float64(123.5),
				expectedErr:   false,
			},

			{
				value:         "123.5",
				expectedValue: float64(0),
				expectedErr:   true,
			},
			{
				value:         "something",
				expectedValue: float64(0),
				expectedErr:   true,
			},
			{
				value:         true,
				expectedValue: float64(0),
				expectedErr:   true,
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("number #%d", i), func(t *testing.T) {
				value, err := numberVal(tc.value)

				if tc.expectedErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, tc.expectedValue, value)
			})
		}
		for i, tc := range testCases {
			t.Run(fmt.Sprintf("numbers (single) #%d", i), func(t *testing.T) {
				value, err := numberArrayVal(tc.value)

				assert.Error(t, err)
				assert.Nil(t, value)
			})
			t.Run(fmt.Sprintf("numbers (array) #%d", i), func(t *testing.T) {
				value, err := numberArrayVal([]interface{}{tc.value})

				if tc.expectedErr {
					assert.Error(t, err)
					assert.Nil(t, value)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, []float64{tc.expectedValue}, value)
				}
			})
		}
	})

	t.Run("string(s)", func(t *testing.T) {
		type testCase struct {
			value         interface{}
			expectedValue string
			expectedErr   bool
		}

		testCases := []testCase{
			{
				value:         "123.5",
				expectedValue: "123.5",
				expectedErr:   false,
			},
			{
				value:         "something",
				expectedValue: "something",
				expectedErr:   false,
			},

			{
				value:         json.Number("123"),
				expectedValue: "",
				expectedErr:   true,
			},
			{
				value:         int64(123),
				expectedValue: "",
				expectedErr:   true,
			},
			{
				value:         float64(123),
				expectedValue: "",
				expectedErr:   true,
			},
			{
				value:         []byte("something"),
				expectedValue: "",
				expectedErr:   true,
			},
			{
				value:         true,
				expectedValue: "",
				expectedErr:   true,
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("string #%d", i), func(t *testing.T) {
				value, err := stringVal(tc.value)

				if tc.expectedErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, tc.expectedValue, value)
			})
		}
		for i, tc := range testCases {
			t.Run(fmt.Sprintf("strings (single) #%d", i), func(t *testing.T) {
				value, err := stringArrayVal(tc.value, "text")

				assert.Error(t, err)
				assert.Nil(t, value)
			})
			t.Run(fmt.Sprintf("strings (array) #%d", i), func(t *testing.T) {
				value, err := stringArrayVal([]interface{}{tc.value}, "text")

				if tc.expectedErr {
					assert.Error(t, err)
					assert.Nil(t, value)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, []string{tc.expectedValue}, value)
				}
			})
		}
	})

	t.Run("bool(s)", func(t *testing.T) {
		type testCase struct {
			value         interface{}
			expectedValue bool
			expectedErr   bool
		}

		testCases := []testCase{
			{
				value:         true,
				expectedValue: true,
				expectedErr:   false,
			},
			{
				value:         false,
				expectedValue: false,
				expectedErr:   false,
			},

			{
				value:         float64(1),
				expectedValue: false,
				expectedErr:   true,
			},
			{
				value:         int64(1),
				expectedValue: false,
				expectedErr:   true,
			},
			{
				value:         "1",
				expectedValue: false,
				expectedErr:   true,
			},
			{
				value:         "true",
				expectedValue: false,
				expectedErr:   true,
			},
			{
				value:         "something",
				expectedValue: false,
				expectedErr:   true,
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("bool #%d", i), func(t *testing.T) {
				value, err := boolVal(tc.value)

				if tc.expectedErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, tc.expectedValue, value)
			})
		}
		for i, tc := range testCases {
			t.Run(fmt.Sprintf("bools (single) #%d", i), func(t *testing.T) {
				value, err := boolArrayVal(tc.value)

				assert.Error(t, err)
				assert.Nil(t, value)
			})
			t.Run(fmt.Sprintf("bools (array) #%d", i), func(t *testing.T) {
				value, err := boolArrayVal([]interface{}{tc.value})

				if tc.expectedErr {
					assert.Error(t, err)
					assert.Nil(t, value)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, []bool{tc.expectedValue}, value)
				}
			})
		}
	})

	t.Run("uuid(s)", func(t *testing.T) {
		type testCase struct {
			value         interface{}
			expectedValue uuid.UUID
			expectedErr   bool
		}

		testCases := []testCase{
			{
				value:         "e780b0a4-8d0e-4c09-898e-19d6b81e1e63",
				expectedValue: uuid.MustParse("e780b0a4-8d0e-4c09-898e-19d6b81e1e63"),
				expectedErr:   false,
			},
			{
				value:         "e780b0a48d0e4c09898e19d6b81e1e63",
				expectedValue: uuid.MustParse("e780b0a4-8d0e-4c09-898e-19d6b81e1e63"),
				expectedErr:   false,
			},

			{
				value:         float64(123),
				expectedValue: [16]byte{},
				expectedErr:   true,
			},
			{
				value:         int64(123),
				expectedValue: [16]byte{},
				expectedErr:   true,
			},
			{
				value:         "123",
				expectedValue: [16]byte{},
				expectedErr:   true,
			},
			{
				value:         "something",
				expectedValue: [16]byte{},
				expectedErr:   true,
			},
			{
				value:         true,
				expectedValue: [16]byte{},
				expectedErr:   true,
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("uuid #%d", i), func(t *testing.T) {
				value, err := uuidVal(tc.value)

				if tc.expectedErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, tc.expectedValue, value)
			})
		}
		for i, tc := range testCases {
			t.Run(fmt.Sprintf("uuids (single) #%d", i), func(t *testing.T) {
				value, err := uuidArrayVal(tc.value)

				assert.Error(t, err)
				assert.Nil(t, value)
			})
			t.Run(fmt.Sprintf("uuids (array) #%d", i), func(t *testing.T) {
				value, err := uuidArrayVal([]interface{}{tc.value})

				if tc.expectedErr {
					assert.Error(t, err)
					assert.Nil(t, value)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, []uuid.UUID{tc.expectedValue}, value)
				}
			})
		}
	})

	t.Run("date(s)", func(t *testing.T) {
		type testCase struct {
			value         interface{}
			expectedValue time.Time
			expectedErr   bool
		}

		testCases := []testCase{
			{
				value:         "2024-01-02T03:04:05.00Z",
				expectedValue: time.Unix(1704164645, 0).UTC(),
				expectedErr:   false,
			},

			{
				value:         float64(123),
				expectedValue: time.Time{},
				expectedErr:   true,
			},
			{
				value:         int64(123),
				expectedValue: time.Time{},
				expectedErr:   true,
			},
			{
				value:         "123",
				expectedValue: time.Time{},
				expectedErr:   true,
			},
			{
				value:         "something",
				expectedValue: time.Time{},
				expectedErr:   true,
			},
			{
				value:         true,
				expectedValue: time.Time{},
				expectedErr:   true,
			},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("date #%d", i), func(t *testing.T) {
				value, err := dateVal(tc.value)

				if tc.expectedErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, tc.expectedValue, value)
			})
		}
		for i, tc := range testCases {
			t.Run(fmt.Sprintf("dates (single) #%d", i), func(t *testing.T) {
				value, err := dateArrayVal(tc.value)

				assert.Error(t, err)
				assert.Nil(t, value)
			})
			t.Run(fmt.Sprintf("dates (array) #%d", i), func(t *testing.T) {
				value, err := dateArrayVal([]interface{}{tc.value})

				if tc.expectedErr {
					assert.Error(t, err)
					assert.Nil(t, value)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, []time.Time{tc.expectedValue}, value)
				}
			})
		}
	})
}
