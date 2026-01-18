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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestDecodePropertyValue_Text(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		dataType schema.DataType
		expected any
	}{
		{
			name:     "simple text",
			input:    []byte("hello world"),
			dataType: schema.DataTypeText,
			expected: "hello world",
		},
		{
			name:     "empty text",
			input:    []byte(""),
			dataType: schema.DataTypeText,
			expected: "",
		},
		{
			name:     "text with special characters",
			input:    []byte("hello\nworld\ttab"),
			dataType: schema.DataTypeText,
			expected: "hello\nworld\ttab",
		},
		{
			name:     "text with unicode",
			input:    []byte("こんにちは世界"),
			dataType: schema.DataTypeText,
			expected: "こんにちは世界",
		},
		{
			name:     "text array type",
			input:    []byte("array element"),
			dataType: schema.DataTypeTextArray,
			expected: "array element",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := decodePropertyValue(tt.input, tt.dataType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDecodePropertyValue_Int(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		dataType schema.DataType
	}{
		{
			name:     "positive int",
			value:    12345,
			dataType: schema.DataTypeInt,
		},
		{
			name:     "negative int",
			value:    -12345,
			dataType: schema.DataTypeInt,
		},
		{
			name:     "zero",
			value:    0,
			dataType: schema.DataTypeInt,
		},
		{
			name:     "max int64",
			value:    9223372036854775807,
			dataType: schema.DataTypeInt,
		},
		{
			name:     "min int64",
			value:    -9223372036854775808,
			dataType: schema.DataTypeInt,
		},
		{
			name:     "int array type",
			value:    42,
			dataType: schema.DataTypeIntArray,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := entinverted.LexicographicallySortableInt64(tt.value)
			require.NoError(t, err)

			result := decodePropertyValue(encoded, tt.dataType)
			assert.Equal(t, tt.value, result)
		})
	}
}

func TestDecodePropertyValue_Int_InvalidFormat(t *testing.T) {
	// Invalid length - should fall back to string
	result := decodePropertyValue([]byte("short"), schema.DataTypeInt)
	assert.Equal(t, "short", result)
}

func TestDecodePropertyValue_Number(t *testing.T) {
	tests := []struct {
		name     string
		value    float64
		dataType schema.DataType
	}{
		{
			name:     "positive float",
			value:    123.456,
			dataType: schema.DataTypeNumber,
		},
		{
			name:     "negative float",
			value:    -123.456,
			dataType: schema.DataTypeNumber,
		},
		{
			name:     "zero",
			value:    0.0,
			dataType: schema.DataTypeNumber,
		},
		{
			name:     "small float",
			value:    0.000001,
			dataType: schema.DataTypeNumber,
		},
		{
			name:     "large float",
			value:    1e100,
			dataType: schema.DataTypeNumber,
		},
		{
			name:     "negative large float",
			value:    -1e100,
			dataType: schema.DataTypeNumber,
		},
		{
			name:     "number array type",
			value:    3.14159,
			dataType: schema.DataTypeNumberArray,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := entinverted.LexicographicallySortableFloat64(tt.value)
			require.NoError(t, err)

			result := decodePropertyValue(encoded, tt.dataType)
			assert.Equal(t, tt.value, result)
		})
	}
}

func TestDecodePropertyValue_Number_InvalidFormat(t *testing.T) {
	// Invalid length - should fall back to string
	result := decodePropertyValue([]byte("short"), schema.DataTypeNumber)
	assert.Equal(t, "short", result)
}

func TestDecodePropertyValue_Boolean(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		dataType schema.DataType
		expected bool
	}{
		{
			name:     "true",
			input:    []byte{1},
			dataType: schema.DataTypeBoolean,
			expected: true,
		},
		{
			name:     "false",
			input:    []byte{0},
			dataType: schema.DataTypeBoolean,
			expected: false,
		},
		{
			name:     "boolean array true",
			input:    []byte{1},
			dataType: schema.DataTypeBooleanArray,
			expected: true,
		},
		{
			name:     "boolean array false",
			input:    []byte{0},
			dataType: schema.DataTypeBooleanArray,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := decodePropertyValue(tt.input, tt.dataType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDecodePropertyValue_Boolean_InvalidFormat(t *testing.T) {
	// Invalid length - should fall back to string
	result := decodePropertyValue([]byte("invalid"), schema.DataTypeBoolean)
	assert.Equal(t, "invalid", result)
}

func TestDecodePropertyValue_Date(t *testing.T) {
	tests := []struct {
		name     string
		nanos    int64
		dataType schema.DataType
	}{
		{
			name:     "recent date",
			nanos:    time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC).UnixNano(),
			dataType: schema.DataTypeDate,
		},
		{
			name:     "epoch",
			nanos:    0,
			dataType: schema.DataTypeDate,
		},
		{
			name:     "date with nanoseconds",
			nanos:    time.Date(2024, 1, 1, 0, 0, 0, 123456789, time.UTC).UnixNano(),
			dataType: schema.DataTypeDate,
		},
		{
			name:     "date array type",
			nanos:    time.Date(2020, 12, 31, 23, 59, 59, 0, time.UTC).UnixNano(),
			dataType: schema.DataTypeDateArray,
		},
		{
			name:     "negative timestamp (before epoch)",
			nanos:    time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
			dataType: schema.DataTypeDate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := entinverted.LexicographicallySortableInt64(tt.nanos)
			require.NoError(t, err)

			result := decodePropertyValue(encoded, tt.dataType)

			// The decode function uses time.Unix(0, nanos) which returns local time
			// So we need to compare with what the function actually returns
			expected := time.Unix(0, tt.nanos).Format(time.RFC3339Nano)
			assert.Equal(t, expected, result)
		})
	}
}

func TestDecodePropertyValue_Date_InvalidFormat(t *testing.T) {
	// Invalid length - should fall back to string
	result := decodePropertyValue([]byte("short"), schema.DataTypeDate)
	assert.Equal(t, "short", result)
}

func TestDecodePropertyValue_UUID(t *testing.T) {
	tests := []struct {
		name     string
		uuid     string
		bytes    []byte
		dataType schema.DataType
	}{
		{
			name:     "standard uuid",
			uuid:     "550e8400-e29b-41d4-a716-446655440000",
			bytes:    []byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00},
			dataType: schema.DataTypeUUID,
		},
		{
			name:     "all zeros uuid",
			uuid:     "00000000-0000-0000-0000-000000000000",
			bytes:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			dataType: schema.DataTypeUUID,
		},
		{
			name:     "all ones uuid",
			uuid:     "ffffffff-ffff-ffff-ffff-ffffffffffff",
			bytes:    []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			dataType: schema.DataTypeUUID,
		},
		{
			name:     "uuid array type",
			uuid:     "12345678-1234-5678-1234-567812345678",
			bytes:    []byte{0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78},
			dataType: schema.DataTypeUUIDArray,
		},
		{
			name:     "uuid with leading zeros",
			uuid:     "00000001-0002-0003-0004-000000000005",
			bytes:    []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05},
			dataType: schema.DataTypeUUID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := decodePropertyValue(tt.bytes, tt.dataType)
			assert.Equal(t, tt.uuid, result)
		})
	}
}

func TestDecodePropertyValue_UUID_InvalidFormat(t *testing.T) {
	// Invalid length (not 16 bytes) - should fall back to string
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "too short",
			input:    []byte("short"),
			expected: "short",
		},
		{
			name:     "too long",
			input:    []byte("this is way too long to be a uuid"),
			expected: "this is way too long to be a uuid",
		},
		{
			name:     "empty",
			input:    []byte{},
			expected: "",
		},
		{
			name:     "15 bytes",
			input:    []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
			expected: string([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := decodePropertyValue(tt.input, schema.DataTypeUUID)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDecodePropertyValue_UnknownType(t *testing.T) {
	// Unknown data types should fall back to returning the raw string
	tests := []struct {
		name     string
		input    []byte
		dataType schema.DataType
		expected string
	}{
		{
			name:     "unknown type",
			input:    []byte("raw data"),
			dataType: schema.DataType("unknownType"),
			expected: "raw data",
		},
		{
			name:     "empty unknown type",
			input:    []byte(""),
			dataType: schema.DataType(""),
			expected: "",
		},
		{
			name:     "blob type falls back to string",
			input:    []byte("blob data"),
			dataType: schema.DataTypeBlob,
			expected: "blob data",
		},
		{
			name:     "geoCoordinates type falls back to string",
			input:    []byte("geo data"),
			dataType: schema.DataTypeGeoCoordinates,
			expected: "geo data",
		},
		{
			name:     "phoneNumber type falls back to string",
			input:    []byte("phone data"),
			dataType: schema.DataTypePhoneNumber,
			expected: "phone data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := decodePropertyValue(tt.input, tt.dataType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestDecodePropertyValue_UUIDBugRegression specifically tests the bug that was fixed
// where UUIDs stored as 16-byte binary were incorrectly returned as raw strings
func TestDecodePropertyValue_UUIDBugRegression(t *testing.T) {
	// This test ensures the bug fix works correctly:
	// UUIDs are stored as 16-byte binary, not as string representations
	// The bug was returning string([]byte{...}) which produced garbage
	// The fix properly formats the 16 bytes as a UUID string

	// Test case that would have caught the original bug
	uuid := []byte{
		0x55, 0x0e, 0x84, 0x00, // 550e8400
		0xe2, 0x9b, // e29b
		0x41, 0xd4, // 41d4
		0xa7, 0x16, // a716
		0x44, 0x66, 0x55, 0x44, 0x00, 0x00, // 446655440000
	}

	result := decodePropertyValue(uuid, schema.DataTypeUUID)

	// Before the fix, this would return garbage like "U\x0e\x84\x00\xe2\x9bA\xd4\xa7\x16Df\x55D\x00\x00"
	// After the fix, it correctly returns the UUID string
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", result)

	// Verify it's a string type, not bytes
	_, ok := result.(string)
	assert.True(t, ok, "result should be a string")
}

// TestDecodePropertyValue_AllDataTypes provides a comprehensive test across all
// primary data types to ensure consistent behavior
func TestDecodePropertyValue_AllDataTypes(t *testing.T) {
	// Prepare encoded values
	intEncoded, _ := entinverted.LexicographicallySortableInt64(42)
	floatEncoded, _ := entinverted.LexicographicallySortableFloat64(3.14)
	dateNanos := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano()
	dateEncoded, _ := entinverted.LexicographicallySortableInt64(dateNanos)
	uuidBytes := []byte{0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78}

	tests := []struct {
		name      string
		input     []byte
		dataType  schema.DataType
		checkFunc func(t *testing.T, result any)
	}{
		{
			name:     "text",
			input:    []byte("hello"),
			dataType: schema.DataTypeText,
			checkFunc: func(t *testing.T, result any) {
				assert.Equal(t, "hello", result)
			},
		},
		{
			name:     "int",
			input:    intEncoded,
			dataType: schema.DataTypeInt,
			checkFunc: func(t *testing.T, result any) {
				assert.Equal(t, int64(42), result)
			},
		},
		{
			name:     "number",
			input:    floatEncoded,
			dataType: schema.DataTypeNumber,
			checkFunc: func(t *testing.T, result any) {
				assert.Equal(t, 3.14, result)
			},
		},
		{
			name:     "boolean true",
			input:    []byte{1},
			dataType: schema.DataTypeBoolean,
			checkFunc: func(t *testing.T, result any) {
				assert.Equal(t, true, result)
			},
		},
		{
			name:     "boolean false",
			input:    []byte{0},
			dataType: schema.DataTypeBoolean,
			checkFunc: func(t *testing.T, result any) {
				assert.Equal(t, false, result)
			},
		},
		{
			name:     "date",
			input:    dateEncoded,
			dataType: schema.DataTypeDate,
			checkFunc: func(t *testing.T, result any) {
				// The decode function uses local time
				expected := time.Unix(0, dateNanos).Format(time.RFC3339Nano)
				assert.Equal(t, expected, result)
			},
		},
		{
			name:     "uuid",
			input:    uuidBytes,
			dataType: schema.DataTypeUUID,
			checkFunc: func(t *testing.T, result any) {
				assert.Equal(t, "12345678-1234-5678-1234-567812345678", result)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := decodePropertyValue(tt.input, tt.dataType)
			tt.checkFunc(t, result)
		})
	}
}
