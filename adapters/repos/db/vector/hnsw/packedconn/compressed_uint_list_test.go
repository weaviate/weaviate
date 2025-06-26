package packedconn

import (
	"math"
	"reflect"
	"testing"
)

func TestNewCompressedUint64List(t *testing.T) {
	c := NewCompressedUint64List()

	if c == nil {
		t.Fatal("NewCompressedUint64List returned nil")
	}

	if c.Count() != 0 {
		t.Errorf("Expected count 0, got %d", c.Count())
	}

	if c.scheme != 0 {
		t.Errorf("Expected scheme 0, got %d", c.scheme)
	}

	if len(c.data) != 0 {
		t.Errorf("Expected empty data, got length %d", len(c.data))
	}

	if len(c.positions) != 0 {
		t.Errorf("Expected empty positions, got length %d", len(c.positions))
	}
}

func TestBytesNeeded(t *testing.T) {
	tests := []struct {
		value    uint64
		expected int
	}{
		{0, 1},
		{1, 1},
		{255, 1},
		{256, 2},
		{65535, 2},
		{65536, 3},
		{16777215, 3},
		{16777216, 4},
		{4294967295, 4},
		{4294967296, 5},
		{1099511627775, 5},
		{1099511627776, 6},
		{281474976710655, 6},
		{281474976710656, 7},
		{72057594037927935, 7},
		{72057594037927936, 8},
		{math.MaxUint64, 8},
	}

	for _, test := range tests {
		result := bytesNeeded(test.value)
		if result != test.expected {
			t.Errorf("bytesNeeded(%d) = %d, expected %d", test.value, result, test.expected)
		}
	}
}

func TestCalculateScheme(t *testing.T) {
	tests := []struct {
		maxBytes int
		expected uint8
	}{
		{1, 0},
		{2, 1},
		{3, 2},
		{4, 2},
		{5, 3},
		{6, 3},
		{7, 3},
		{8, 3},
	}

	for _, test := range tests {
		result := calculateScheme(test.maxBytes)
		if result != test.expected {
			t.Errorf("calculateScheme(%d) = %d, expected %d", test.maxBytes, result, test.expected)
		}
	}
}

func TestAppendSingleValue(t *testing.T) {
	c := NewCompressedUint64List()

	// Test adding a single byte value
	c.Append(100)

	if c.Count() != 1 {
		t.Errorf("Expected count 1, got %d", c.Count())
	}

	values := c.DecodeValues()
	if len(values) != 1 || values[0] != 100 {
		t.Errorf("Expected [100], got %v", values)
	}
}

func TestEncodeValuesEmpty(t *testing.T) {
	c := NewCompressedUint64List()
	c.EncodeValues([]uint64{})

	if c.Count() != 0 {
		t.Errorf("Expected count 0 after encoding empty slice, got %d", c.Count())
	}

	values := c.DecodeValues()
	if values != nil {
		t.Errorf("Expected nil from DecodeValues on empty list, got %v", values)
	}
}

func TestEncodeValuesSingleByte(t *testing.T) {
	c := NewCompressedUint64List()
	input := []uint64{0, 1, 100, 255}

	c.EncodeValues(input)

	if c.Count() != len(input) {
		t.Errorf("Expected count %d, got %d", len(input), c.Count())
	}

	if c.scheme != 0 {
		t.Errorf("Expected scheme 0 for single-byte values, got %d", c.scheme)
	}

	values := c.DecodeValues()
	if !reflect.DeepEqual(values, input) {
		t.Errorf("Expected %v, got %v", input, values)
	}
}

func TestEncodeValuesTwoBytes(t *testing.T) {
	c := NewCompressedUint64List()
	input := []uint64{100, 256, 65535}

	c.EncodeValues(input)

	if c.Count() != len(input) {
		t.Errorf("Expected count %d, got %d", len(input), c.Count())
	}

	if c.scheme != 1 {
		t.Errorf("Expected scheme 1 for values up to 2 bytes, got %d", c.scheme)
	}

	values := c.DecodeValues()
	if !reflect.DeepEqual(values, input) {
		t.Errorf("Expected %v, got %v", input, values)
	}
}

func TestEncodeValuesMultipleByteSizes(t *testing.T) {
	c := NewCompressedUint64List()
	input := []uint64{100, 256, 65536, 16777216}

	c.EncodeValues(input)

	if c.Count() != len(input) {
		t.Errorf("Expected count %d, got %d", len(input), c.Count())
	}

	if c.scheme != 2 {
		t.Errorf("Expected scheme 2 for values up to 4 bytes, got %d", c.scheme)
	}

	values := c.DecodeValues()
	if !reflect.DeepEqual(values, input) {
		t.Errorf("Expected %v, got %v", input, values)
	}
}

func TestSchemeUpgrade(t *testing.T) {
	c := NewCompressedUint64List()

	// Start with single-byte values
	c.EncodeValues([]uint64{100, 200})
	if c.scheme != 0 {
		t.Errorf("Expected initial scheme 0, got %d", c.scheme)
	}

	// Add a two-byte value - should upgrade scheme
	c.Append(300)
	if c.scheme != 1 {
		t.Errorf("Expected scheme still 0 for values <= 255, got %d", c.scheme)
	}

	// Add a value that requires 2 bytes
	c.Append(256)
	if c.scheme != 1 {
		t.Errorf("Expected scheme upgrade to 1, got %d", c.scheme)
	}

	// Verify all values are still correct
	expected := []uint64{100, 200, 300, 256}
	values := c.DecodeValues()
	if !reflect.DeepEqual(values, expected) {
		t.Errorf("Expected %v, got %v", expected, values)
	}

	// Add a value that requires 3 bytes
	c.Append(65536)
	if c.scheme != 2 {
		t.Errorf("Expected scheme upgrade to 2, got %d", c.scheme)
	}

	// Verify all values are still correct after scheme upgrade
	expected = append(expected, 65536)
	values = c.DecodeValues()
	if !reflect.DeepEqual(values, expected) {
		t.Errorf("Expected %v, got %v", expected, values)
	}
}

func TestMixedAppendAndEncodeValues(t *testing.T) {
	c := NewCompressedUint64List()

	// Mix of Append and EncodeValues
	c.Append(100)
	c.EncodeValues([]uint64{200, 300})
	c.Append(400)
	c.EncodeValues([]uint64{500, 600, 700})

	expected := []uint64{100, 200, 300, 400, 500, 600, 700}
	values := c.DecodeValues()

	if !reflect.DeepEqual(values, expected) {
		t.Errorf("Expected %v, got %v", expected, values)
	}

	if c.Count() != len(expected) {
		t.Errorf("Expected count %d, got %d", len(expected), c.Count())
	}
}

func TestCopyValues(t *testing.T) {
	c := NewCompressedUint64List()
	input := []uint64{100, 200, 300, 400}
	c.EncodeValues(input)

	// Test with nil target
	result1 := c.CopyValues(nil)
	if !reflect.DeepEqual(result1, input) {
		t.Errorf("CopyValues with nil target: expected %v, got %v", input, result1)
	}

	// Test with target having sufficient capacity
	target := make([]uint64, 0, 10)
	result2 := c.CopyValues(target)
	if !reflect.DeepEqual(result2, input) {
		t.Errorf("CopyValues with sufficient capacity: expected %v, got %v", input, result2)
	}

	// Test with target having insufficient capacity
	smallTarget := make([]uint64, 0, 2)
	result3 := c.CopyValues(smallTarget)
	if !reflect.DeepEqual(result3, input) {
		t.Errorf("CopyValues with insufficient capacity: expected %v, got %v", input, result3)
	}

	// Test with empty list
	emptyList := NewCompressedUint64List()
	result4 := emptyList.CopyValues(target)
	if len(result4) != 0 {
		t.Errorf("CopyValues on empty list: expected empty slice, got %v", result4)
	}
}

func TestEdgeCases(t *testing.T) {
	c := NewCompressedUint64List()

	// Test with zero
	c.Append(0)
	values := c.DecodeValues()
	if len(values) != 1 || values[0] != 0 {
		t.Errorf("Expected [0], got %v", values)
	}

	// Test with powers of 2 minus 1 (edge cases for byte boundaries)
	edgeCases := []uint64{
		255,        // 2^8 - 1
		65535,      // 2^16 - 1
		16777215,   // 2^24 - 1
		4294967295, // 2^32 - 1
	}

	c2 := NewCompressedUint64List()
	c2.EncodeValues(edgeCases)

	decoded := c2.DecodeValues()
	if !reflect.DeepEqual(decoded, edgeCases) {
		t.Errorf("Expected %v, got %v", edgeCases, decoded)
	}

	// Test with powers of 2 (next byte boundary)
	nextBoundary := []uint64{
		256,        // 2^8
		65536,      // 2^16
		16777216,   // 2^24
		4294967296, // 2^32
	}

	c3 := NewCompressedUint64List()
	c3.EncodeValues(nextBoundary)

	decoded2 := c3.DecodeValues()
	if !reflect.DeepEqual(decoded2, nextBoundary) {
		t.Errorf("Expected %v, got %v", nextBoundary, decoded2)
	}
}

func TestPositionBitPacking(t *testing.T) {
	c := NewCompressedUint64List()

	// Add values that will create different position encodings
	// This tests the bit packing in positions array
	values := []uint64{
		100,      // 1 byte
		256,      // 2 bytes
		100,      // 1 byte
		65536,    // 3 bytes
		200,      // 1 byte
		300,      // 2 bytes
		16777216, // 4 bytes
		150,      // 1 byte
	}

	c.EncodeValues(values)

	// Should use scheme 2 (2 bits per position) since max is 4 bytes
	if c.scheme != 2 {
		t.Errorf("Expected scheme 2, got %d", c.scheme)
	}

	decoded := c.DecodeValues()
	if !reflect.DeepEqual(decoded, values) {
		t.Errorf("Expected %v, got %v", values, decoded)
	}
}

func TestConsistencyAfterMultipleOperations(t *testing.T) {
	c := NewCompressedUint64List()
	var expected []uint64

	// Perform many mixed operations
	operations := []struct {
		isAppend bool
		values   []uint64
	}{
		{false, []uint64{1, 2, 3}},
		{true, []uint64{256}},
		{false, []uint64{4, 5}},
		{true, []uint64{65536}},
		{false, []uint64{6, 7, 8, 9}},
		{true, []uint64{16777216}},
		{false, []uint64{10, 11}},
	}

	for _, op := range operations {
		if op.isAppend {
			for _, v := range op.values {
				c.Append(v)
				expected = append(expected, v)
			}
		} else {
			c.EncodeValues(op.values)
			expected = append(expected, op.values...)
		}
	}

	// Verify consistency
	if c.Count() != len(expected) {
		t.Errorf("Expected count %d, got %d", len(expected), c.Count())
	}

	decoded := c.DecodeValues()
	if !reflect.DeepEqual(decoded, expected) {
		t.Errorf("Expected %v, got %v", expected, decoded)
	}

	// Test CopyValues consistency
	target := make([]uint64, 0, len(expected))
	copied := c.CopyValues(target)
	if !reflect.DeepEqual(copied, expected) {
		t.Errorf("CopyValues inconsistent: expected %v, got %v", expected, copied)
	}
}

func TestMemoryEfficiency(t *testing.T) {
	c := NewCompressedUint64List()

	// Add many small values (should use scheme 0)
	smallValues := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		smallValues[i] = uint64(i + 1) // 1 to 100
	}

	c.EncodeValues(smallValues)

	if c.scheme != 0 {
		t.Errorf("Expected scheme 0 for small values, got %d", c.scheme)
	}

	// Data should be exactly 100 bytes (1 byte per value)
	if len(c.data) != 100 {
		t.Errorf("Expected 100 bytes of data, got %d", len(c.data))
	}

	// Positions should be empty for scheme 0
	if len(c.positions) != 0 {
		t.Errorf("Expected empty positions for scheme 0, got %d bytes", len(c.positions))
	}

	decoded := c.DecodeValues()
	if !reflect.DeepEqual(decoded, smallValues) {
		t.Errorf("Decoded values don't match original")
	}
}

func TestCornerCaseValues(t *testing.T) {
	tests := []struct {
		name   string
		values []uint64
	}{
		{"Single zero", []uint64{0}},
		{"Multiple zeros", []uint64{0, 0, 0, 0}},
		{"Mixed with zeros", []uint64{0, 100, 0, 256, 0}},
		{"All same value", []uint64{42, 42, 42, 42, 42}},
		{"Descending order", []uint64{1000, 500, 100, 50, 10}},
		{"Random pattern", []uint64{1, 1000, 2, 2000, 3, 3000}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := NewCompressedUint64List()
			c.EncodeValues(test.values)

			decoded := c.DecodeValues()
			if !reflect.DeepEqual(decoded, test.values) {
				t.Errorf("Expected %v, got %v", test.values, decoded)
			}

			if c.Count() != len(test.values) {
				t.Errorf("Expected count %d, got %d", len(test.values), c.Count())
			}
		})
	}
}

// Benchmark tests
func BenchmarkAppend(b *testing.B) {
	c := NewCompressedUint64List()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.Append(uint64(i % 1000))
	}
}

func BenchmarkEncodeValues(b *testing.B) {
	values := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		values[i] = uint64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := NewCompressedUint64List()
		c.EncodeValues(values)
	}
}

func BenchmarkDecodeValues(b *testing.B) {
	c := NewCompressedUint64List()
	values := make([]uint64, 1000)
	for i := 0; i < 1000; i++ {
		values[i] = uint64(i)
	}
	c.EncodeValues(values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.DecodeValues()
	}
}

func BenchmarkCopyValues(b *testing.B) {
	c := NewCompressedUint64List()
	values := make([]uint64, 1000)
	for i := 0; i < 1000; i++ {
		values[i] = uint64(i)
	}
	c.EncodeValues(values)

	target := make([]uint64, 0, 1000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		target = c.CopyValues(target)
	}
}
