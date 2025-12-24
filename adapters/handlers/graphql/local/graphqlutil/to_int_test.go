package graphqlutil

import (
	"encoding/json"
	"testing"
)

const unexpectedErrFmt = "unexpected error: %v"

func TestToIntVariousInputs(t *testing.T) {
	tests := []struct {
		name      string
		in        interface{}
		want      int
		wantError bool
	}{
		{"int", 5, 5, false},
		{"int32", int32(7), 7, false},
		{"int64", int64(9), 9, false},
		{"int64_out_of_range", int64(1 << 40), 0, true},
		{"float64_integral", float64(10.0), 10, false},
		{"float64_non_integral", float64(4.5), 0, true},
		{"json_number", json.Number("12"), 12, false},
		{"json_number_float_integral", json.Number("12.0"), 12, false},
		{"json_number_float_non_integral", json.Number("12.3"), 0, true},
		{"string_number", "13", 13, false},
		{"string_float_integral", "14.0", 14, false},
		{"string_non_number", "abc", 0, true},
		{"out_of_range", json.Number("2147483648"), 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToInt(tt.in)
			if tt.wantError {
				if err == nil {
					t.Fatalf("expected error for input %v, got nil", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf(unexpectedErrFmt, err)
			}
			if got != tt.want {
				t.Fatalf("wanted %d, got %d", tt.want, got)
			}
		})
	}
}
