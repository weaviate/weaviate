package rest

import (
	"testing"
)

func TestParseMemLimit_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{
			name:    "valid GiB",
			input:   "2GiB",
			want:    2 * 1024 * 1024 * 1024,
			wantErr: false,
		},
		{
			name:    "valid MiB",
			input:   "512MiB",
			want:    512 * 1024 * 1024,
			wantErr: false,
		},
		{
			name:    "valid bytes",
			input:   "1048576",
			want:    1048576,
			wantErr: false,
		},
		{
			name:    "invalid format",
			input:   "invalid",
			want:    0,
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMemLimit(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseMemLimit() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parseMemLimit() = %v, want %v", got, tt.want)
			}
		})
	}
}
