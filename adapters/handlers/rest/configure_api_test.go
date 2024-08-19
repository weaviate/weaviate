//go:build linux
// +build linux

package rest

import (
	"os"
	"testing"
)


func TestGetCores(t *testing.T) {
	tests := []struct {
		name     string
		cpuset   string
		expected int
		wantErr  bool
	}{
		{"Single core", "0", 1, false},
		{"Multiple cores", "0,1,2,3", 4, false},
		{"Range of cores", "0-3", 4, false},
		{"Multiple ranges", "0-3,5-7", 7, false},
		{"Mixed format", "0-2,4,6-7", 6, false},
		{"Mixed format 2", "0,2-4,7", 5, false},
		{"Empty cpuset", "", 0, false},
		{"Invalid format", "0-2-4", 0, true},
		{"Non-numeric", "a-b", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary file with the test cpuset content
			tmpfile, err := os.CreateTemp("", "cpuset")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(tmpfile.Name())

			if _, err := tmpfile.Write([]byte(tt.cpuset)); err != nil {
				t.Fatal(err)
			}
			if err := tmpfile.Close(); err != nil {
				t.Fatal(err)
			}

			// Temporarily replace the cpuset file path
			originalPath := "/sys/fs/cgroup/cpuset/cpuset.cpus"
			if err := os.Rename(originalPath, originalPath+".bak"); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			defer os.Rename(originalPath+".bak", originalPath)

			if err := os.Symlink(tmpfile.Name(), originalPath); err != nil {
				t.Fatal(err)
			}
			defer os.Remove(originalPath)

			// Run the test
			got, err := getCores()
			if (err != nil) != tt.wantErr {
				t.Errorf("getCores() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("getCores() = %v, want %v", got, tt.expected)
			}
		})
	}
}