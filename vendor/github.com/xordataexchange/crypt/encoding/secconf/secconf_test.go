package secconf

import (
	"bytes"
	"testing"
)

var encodingTests = []struct {
	in, out string
}{
	{"secret", "secret"},
}

func TestEncoding(t *testing.T) {
	for _, tt := range encodingTests {
		encoded, err := Encode([]byte(tt.in), bytes.NewBufferString(pubring))
		if err != nil {
			t.Errorf(err.Error())
		}
		decoded, err := Decode(encoded, bytes.NewBufferString(secring))
		if err != nil {
			t.Errorf(err.Error())
		}
		if tt.out != string(decoded) {
			t.Errorf("want %s, got %s", tt.out, decoded)
		}
	}
}
