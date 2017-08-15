/*
Copyright 2017 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package s2

import "testing"

func TestInterleaveUint32(t *testing.T) {
	x, y := uint32(13356), uint32(1073728367)
	gotX, gotY := deinterleaveUint32(interleaveUint32(x, y))
	if gotX != x || gotY != gotY {
		t.Errorf("deinterleave after interleave = %d, %d; want %d, %d", gotX, gotY, x, y)
	}
}

func referenceBitInterleave(x, y uint32) uint64 {
	var ret uint64
	for i := uint(0); i < 32; i++ {
		ret |= uint64((x>>i)&1) << (i * 2)
		ret |= uint64((y>>i)&1) << (i*2 + 1)
	}
	return ret
}

func TestInterleaveUint32AgainstReference(t *testing.T) {
	// TODO(nsch): Add the remaining parts of the tests later. (the various other primes and bit AND-ings.)
	for i := 0; i < 100000; i++ {
		wantEven := uint32(i) * 14233781
		wantOdd := uint32(i) * 18400439

		gotInter := interleaveUint32(wantEven, wantOdd)
		wantInter := referenceBitInterleave(wantEven, wantOdd)
		if gotInter != wantInter {
			t.Fatalf("interleaveUint32(%d, %d) = %d, want %d", wantEven, wantOdd, gotInter, wantInter)
		}

		gotEven, gotOdd := deinterleaveUint32(gotInter)
		if gotEven != wantEven {
			t.Fatalf("interleave: %d vs %d", gotEven, wantEven)
		}
		if gotOdd != wantOdd {
			t.Fatalf("interleave: %d vs %d", gotOdd, wantOdd)
		}
	}
}

func TestInterleaveBasics(t *testing.T) {
	tests := []struct {
		x, y uint32
		want uint64
	}{
		// Interleave zeroes.
		{0, 0, 0},
		// Interleave ones.
		{1, 0, 1}, {0, 1, 2}, {1, 1, 3},
		// Interleave all bits.
		{0xffffffff, 0, 0x5555555555555555},
		{0, 0xffffffff, 0xaaaaaaaaaaaaaaaa},
		{0xffffffff, 0xffffffff, 0xffffffffffffffff},
	}
	for _, tt := range tests {
		if got := interleaveUint32(tt.x, tt.y); got != tt.want {
			t.Errorf("interleaveUint32(%d, %d) = %d, want %d", tt.x, tt.y, got, tt.want)
		}
	}
}
