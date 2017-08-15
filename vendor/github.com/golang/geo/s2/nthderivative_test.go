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

func TestCoder(t *testing.T) {
	// Fixed testcases
	input := []int32{1, 5, 10, 15, 20, 23}
	order0 := []int32{1, 5, 10, 15, 20, 23}
	order1 := []int32{1, 4, 5, 5, 5, 3}
	order2 := []int32{1, 4, 1, 0, 0, -2}

	enc0, dec0 := newNthDerivativeCoder(0), newNthDerivativeCoder(0)
	enc1, dec1 := newNthDerivativeCoder(1), newNthDerivativeCoder(1)
	enc2, dec2 := newNthDerivativeCoder(2), newNthDerivativeCoder(2)

	for i, in := range input {
		if got := enc0.encode(in); got != order0[i] {
			t.Errorf("enc0 = %d, want %d", got, order0[i])
		}
		if got := dec0.decode(order0[i]); got != in {
			t.Errorf("dec0 = %d, want %d", got, in)
		}
		if got := enc1.encode(in); got != order1[i] {
			t.Errorf("enc1 = %d, want %d", got, order1[i])
		}
		if got := dec1.decode(order1[i]); got != in {
			t.Errorf("dec1 = %d, want %d", got, in)
		}
		if got := enc2.encode(in); got != order2[i] {
			t.Errorf("enc2 = %d, want %d", got, order2[i])
		}
		if got := dec2.decode(order2[i]); got != in {
			t.Errorf("dec2 = %d, want %d", got, in)
		}
	}
}
