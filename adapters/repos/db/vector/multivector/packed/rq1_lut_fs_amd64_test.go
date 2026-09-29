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

//go:build !noasm && amd64

package packed

import (
	"reflect"
	"testing"
)

// TestX86Kernels pins the dispatch for every combination of feature flags,
// not only the test machine's: which kernels are listed and that the widest
// one is last. The case
// that matters is AVX-512 reported without AVX2, where the AVX-512 kernel must
// not be installed because its remainder loop runs AVX2 instructions.
func TestX86Kernels(t *testing.T) {
	tests := []struct {
		name              string
		sse, avx2, avx512 bool
		want              []string
	}{
		{"nothing", false, false, false, nil},
		{"avx without sse4.1", false, true, true, nil},
		{"sse", true, false, false, []string{"sse"}},
		{"avx2", true, true, false, []string{"sse", "avx2"}},
		{"avx512 without avx2", true, false, true, []string{"sse"}},
		{"avx512", true, true, true, []string{"sse", "avx2", "avx512"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := x86Kernels(tt.sse, tt.avx2, tt.avx512)
			var names []string
			for _, v := range got {
				names = append(names, v.name)
			}
			if !reflect.DeepEqual(names, tt.want) {
				t.Fatalf("kernels = %v, want %v", names, tt.want)
			}
		})
	}
}
