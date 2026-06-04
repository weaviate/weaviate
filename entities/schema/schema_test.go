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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUppercaseClassName(t *testing.T) {
	tests := []struct {
		name string
		in   string
		out  string
	}{
		{name: "empty", in: "", out: ""},
		{name: "single lowercase", in: "a", out: "A"},
		{name: "single uppercase", in: "A", out: "A"},
		{name: "already uppercased", in: "Movies", out: "Movies"},
		{name: "lowercase first letter", in: "movies", out: "Movies"},
		{name: "namespaced lowercase class", in: "customer1:movies", out: "customer1:Movies"},
		{name: "namespaced uppercase class", in: "customer1:Movies", out: "customer1:Movies"},
		{name: "namespaced single-char class", in: "customer1:m", out: "customer1:M"},
		{name: "namespaced empty class", in: "customer1:", out: "customer1:"},
		{name: "empty namespace", in: ":Movies", out: ":Movies"},
		{name: "namespace with uppercase prefix preserved verbatim", in: "Customer1:movies", out: "Customer1:Movies"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.out, UppercaseClassName(tt.in))
		})
	}
}

func TestUppercaseClassesNames(t *testing.T) {
	in := []string{"movies", "customer1:movies", "Books"}
	out := UppercaseClassesNames(in...)
	assert.Equal(t, []string{"Movies", "customer1:Movies", "Books"}, out)
}
