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

package namespace

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateNamespace(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		wantErr   bool
		errMsg    string
	}{
		{
			name:      "valid simple namespace",
			namespace: "myapp",
			wantErr:   false,
		},
		{
			name:      "valid namespace with numbers",
			namespace: "app123",
			wantErr:   false,
		},
		{
			name:      "valid default namespace",
			namespace: "default",
			wantErr:   false,
		},
		{
			name:      "valid single letter",
			namespace: "a",
			wantErr:   false,
		},
		{
			name:      "empty namespace",
			namespace: "",
			wantErr:   true,
			errMsg:    "namespace cannot be empty",
		},
		{
			name:      "starts with number",
			namespace: "1app",
			wantErr:   true,
			errMsg:    "must start with a lowercase letter",
		},
		{
			name:      "starts with uppercase",
			namespace: "Myapp",
			wantErr:   true,
			errMsg:    "must start with a lowercase letter",
		},
		{
			name:      "contains uppercase",
			namespace: "myApp",
			wantErr:   true,
			errMsg:    "must start with a lowercase letter",
		},
		{
			name:      "contains underscore",
			namespace: "my_app",
			wantErr:   true,
			errMsg:    "must start with a lowercase letter",
		},
		{
			name:      "contains hyphen",
			namespace: "my-app",
			wantErr:   true,
			errMsg:    "must start with a lowercase letter",
		},
		{
			name:      "contains space",
			namespace: "my app",
			wantErr:   true,
			errMsg:    "must start with a lowercase letter",
		},
		{
			name:      "too long",
			namespace: strings.Repeat("a", MaxNamespaceLength+1),
			wantErr:   true,
			errMsg:    "cannot exceed",
		},
		{
			name:      "exactly max length",
			namespace: strings.Repeat("a", MaxNamespaceLength),
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateNamespace(tt.namespace)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPrefixClassName(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		className string
		want      string
	}{
		{
			name:      "standard prefix",
			namespace: "myapp",
			className: "Article",
			want:      "myapp__Article",
		},
		{
			name:      "default namespace",
			namespace: "default",
			className: "Article",
			want:      "default__Article",
		},
		{
			name:      "empty namespace uses default",
			namespace: "",
			className: "Article",
			want:      "default__Article",
		},
		{
			name:      "class with underscore",
			namespace: "myapp",
			className: "My_Class",
			want:      "myapp__My_Class",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PrefixClassName(tt.namespace, tt.className)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStripNamespacePrefix(t *testing.T) {
	tests := []struct {
		name          string
		prefixedClass string
		want          string
	}{
		{
			name:          "standard strip",
			prefixedClass: "myapp__Article",
			want:          "Article",
		},
		{
			name:          "default namespace",
			prefixedClass: "default__Article",
			want:          "Article",
		},
		{
			name:          "no prefix returns unchanged",
			prefixedClass: "Article",
			want:          "Article",
		},
		{
			name:          "class with single underscore",
			prefixedClass: "myapp__My_Class",
			want:          "My_Class",
		},
		{
			name:          "multiple separators takes first",
			prefixedClass: "ns__Class__Extra",
			want:          "Class__Extra",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StripNamespacePrefix(tt.prefixedClass)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractNamespace(t *testing.T) {
	tests := []struct {
		name          string
		prefixedClass string
		want          string
	}{
		{
			name:          "standard extract",
			prefixedClass: "myapp__Article",
			want:          "myapp",
		},
		{
			name:          "default namespace",
			prefixedClass: "default__Article",
			want:          "default",
		},
		{
			name:          "no prefix returns default",
			prefixedClass: "Article",
			want:          "default",
		},
		{
			name:          "multiple separators takes first",
			prefixedClass: "ns__Class__Extra",
			want:          "ns",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractNamespace(tt.prefixedClass)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHasNamespacePrefix(t *testing.T) {
	tests := []struct {
		name      string
		className string
		want      bool
	}{
		{
			name:      "has prefix",
			className: "myapp__Article",
			want:      true,
		},
		{
			name:      "no prefix",
			className: "Article",
			want:      false,
		},
		{
			name:      "single underscore is not separator",
			className: "My_Class",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HasNamespacePrefix(tt.className)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBelongsToNamespace(t *testing.T) {
	tests := []struct {
		name          string
		prefixedClass string
		namespace     string
		want          bool
	}{
		{
			name:          "belongs to namespace",
			prefixedClass: "myapp__Article",
			namespace:     "myapp",
			want:          true,
		},
		{
			name:          "does not belong",
			prefixedClass: "myapp__Article",
			namespace:     "otherapp",
			want:          false,
		},
		{
			name:          "empty namespace checks default",
			prefixedClass: "default__Article",
			namespace:     "",
			want:          true,
		},
		{
			name:          "no prefix does not belong",
			prefixedClass: "Article",
			namespace:     "myapp",
			want:          false,
		},
		{
			name:          "partial match is not match",
			prefixedClass: "myapp__Article",
			namespace:     "my",
			want:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BelongsToNamespace(tt.prefixedClass, tt.namespace)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestContextFunctions(t *testing.T) {
	t.Run("WithNamespace and FromContext", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithNamespace(ctx, "myapp")

		got := FromContext(ctx)
		assert.Equal(t, "myapp", got)
	})

	t.Run("FromContext returns default when not set", func(t *testing.T) {
		ctx := context.Background()

		got := FromContext(ctx)
		assert.Equal(t, DefaultNamespace, got)
	})

	t.Run("FromContext returns default for empty string", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithNamespace(ctx, "")

		got := FromContext(ctx)
		assert.Equal(t, DefaultNamespace, got)
	})
}

func TestRoundTrip(t *testing.T) {
	// Test that prefix and strip are inverse operations
	testCases := []struct {
		namespace string
		className string
	}{
		{"myapp", "Article"},
		{"default", "MyClass"},
		{"test123", "Some_Class"},
	}

	for _, tc := range testCases {
		t.Run(tc.namespace+"_"+tc.className, func(t *testing.T) {
			prefixed := PrefixClassName(tc.namespace, tc.className)
			stripped := StripNamespacePrefix(prefixed)
			extracted := ExtractNamespace(prefixed)

			assert.Equal(t, tc.className, stripped, "round-trip should preserve class name")
			assert.Equal(t, tc.namespace, extracted, "should extract correct namespace")
			assert.True(t, BelongsToNamespace(prefixed, tc.namespace), "should belong to namespace")
		})
	}
}
