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
	"fmt"
	"regexp"
	"strings"
)

const (
	// DefaultNamespace is used when no namespace header is provided.
	DefaultNamespace = "default"

	// NamespaceSeparator separates namespace from class name in internal storage.
	NamespaceSeparator = "__"

	// HeaderName is the HTTP header used to specify the namespace.
	HeaderName = "X-Weaviate-Namespace"

	// MaxNamespaceLength is the maximum allowed length for a namespace name.
	MaxNamespaceLength = 64
)

// namespaceContextKey is the key type for storing namespace in context.
type namespaceContextKey struct{}

// validNamespaceRegex validates namespace names:
// - starts with a lowercase letter
// - contains only lowercase alphanumeric characters
var validNamespaceRegex = regexp.MustCompile(`^[a-z][a-z0-9]*$`)

// ValidateNamespace validates that a namespace name is valid.
// Valid namespaces:
//   - start with a lowercase letter
//   - contain only lowercase alphanumeric characters
//   - are at most MaxNamespaceLength characters
func ValidateNamespace(ns string) error {
	if ns == "" {
		return fmt.Errorf("namespace cannot be empty")
	}
	if len(ns) > MaxNamespaceLength {
		return fmt.Errorf("namespace cannot exceed %d characters", MaxNamespaceLength)
	}
	if !validNamespaceRegex.MatchString(ns) {
		return fmt.Errorf("namespace must start with a lowercase letter and contain only lowercase alphanumeric characters")
	}
	return nil
}

// PrefixClassName prefixes a class name with the namespace.
// Example: PrefixClassName("myapp", "Article") returns "myapp__Article"
func PrefixClassName(namespace, className string) string {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	return namespace + NamespaceSeparator + className
}

// StripNamespacePrefix removes the namespace prefix from a prefixed class name.
// Example: StripNamespacePrefix("myapp__Article") returns "Article"
// If no separator is found, returns the original string unchanged.
func StripNamespacePrefix(prefixedClass string) string {
	idx := strings.Index(prefixedClass, NamespaceSeparator)
	if idx == -1 {
		return prefixedClass
	}
	return prefixedClass[idx+len(NamespaceSeparator):]
}

// ExtractNamespace extracts the namespace from a prefixed class name.
// Example: ExtractNamespace("myapp__Article") returns "myapp"
// If no separator is found, returns DefaultNamespace.
func ExtractNamespace(prefixedClass string) string {
	idx := strings.Index(prefixedClass, NamespaceSeparator)
	if idx == -1 {
		return DefaultNamespace
	}
	return prefixedClass[:idx]
}

// HasNamespacePrefix checks if a class name has a namespace prefix.
func HasNamespacePrefix(className string) bool {
	return strings.Contains(className, NamespaceSeparator)
}

// BelongsToNamespace checks if a prefixed class name belongs to the given namespace.
// Note: Uses case-insensitive comparison because Weaviate capitalizes the first letter
// of class names, which includes the namespace prefix when stored internally.
func BelongsToNamespace(prefixedClass, namespace string) bool {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	prefix := namespace + NamespaceSeparator
	return strings.HasPrefix(strings.ToLower(prefixedClass), strings.ToLower(prefix))
}

// WithNamespace returns a new context with the namespace stored.
func WithNamespace(ctx context.Context, namespace string) context.Context {
	return context.WithValue(ctx, namespaceContextKey{}, namespace)
}

// FromContext extracts the namespace from context.
// Returns DefaultNamespace if not found.
func FromContext(ctx context.Context) string {
	if ns, ok := ctx.Value(namespaceContextKey{}).(string); ok && ns != "" {
		return ns
	}
	return DefaultNamespace
}
