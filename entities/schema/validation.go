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
	"fmt"
	"regexp"
)

var (
	validateClassNameRegex          = regexp.MustCompile(`^` + ClassNameRegexCore + `$`)
	validateQualifiedClassNameRegex = regexp.MustCompile(`^` + IndexNameRegexCore + `$`)
	validateTenantNameRegex         = regexp.MustCompile(`^` + ShardNameRegexCore + `$`)
	validatePropertyNameRegex       = regexp.MustCompile(`^` + PropertyNameRegex + `$`)
	validateNestedPropertyNameRegex = regexp.MustCompile(`^` + NestedPropertyNameRegex + `$`)
	validateNamespaceNameRegex      = regexp.MustCompile(`^` + NamespaceNameRegexCore + `$`)
	reservedPropertyNames           = []string{"_additional", "_id", "id"}

	// NamespaceNameRegexCore is the single source of truth for the namespace
	// name character set + length contract: lowercase letter/digit edges,
	// hyphens only internally, total length in [NamespaceMinLength,
	// NamespaceMaxLength]. The {N,M} middle bound subtracts 2 to account for
	// the required leading and trailing [a-z0-9] characters.
	//
	// Used both by the anchored validateNamespaceNameRegex (callers that
	// receive a bare namespace name) and by IndexNameRegexCore (the
	// cluster-API URL router, which needs the same syntax embedded in a
	// larger pattern). Do not duplicate this regex elsewhere — extend or
	// reuse this constant so the contract stays in one place.
	NamespaceNameRegexCore = fmt.Sprintf(`[a-z0-9][a-z0-9-]{%d,%d}[a-z0-9]`,
		NamespaceMinLength-2, NamespaceMaxLength-2)

	// IndexNameRegexCore matches an internal index name: an optional
	// "<namespace>:" prefix followed by a ClassNameRegexCore class name.
	//
	// Used by cluster-API URL routing and by ValidateQualifiedClassName
	// at post-resolver boundaries (filter parser). User-facing class-name
	// validation continues to use ClassNameRegexCore (which rejects ":").
	IndexNameRegexCore = `(?:` + NamespaceNameRegexCore + NamespaceSeparator + `)?` + ClassNameRegexCore
)

const (
	// ClassNameMaxLength is restricted by the max length allowed for a
	// directory name (255 chars). As the dir containing class data is named
	// after the class, 255 chars are allowed.
	ClassNameMaxLength = 255
	ClassNameRegexCore = `[A-Z][_0-9A-Za-z]{0,254}`
	// ClassNameRegexAllowRegex allowed chars in class name including regex patterns, 255 chars are allowed
	ClassNameRegexAllowRegex = `^(\*|[A-Z][_0-9A-Za-z\-.*+?^$()|{}\[\]\\]{0,254})$`
	// ShardNameRegexCore allowed chars in shard name, 64 chars are allowed
	ShardNameRegexCore = `[A-Za-z0-9\-\_]{1,64}`
	// ShardNameRegexAllowRegex allowed chars in shard name including regex patterns, 64 chars are allowed
	ShardNameRegexAllowRegex = `^[A-Za-z0-9\-_.*+?^$()|{}\[\]\\*]{1,64}$`
	// Restricted by max length allowed for dir name (255 chars)
	// Property name is used to build dir names of various purposes containing property
	// related data. Among them might be (depending on the settings):
	// - geo.{property_name}.hnsw.commitlog.d
	// - property_{property_name}__meta_count
	// - property_{property_name}_propertyLength
	// Last one seems to add the most additional characters (24) to property name,
	// therefore poperty max lentgh should not exceed 255 - 24 = 231 chars.
	propertyNameMaxLength = 231
	PropertyNameRegex     = `[_A-Za-z][_0-9A-Za-z]{0,230}`
	// Nested properties names are not used to build directory names (yet),
	// no max length restriction is imposed
	NestedPropertyNameRegex = `[_A-Za-z][_0-9A-Za-z]*`
	// Target vector names must be GraphQL compliant names no longer then 230 characters
	TargetVectorNameMaxLength = 230
	TargetVectorNameRegex     = `[_A-Za-z][_0-9A-Za-z]{0,229}`

	// NamespaceSeparator is the reserved character used to qualify class names
	// with their owning namespace ("<namespace>:<ClassName>"). Plain class names
	// must NOT contain this character — the ClassNameRegexCore character class
	// [_0-9A-Za-z] excludes it, so any name passing ValidateClassName cannot
	// contain one.
	//
	// The namespace startup invariant and the name resolver both depend on
	// this contract. If ClassNameRegexCore is ever loosened to allow ":",
	// audit all consumers of NamespaceSeparator and update them atomically.
	// TestValidateClassName_RejectsNamespaceSeparator locks the contract.
	NamespaceSeparator = ":"

	// Namespace name validation contract (kept tight so the operator API gives
	// predictable results and so name-in-URL round-tripping is unambiguous):
	//
	//   - Contains only lowercase ASCII letters, digits, and hyphens.
	//   - Must start and end with a lowercase letter or digit (no leading or
	//     trailing hyphens).
	//   - Length in [NamespaceMinLength, NamespaceMaxLength].
	//   - Must not collide with a reserved name.
	//
	// The regex and reserved-name list live in usecases/namespaces alongside
	// ValidateName; only the length bounds are shared here so both the
	// namespace controller and the syntactic qualification helpers
	// (usecases/schema/namespacing) read from a single source of truth.
	// Reserved names are held back for platform/system use (e.g. a future
	// "default" namespace or routing sentinels) and are refused at Create time.
	NamespaceMinLength = 3
	NamespaceMaxLength = 36
)

// ValidateNamespaceNameSyntax checks the syntax of a namespace name: length
// bounds, lowercase ASCII letters/digits/hyphens, no leading/trailing hyphen.
// It does NOT check reserved names — that policy lives in usecases/namespaces
// alongside ValidateName and is enforced at Create time. This split lets the
// name-resolver (usecases/schema/namespacing) reuse the syntactic part without
// importing the namespace controller (which would form a cycle via the apikey
// dependency chain).
func ValidateNamespaceNameSyntax(name string) error {
	if l := len(name); l < NamespaceMinLength || l > NamespaceMaxLength {
		return fmt.Errorf("namespace name %q must be %d-%d characters", name, NamespaceMinLength, NamespaceMaxLength)
	}
	if !validateNamespaceNameRegex.MatchString(name) {
		return fmt.Errorf("namespace name %q must contain only lowercase letters, digits, and hyphens, must start and end with a letter or digit, and must not contain ':'", name)
	}
	return nil
}

// ValidateClassName validates that this string is a valid class name (format wise)
func ValidateClassName(name string) (ClassName, error) {
	c, err := validateClassOrAliasName(name, false)
	if err != nil {
		return "", err
	}
	return ClassName(c), nil
}

// ValidateQualifiedClassName validates that name is either a plain class
// name matching ClassNameRegexCore, or a namespace-qualified name
// "<namespace>:<Class>" where the namespace portion matches the shared
// namespace-name contract (lowercase ASCII letter/digit edges, hyphens
// only internally, length NamespaceMinLength..NamespaceMaxLength) and the
// class portion matches ClassNameRegexCore. The full qualified name is
// returned unchanged.
//
// Use at post-resolver boundaries (e.g. the filter parser) where a
// qualified class name is a legitimate input that has already been
// produced by namespacing.Resolve. User-facing inputs (schema create,
// alias create, cross-ref data types) must continue to call
// ValidateClassName, which rejects ":".
func ValidateQualifiedClassName(name string) (ClassName, error) {
	if len(name) > NamespaceMaxLength+len(NamespaceSeparator)+ClassNameMaxLength {
		return "", fmt.Errorf("'%s' is not a valid class name", name)
	}
	if !validateQualifiedClassNameRegex.MatchString(name) {
		return "", fmt.Errorf("'%s' is not a valid class name", name)
	}
	return ClassName(name), nil
}

func ValidateAliasName(name string) (string, error) {
	return validateClassOrAliasName(name, true)
}

// ValidateClassNameIncludesRegex validates that this string is a valid class name (format wise)
// can include regex pattern
func ValidateClassNameIncludesRegex(name string) (ClassName, error) {
	if len(name) > ClassNameMaxLength {
		return "", fmt.Errorf("'%s' is not a valid class name. Name should not be longer than %d characters",
			name, ClassNameMaxLength)
	}
	if !regexp.MustCompile(ClassNameRegexAllowRegex).MatchString(name) {
		return "", fmt.Errorf("'%s' is not a valid class name", name)
	}
	return ClassName(name), nil
}

func validateClassOrAliasName(name string, isAlias bool) (string, error) {
	typ := "class"
	if isAlias {
		typ = "alias"
	}

	if len(name) > ClassNameMaxLength {
		return "", fmt.Errorf("'%s' is not a valid %s name. Name should not be longer than %d characters",
			name, typ, ClassNameMaxLength)
	}
	if !validateClassNameRegex.MatchString(name) {
		return "", fmt.Errorf("'%s' is not a valid %s name", name, typ)
	}
	return name, nil
}

// ValidateTenantName validates that this string is a valid tenant name (format wise)
func ValidateTenantName(name string) error {
	if !validateTenantNameRegex.MatchString(name) {
		var msg string
		if name == "" {
			msg = "empty tenant name"
		} else {
			msg = fmt.Sprintf(
				" '%s' is not a valid tenant name. should only contain alphanumeric characters (a-z, A-Z, 0-9), "+
					"underscore (_), and hyphen (-), with a length between 1 and 64 characters",
				name,
			)
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// ValidateTenantNameIncludesRegex validates that this string is a valid tenant name (format wise)
// can include regex pattern
func ValidateTenantNameIncludesRegex(name string) error {
	if !regexp.MustCompile(ShardNameRegexAllowRegex).MatchString(name) {
		var msg string
		if name == "" {
			msg = "empty tenant name"
		} else {
			msg = fmt.Sprintf(
				" '%s' is not a valid tenant name. should only contain alphanumeric characters (a-z, A-Z, 0-9), "+
					"underscore (_), and hyphen (-), with a length between 1 and 64 characters",
				name,
			)
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// ValidatePropertyName validates that this string is a valid property name
func ValidatePropertyName(name string) (PropertyName, error) {
	if len(name) > propertyNameMaxLength {
		return "", fmt.Errorf("'%s' is not a valid property name. Name should not be longer than %d characters",
			name, propertyNameMaxLength)
	}
	if !validatePropertyNameRegex.MatchString(name) {
		return "", fmt.Errorf("'%s' is not a valid property name. "+
			"Property names in Weaviate are restricted to valid GraphQL names, "+
			"which must be “/%s/”", name, PropertyNameRegex)
	}
	return PropertyName(name), nil
}

// ValidateNestedPropertyName validates that this string is a valid nested property name
func ValidateNestedPropertyName(name, prefix string) error {
	if !validateNestedPropertyNameRegex.MatchString(name) {
		return fmt.Errorf("'%s' is not a valid nested property name of '%s'. "+
			"NestedProperty names in Weaviate are restricted to valid GraphQL names, "+
			"which must be “/%s/”", name, prefix, NestedPropertyNameRegex)
	}
	return nil
}

// ValidateReservedPropertyName validates that a string is not a reserved property name
func ValidateReservedPropertyName(name string) error {
	for i := range reservedPropertyNames {
		if name == reservedPropertyNames[i] {
			return fmt.Errorf("'%s' is a reserved property name", name)
		}
	}
	return nil
}

// AssertValidClassName assert that this string is a valid class name or
// panics and should therefore most likely not be used
func AssertValidClassName(name string) ClassName {
	n, err := ValidateClassName(name)
	if err != nil {
		panic(fmt.Sprintf("Did not expect to be handled '%s', an invalid class name", name))
	}
	return n
}

// AssertValidPropertyName asserts that this string is a valid property name or
// panics and should therefore most likely never be used.
func AssertValidPropertyName(name string) PropertyName {
	n, err := ValidatePropertyName(name)
	if err != nil {
		panic(fmt.Sprintf("Did not expect to be handled '%s', an invalid property name", name))
	}
	return n
}
