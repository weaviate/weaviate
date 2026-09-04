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

package config

// StringSet is parsed from comma-separated env lists and used for membership
// checks, e.g. an allow-list of "<Collection>.<property>" keys.
type StringSet map[string]struct{}

// Has reports whether key is in the set. It is safe to call on a nil set.
func (s StringSet) Has(key string) bool {
	_, ok := s[key]
	return ok
}
