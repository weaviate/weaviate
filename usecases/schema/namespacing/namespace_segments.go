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

package namespacing

import (
	"strings"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// Authorization resource-path prefixes whose name segment is namespace-bearing.
const (
	SchemaCollectionsPrefix  = authorization.SchemaDomain + "/collections/"
	DataCollectionsPrefix    = authorization.DataDomain + "/collections/"
	AliasesCollectionsPrefix = authorization.AliasesDomain + "/collections/"
	UsersPrefix              = authorization.UsersDomain + "/"
	RolesPrefix              = authorization.RolesDomain + "/"

	ShardsMidSeg  = "/shards/"
	AliasesMidSeg = "/aliases/"
)

// FindNamespaceSegments returns the [start, end) bounds of the collection-name
// segment in path for the known shapes (schema/data/aliases). end == 0 means
// path is not namespaceable. hasAlias reports whether path also has a 2nd
// namespace-bearing alias segment, located at
// [end + len(AliasesMidSeg), len(path)).
func FindNamespaceSegments(path string) (start, end int, hasAlias bool) {
	if rest, ok := strings.CutPrefix(path, SchemaCollectionsPrefix); ok {
		idx := strings.Index(rest, ShardsMidSeg)
		if idx == -1 {
			return 0, 0, false
		}
		s := len(SchemaCollectionsPrefix)
		return s, s + idx, false
	}
	if rest, ok := strings.CutPrefix(path, DataCollectionsPrefix); ok {
		idx := strings.Index(rest, ShardsMidSeg)
		if idx == -1 {
			return 0, 0, false
		}
		s := len(DataCollectionsPrefix)
		return s, s + idx, false
	}
	if rest, ok := strings.CutPrefix(path, AliasesCollectionsPrefix); ok {
		idx := strings.Index(rest, AliasesMidSeg)
		if idx == -1 {
			return 0, 0, false
		}
		s := len(AliasesCollectionsPrefix)
		return s, s + idx, true
	}
	// users/<id> is terminal — the id runs to end of string (no /shards/ or
	// /aliases/ delimiter like the collection shapes above).
	if _, ok := strings.CutPrefix(path, UsersPrefix); ok {
		return len(UsersPrefix), len(path), false
	}
	// roles/<id> is terminal, mirroring users/<id>.
	if _, ok := strings.CutPrefix(path, RolesPrefix); ok {
		return len(RolesPrefix), len(path), false
	}
	// groups/ is intentionally not registered: a colon-bearing group id is
	// matched literally. Don't add it without also short-circuiting groups/
	// in globalCallerNeedsNoWidening, or such ids would wrongly widen.
	return 0, 0, false
}

// SegmentHasSeparator reports whether path[start:end] contains the namespace separator.
func SegmentHasSeparator(path string, start, end int) bool {
	return strings.IndexByte(path[start:end], schema.NamespaceSeparator[0]) >= 0
}
