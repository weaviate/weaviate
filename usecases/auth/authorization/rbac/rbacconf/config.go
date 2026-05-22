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

package rbacconf

import "slices"

// Config makes every subject on the list an admin, whereas everyone else
// has no rights whatsoever
type Config struct {
	Enabled           bool     `json:"enabled" yaml:"enabled"`
	RootUsers         []string `json:"root_users" yaml:"root_users"`
	RootGroups        []string `json:"root_groups" yaml:"root_groups"`
	ReadOnlyGroups    []string `json:"readonly_groups" yaml:"readonly_groups"`
	ViewerUsers       []string `json:"viewer_users" yaml:"viewer_users"`
	AdminUsers        []string `json:"admin_users" yaml:"admin_users"`
	IpInAuditDisabled bool     `json:"ip_in_audit" yaml:"ip_in_audit"`
}

// Validate admin list config for viability, can be called from the central
// config package
func (c Config) Validate() error {
	return nil
}

// IsRoot reports whether a principal with the given username and groups
// would be granted the root role via the static RootUsers/RootGroups
// bindings. username must be the same form casbin sees — i.e. the
// namespace-qualified name on namespace-enabled clusters.
func (c Config) IsRoot(username string, groups []string) bool {
	for _, group := range groups {
		if slices.Contains(c.RootGroups, group) {
			return true
		}
	}
	return slices.Contains(c.RootUsers, username)
}
