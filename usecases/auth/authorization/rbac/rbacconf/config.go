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

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

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
	// Checking a user's "oidc:<name>" row also covers the "db:<name>" row
	// applyPredefinedRoles saves for a user with an API key. The rows differ
	// only in that prefix, and "db:" is the shorter one.
	user := func(name string) string { return conv.UserNameWithTypeFromId(name, authentication.AuthTypeOIDC) }
	group := conv.PrefixGroupName
	return errors.Join(
		validateGrants("root_users", c.RootUsers, user, authorization.Root),
		validateGrants("admin_users", c.AdminUsers, user, authorization.Admin),
		validateGrants("viewer_users", c.ViewerUsers, user, authorization.Viewer),
		validateGrants("root_groups", c.RootGroups, group, authorization.Root),
		validateGrants("readonly_groups", c.ReadOnlyGroups, group, authorization.ReadOnly),
	)
}

// validateGrants rejects an entry whose policy.csv row would fail to load or
// give a subject a role the config does not. A row that fails to load stops
// every boot, since Init loads the file before applyPredefinedRoles rewrites it.
func validateGrants(setting string, entries []string, subject func(string) string, role string) error {
	for _, entry := range entries {
		// applyPredefinedRoles skips blank entries, so they never reach the file.
		if strings.TrimSpace(entry) == "" {
			continue
		}
		if err := conv.ValidateStorableRow("g", subject(entry), conv.PrefixRoleName(role)); err != nil {
			return fmt.Errorf("%s: %w", setting, err)
		}
	}
	return nil
}

// IsRootUser reports whether a principal with the given username and groups
// would be granted the root role via the static RootUsers/RootGroups
// bindings. username must be the same form casbin sees — i.e. the
// namespace-qualified name on namespace-enabled clusters.
func (c Config) IsRootUser(username string, groups []string) bool {
	for _, group := range groups {
		if slices.Contains(c.RootGroups, group) {
			return true
		}
	}
	return slices.Contains(c.RootUsers, username)
}
