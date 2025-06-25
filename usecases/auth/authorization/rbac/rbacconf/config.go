//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rbacconf

// Config makes every subject on the list an admin, whereas everyone else
// has no rights whatsoever
type Config struct {
	Enabled           bool     `json:"enabled" yaml:"enabled"`
	RootUsers         []string `json:"root_users" yaml:"root_users"`
	RootGroups        []string `json:"root_groups" yaml:"root_groups"`
	ViewerGroups      []string `json:"readonly_groups" yaml:"readonly_groups"`
	ViewerUsers       []string `json:"viewer_users" yaml:"viewer_users"`
	AdminUsers        []string `json:"admin_users" yaml:"admin_users"`
	IpInAuditDisabled bool     `json:"ip_in_audit" yaml:"ip_in_audit"`
}

// Validate admin list config for viability, can be called from the central
// config package
func (c Config) Validate() error {
	return nil
}
