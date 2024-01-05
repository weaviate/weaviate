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

package adminlist

import "fmt"

// Config makes every subject on the list an admin, whereas everyone else
// has no rights whatsoever
type Config struct {
	Enabled        bool     `json:"enabled" yaml:"enabled"`
	Users          []string `json:"users" yaml:"users"`
	ReadOnlyUsers  []string `json:"read_only_users" yaml:"read_only_users"`
	Groups         []string `json:"groups" yaml:"groups"`
	ReadOnlyGroups []string `json:"read_only_groups" yaml:"read_only_groups"`
}

// Validate admin list config for viability, can be called from the central
// config package
func (c Config) Validate() error {
	return c.validateOverlap()
}

// we are expecting both lists to always contain few subjects and know that
// this comparison is only done once (at startup). We are therefore fine with
// the O(n^2) complexity of this very primitive overlap search in favor of very
// simple code.
func (c Config) validateOverlap() error {
	for _, a := range c.Users {
		for _, b := range c.ReadOnlyUsers {
			if a == b {
				return fmt.Errorf("admin list: subject '%s' is present on both admin and read-only list", a)
			}
		}
	}
	for _, a := range c.Groups {
		for _, b := range c.ReadOnlyGroups {
			if a == b {
				return fmt.Errorf("admin list: subject '%s' is present on both admin and read-only list", a)
			}
		}
	}

	return nil
}
