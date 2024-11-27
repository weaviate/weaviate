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

package rbac

import "fmt"

// Config makes every subject on the list an admin, whereas everyone else
// has no rights whatsoever
type Config struct {
	Enabled  bool     `json:"enabled" yaml:"enabled"`
	AllUsers []string `json:"all_users" yaml:"all_users"`
	Viewers  []string `json:"viewers" yaml:"viewers"`
	Admins   []string `json:"admins" yaml:"admins"`
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
	if len(c.AllUsers) == 0 {
		return fmt.Errorf("at least one user is required")
	}
	if len(c.Admins) == 0 {
		return fmt.Errorf("at least one admin is required")
	}

viewerLoop:
	for _, a := range c.Viewers {
		for _, b := range c.Admins {
			if a == b {
				return fmt.Errorf("rbac: subject '%s' is present on both admin and viewer list", a)
			}
		}

		for _, b := range c.AllUsers {
			if a == b {
				continue viewerLoop
			}
		}
		return fmt.Errorf("rbac: viewer '%s' is not present as a user", a)
	}

adminLoop:
	for _, a := range c.Admins {
		for _, b := range c.AllUsers {
			if a == b {
				continue adminLoop
			}
		}
		return fmt.Errorf("rbac: admin '%s' is not present as a user", a)
	}

	return nil
}
