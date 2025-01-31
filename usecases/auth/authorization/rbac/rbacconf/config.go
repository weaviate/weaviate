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

import "fmt"

// Config makes every subject on the list an admin, whereas everyone else
// has no rights whatsoever
type Config struct {
	Enabled    bool     `json:"enabled" yaml:"enabled"`
	RootUsers  []string `json:"admins" yaml:"admins"`
	RootGroups []string `json:"rootGroups" yaml:"rootGroups"`
}

// Validate admin list config for viability, can be called from the central
// config package
func (c Config) Validate() error {
	return c.validateOverlap()
}

func (c Config) validateOverlap() error {
	if len(c.RootUsers) == 0 {
		return fmt.Errorf("at least one root user is required")
	}

	return nil
}
