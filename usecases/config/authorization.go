/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package config

// Authorization configuration
type Authorization struct {
	AdminList AdminList `json:"admin_list" yaml:"admin_list"`
}

// Validate the Authorization configuration. This only validates at a general
// level. Validation specific to the individual auth methods should happen
// inside their respective packages
func (a Authorization) Validate() error {
	return nil
}

// AdminList makes every subject on the list an admin, whereas everyone else
// has no rights whatsoever
type AdminList struct {
	Enabled bool     `json:"enabled" yaml:"enabled"`
	Users   []string `json:"users" yaml:"users"`
}
