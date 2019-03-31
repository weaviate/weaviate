/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package feature_flags

import (
	"fmt"
	"os"
)

// Flags and their default value
var (
	EnableGetMeta bool = false
)

var flags = []struct {
	toggleVar *bool
	envVar    string
}{
	{
		toggleVar: &EnableGetMeta,
		envVar:    "FEATURE_GET_META",
	},
}

func init() {
	for _, flag := range flags {
		val, present := os.LookupEnv(flag.envVar)
		if present {
			if val == "on" || val == "ON" {
				*flag.toggleVar = true
				continue
			}
			if val == "off" || val == "OFF" {
				*flag.toggleVar = false
				continue
			}
			panic(fmt.Sprintf("Expected feature toggle %s to be set to 'on' or 'off', not '%s'", flag.envVar, val))
		}
	}
}
