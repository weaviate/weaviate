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

package rest

import (
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// namespaceQualifier picks the Qualifier that startupRoutine stores in
// state.State for every class-name resolver call on this node.
func namespaceQualifier(cfg config.Config) namespacing.Qualifier {
	if cfg.Namespaces.Enabled {
		return namespacing.NewPrefixing()
	}
	return namespacing.Disabled
}
