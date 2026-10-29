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

package clients

import (
	"time"

	"github.com/weaviate/weaviate/modules/text2vec-digitalocean/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/digitalocean"
)

// init registers the default model lister with the ent package so that
// ValidateClass can call out to DigitalOcean without ent depending on this
// package.
func init() {
	ent.DefaultModelLister = digitalocean.NewModelLister(30 * time.Second)
}
