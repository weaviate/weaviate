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

	"github.com/weaviate/weaviate/modules/generative-digitalocean/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/digitalocean"
)

// init registers the default model lister with the config package so that
// ValidateClass can call out to DigitalOcean without config depending on this
// package. The timeout is short because the model check only warns, and it
// also runs during backup restore.
func init() {
	config.DefaultModelLister = digitalocean.NewModelLister(5 * time.Second)
}
