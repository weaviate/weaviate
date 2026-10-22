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

package client

import (
	"time"

	"github.com/weaviate/weaviate/modules/reranker-digitalocean/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/digitalocean"
)

func init() {
	config.DefaultModelLister = digitalocean.NewModelLister(30 * time.Second)
}
