//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package vectorizer

import c "github.com/weaviate/weaviate/modules/text2vec-kserve/clients"

type Vectorizer struct {
	client c.Client
}

func New(client c.Client) *Vectorizer {
	return &Vectorizer{
		client: client,
	}
}
