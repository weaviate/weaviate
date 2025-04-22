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

package clients

import (
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/transformers"
)

func (v *vectorizer) MetaInfo() (map[string]any, error) {
	endpoint := v.urlBuilder.GetPassageURL("/meta", transformers.VectorizationConfig{})
	return v.client.MetaInfo(endpoint)
}
