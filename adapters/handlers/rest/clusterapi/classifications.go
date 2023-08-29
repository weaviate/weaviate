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

package clusterapi

import "github.com/weaviate/weaviate/usecases/cluster"

type classifications struct {
	txHandler
}

func NewClassifications(manager txManager, authConfig cluster.AuthConfig) *classifications {
	return &classifications{txHandler{manager: manager, auth: newBasicAuthHandler(authConfig)}}
}
