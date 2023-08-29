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

type schema struct {
	txHandler
}

func NewSchema(manager txManager, authConfig cluster.AuthConfig) *schema {
	return &schema{txHandler{manager: manager, auth: newBasicAuthHandler(authConfig)}}
}
