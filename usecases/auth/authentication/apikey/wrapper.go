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

package apikey

import (
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/dynamic"
	"github.com/weaviate/weaviate/usecases/config"
)

type ApiKey struct {
	static  *StaticApiKey
	Dynamic *DynamicApiKey
}

func New(cfg config.Config) (*ApiKey, error) {
	static, err := NewStatic(cfg)
	if err != nil {
		return nil, err
	}
	return &ApiKey{
		static:  static,
		Dynamic: NewDynamicApiKey(),
	}, nil
}

func (a *ApiKey) ValidateAndExtract(token string, scopes []string) (*models.Principal, error) {
	if randomKey, userIdentifier, err := dynamic.DecodeApiKey(token); err == nil {
		return a.Dynamic.ValidateAndExtract(randomKey, userIdentifier)
	} else {
		return a.static.ValidateAndExtract(token, scopes)
	}
}
