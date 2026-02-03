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

package apikey

import (
	"fmt"

	"github.com/go-openapi/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"
	"github.com/weaviate/weaviate/usecases/config"
)

type ApiKey struct {
	static  *StaticApiKey
	Dynamic *DBUser
}

func New(cfg config.Config, logger logrus.FieldLogger) (*ApiKey, error) {
	static, err := NewStatic(cfg)
	if err != nil {
		return nil, err
	}
	dynamic, err := NewDBUser(cfg.Persistence.DataPath, cfg.Authentication.DBUsers.Enabled, logger)
	if err != nil {
		return nil, err
	}

	return &ApiKey{
		static:  static,
		Dynamic: dynamic,
	}, nil
}

func (a *ApiKey) ValidateAndExtract(token string, scopes []string) (*authentication.AuthResult, error) {
	validate := func(token string, scopes []string) (*authentication.AuthResult, error) {
		if a.Dynamic.enabled {
			if randomKey, userIdentifier, err := keys.DecodeApiKey(token); err == nil {
				result, err := a.Dynamic.ValidateAndExtract(randomKey, userIdentifier)
				if err != nil {
					return nil, fmt.Errorf("invalid api key: %w", err)
				}
				return result, nil
			}
			result, err := a.Dynamic.ValidateImportedKey(token)
			if err != nil {
				return nil, fmt.Errorf("invalid api key: %w", err)
			}
			if result != nil {
				return result, nil
			} else if a.Dynamic.IsBlockedKey(token) {
				// make sure static keys do not work after import and key rotation
				return nil, fmt.Errorf("invalid api key")
			}
		}
		if a.static.config.Enabled {
			return a.static.ValidateAndExtract(token, scopes)
		}
		return nil, fmt.Errorf("invalid api key")
	}

	result, err := validate(token, scopes)
	if err != nil {
		return nil, errors.New(401, "unauthorized: %v", err)
	}
	return result, nil
}
