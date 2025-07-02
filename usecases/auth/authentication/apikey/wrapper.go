//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package apikey

import (
	"github.com/go-openapi/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
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

func (a *ApiKey) ValidateAndExtract(token string, scopes []string) (*models.Principal, error) {
	if randomKey, userIdentifier, err := keys.DecodeApiKey(token); err == nil {
		principal, err := a.Dynamic.ValidateAndExtract(randomKey, userIdentifier)
		if err != nil {
			return nil, errors.New(401, "unauthorized: %v", err)
		}
		return principal, nil
	}
	principal, err := a.Dynamic.ValidateImportedKey(token)
	if err != nil {
		return nil, errors.New(401, "unauthorized: %v", err)
	}
	if principal != nil {
		return principal, nil
	} else if a.Dynamic.IsBlockedKey(token) {
		// make sure static keys do not work after import and key rotation
		return nil, errors.New(401, "unauthorized: invalid token")
	} else {
		return a.static.ValidateAndExtract(token, scopes)
	}
}
