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

package rest

import (
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
	wldbusers "github.com/weaviate/weaviate/wl/dbusers"
)

// dbUserExpirationFeature names DB user expiration in the license refusal.
const dbUserExpirationFeature = "db user expiration"

// dbUserExpirationModeFor is the only code that pairs
// AUTHENTICATION_DB_USERS_ENABLED with Config.WeaviateLicense.
func dbUserExpirationModeFor(cfg config.Config) license.Mode {
	return license.ModeFor(cfg.Authentication.DBUsers.Enabled, cfg.WeaviateLicense)
}

// dbUserExpiryResolver picks the ExpiryResolver that createUser resolves a
// requested expiresAt with. Any mode but FeatureLicensed gets the license
// refusal.
func dbUserExpiryResolver(mode license.Mode) apikey.ExpiryResolver {
	switch mode {
	case license.FeatureLicensed:
		return wldbusers.NewValidatingExpiry()
	case license.FeatureOff, license.FeatureUnlicensed:
	}
	return apikey.RefusingExpiry(license.Required(dbUserExpirationFeature))
}
