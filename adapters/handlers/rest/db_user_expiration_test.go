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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
)

func TestDBUserExpirationModeFor(t *testing.T) {
	cases := []struct {
		name           string
		dbUsersEnabled bool
		licensed       bool
		want           license.Mode
	}{
		{name: "db users on without a license", dbUsersEnabled: true, licensed: false, want: license.FeatureUnlicensed},
		{name: "db users off with a license", dbUsersEnabled: false, licensed: true, want: license.FeatureOff},
		{name: "db users on with a license", dbUsersEnabled: true, licensed: true, want: license.FeatureLicensed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg config.Config
			cfg.Authentication.DBUsers.Enabled = tc.dbUsersEnabled
			cfg.WeaviateLicense = tc.licensed

			require.Equal(t, tc.want, dbUserExpirationModeFor(cfg))
		})
	}
}

func TestDBUserExpiryResolver(t *testing.T) {
	requested := time.Now().Add(time.Hour)
	cases := []struct {
		name    string
		mode    license.Mode
		wantWL  bool
		wantErr error
	}{
		{name: "unlicensed refuses a requested time", mode: license.FeatureUnlicensed, wantWL: false, wantErr: license.ErrRequired},
		{name: "licensed wires the wl resolver", mode: license.FeatureLicensed, wantWL: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := dbUserExpiryResolver(tc.mode)

			require.Equal(t, tc.wantWL, license.DeclaredInWL(r), "only FeatureLicensed may wire wl code")
			_, err := r.Resolve(&requested)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
