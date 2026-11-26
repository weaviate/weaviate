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

package raftauth

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/usecases/cluster"
)

func TestModeFrom(t *testing.T) {
	tests := []struct {
		name     string
		auth     cluster.BasicAuth
		want     Mode
		str      string
		sends    bool
		verifies bool
	}{
		{name: "no credentials", auth: cluster.BasicAuth{}, want: ModeOff, str: "off"},
		{name: "no credentials with send only", auth: cluster.BasicAuth{SendOnly: true}, want: ModeOff, str: "off"},
		{name: "credentials with send only", auth: cluster.BasicAuth{Username: "u", Password: "p", SendOnly: true}, want: ModeSend, str: "send", sends: true},
		{name: "credentials", auth: cluster.BasicAuth{Username: "u", Password: "p"}, want: ModeVerify, str: "verify", sends: true, verifies: true},
		{name: "username only", auth: cluster.BasicAuth{Username: "u"}, want: ModeVerify, str: "verify", sends: true, verifies: true},
		{name: "password only", auth: cluster.BasicAuth{Password: "p"}, want: ModeVerify, str: "verify", sends: true, verifies: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ModeFrom(tt.auth)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.str, got.String())
			assert.Equal(t, tt.sends, got.Sends())
			assert.Equal(t, tt.verifies, got.Verifies())
			assert.Equal(t, tt.auth.Enabled(), got.Sends())
			assert.Equal(t, tt.auth.VerifyEnabled(), got.Verifies())
		})
	}
}

func TestIsLoopbackBind(t *testing.T) {
	tests := []struct {
		host string
		want bool
	}{
		{host: "127.0.0.1", want: true},
		{host: "127.0.0.2", want: true},
		{host: "::1", want: true},
		{host: "localhost", want: true},
		{host: "", want: false},
		{host: "0.0.0.0", want: false},
		{host: "::", want: false},
		{host: "10.0.0.5", want: false},
		{host: "weaviate-0", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			assert.Equal(t, tt.want, IsLoopbackBind(tt.host))
		})
	}
}

func TestDeriveKey(t *testing.T) {
	assert.Nil(t, deriveKey(cluster.BasicAuth{}))
	assert.Len(t, deriveKey(cluster.BasicAuth{Username: "u", Password: "p"}), 32)
	assert.Equal(t, deriveKey(cluster.BasicAuth{Username: "u", Password: "p"}), deriveKey(cluster.BasicAuth{Username: "u", Password: "p", SendOnly: true}))
	assert.NotEqual(t, deriveKey(cluster.BasicAuth{Username: "ab", Password: "c"}), deriveKey(cluster.BasicAuth{Username: "a", Password: "bc"}))
	assert.NotEqual(t, deriveKey(cluster.BasicAuth{Username: "u", Password: "p"}), deriveKey(cluster.BasicAuth{Username: "u", Password: "q"}))
}
