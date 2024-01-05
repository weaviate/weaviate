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

package oidc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v4"
	jose "github.com/square/go-jose"
)

func newOIDCServer(t *testing.T) *httptest.Server {
	// we need to start up with an empty handler
	s := httptest.NewServer(nil)

	// so that we can configure it once we now the url, this is used to match the
	// issue field
	s.Config.Handler = oidcHandler(t, s.URL)
	return s
}

type oidcDiscovery struct {
	Issuer  string `json:"issuer"`
	JWKSUri string `json:"jwks_uri"`
}

type jwksResponse struct {
	Keys []jose.JSONWebKey `json:"keys"`
}

func oidcHandler(t *testing.T, url string) http.Handler {
	mux := http.NewServeMux()

	publicKey, err := jwt.ParseRSAPublicKeyFromPEM([]byte(testingPublicKey))
	if err != nil {
		t.Fatalf("test server: couldn't parse public key: %v", err)
	}

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		d := oidcDiscovery{
			Issuer:  url,
			JWKSUri: fmt.Sprintf("%v/.well-known/jwks", url),
		}
		json.NewEncoder(w).Encode(d)
	})

	mux.HandleFunc("/.well-known/jwks", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		d := jwksResponse{
			Keys: []jose.JSONWebKey{
				{
					Key:       publicKey,
					Use:       "sig",
					Algorithm: string(jose.RS256),
					KeyID:     "my-key",
				},
			},
		}
		if err := json.NewEncoder(w).Encode(d); err != nil {
			t.Fatalf("encoding jwks in test server: %v", err)
		}
	})

	return mux
}

// those keys are intended to make it possible to sign our own tokens in tests.
// Never use these keys for anything outside a test scenario!

var testingPrivateKey = `-----BEGIN RSA PRIVATE KEY-----
MIICXAIBAAKBgQDFRV9sD1ULVV7q1w9OXCXPTFRcrTYAZAVZwg8X9V1QyBd8eyp5
OMI4YxuL7sk+Las+PTcS6AdrHitdDZNqUjWFYOo5EQLnVBghIlu3ZWlAnM2SCPo5
e2jFD8IgAVHtkAHbFUliQtP6a6OOLMRq9GMhIv2ZWf79KyXvh5DFuM7zbwIDAQAB
AoGAXptEhghcWtEYcjutZYEfyOjsVH3lNg7B2igNIQpVNFahnNtcpUIpMu2k2lks
Phuc0n59GR4Z4K9ZUIkgN48xhuqDtHevMQLfg6KQaqf0KRwxBw4dIOhUX0aLkvcJ
WTtUPE+3hYbOuAPuXVBDB6hBZAe5mbvLPYDM3yYyRotbN7ECQQD/S3Y+shEHOMg1
ve1eQ4tjN+5Fdmq8l2JIbOPpvH6ytiEQSV2Q55u8gL+1x5Tb9vh3rAdg2OJ0LFay
VTqmCmkDAkEAxdDgvDqk7JwMbM2jxozVEcECoN07eGrshVWlXtnEpJgU4vBN8wAj
sS94WZCWu4LZRzPHp36dVDiPFS0aqGlCJQJAMGKX/Zf4HDtJzs25YEVC9MIT+bxQ
zH+QlBN3OsSL6skUCScugZkz7g0kyIoUD4CGZQAIwfU5LjV9FP2MSQ3uCwJAZxS0
t4F7xcx/cQcry+BBe7HvU7JVNifJvqVlumqSXQ7e+28rv3AYKVHKTinZUjcaUE88
QBzrkSKz9N3/ITlQfQJBAL25aXdmooBdYQUvXmNu+n10wwDAqCKtoGW75cZBJvjX
WnBQsDVlzaBcs32lr08XZIAH318OibfmAs5HKHABoFk=
-----END RSA PRIVATE KEY-----`

var testingPublicKey = `-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDFRV9sD1ULVV7q1w9OXCXPTFRc
rTYAZAVZwg8X9V1QyBd8eyp5OMI4YxuL7sk+Las+PTcS6AdrHitdDZNqUjWFYOo5
EQLnVBghIlu3ZWlAnM2SCPo5e2jFD8IgAVHtkAHbFUliQtP6a6OOLMRq9GMhIv2Z
Wf79KyXvh5DFuM7zbwIDAQAB
-----END PUBLIC KEY-----`

func signToken(claims jwt.Claims) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	key, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(testingPrivateKey))
	if err != nil {
		return "", err
	}

	return token.SignedString(key)
}
