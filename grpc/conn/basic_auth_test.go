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

package grpcconn

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestValidateBasicAuth(t *testing.T) {
	encode := func(s string) string { return "Basic " + base64.StdEncoding.EncodeToString([]byte(s)) }
	tests := []struct {
		name    string
		md      metadata.MD
		noMD    bool
		wantErr bool
	}{
		{name: "valid", md: metadata.Pairs("authorization", BasicAuthHeader("user", "pass"))},
		{name: "no metadata", noMD: true, wantErr: true},
		{name: "no authorization header", md: metadata.Pairs("x", "y"), wantErr: true},
		{name: "bearer scheme", md: metadata.Pairs("authorization", "Bearer abc"), wantErr: true},
		{name: "invalid base64", md: metadata.Pairs("authorization", "Basic !!!"), wantErr: true},
		{name: "no colon", md: metadata.Pairs("authorization", encode("userpass")), wantErr: true},
		{name: "wrong password", md: metadata.Pairs("authorization", BasicAuthHeader("user", "wrong")), wantErr: true},
		{name: "wrong username", md: metadata.Pairs("authorization", BasicAuthHeader("other", "pass")), wantErr: true},
		{name: "password prefix", md: metadata.Pairs("authorization", BasicAuthHeader("user", "pas")), wantErr: true},
		{name: "password suffix", md: metadata.Pairs("authorization", BasicAuthHeader("user", "pass1")), wantErr: true},
		{name: "empty credentials", md: metadata.Pairs("authorization", BasicAuthHeader("", "")), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBasicAuth(tt.md, !tt.noMD, "user", "pass")
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, codes.Unauthenticated, status.Code(err))
		})
	}
}

func TestValidateBasicAuth_PasswordContainingColon(t *testing.T) {
	require.NoError(t, ValidateBasicAuth(metadata.Pairs("authorization", BasicAuthHeader("user", "a:b")), true, "user", "a:b"))
	require.Error(t, ValidateBasicAuth(metadata.Pairs("authorization", BasicAuthHeader("user", "a")), true, "user", "a:b"))
}

func TestBasicAuthServerUnaryInterceptor(t *testing.T) {
	interceptor := BasicAuthServerUnaryInterceptor("user", "pass")
	called := 0
	handler := func(context.Context, any) (any, error) { called++; return "ok", nil }
	info := &grpc.UnaryServerInfo{FullMethod: "/svc/Method"}

	_, err := interceptor(context.Background(), nil, info, handler)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.Equal(t, 0, called)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", BasicAuthHeader("user", "pass")))
	resp, err := interceptor(ctx, nil, info, handler)
	require.NoError(t, err)
	assert.Equal(t, "ok", resp)
	assert.Equal(t, 1, called)
}
