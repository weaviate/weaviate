package test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"
)

const AuthCode = "auth"

func TestRbacWithOIDCManual(t *testing.T) {
	rsaKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	m, _ := mockoidc.NewServer(rsaKey)
	ln, _ := net.Listen("tcp", "127.0.0.1:48001")
	m.Start(ln, nil)
	defer m.Shutdown()

	admin := &mockoidc.MockUser{Subject: "admin-user"}
	m.QueueUser(admin)
	m.QueueCode(AuthCode)

	custom := &mockoidc.MockUser{Subject: "custom-user"}
	m.QueueUser(custom)
	m.QueueCode(AuthCode)

	fmt.Println(m.Issuer())
}

func TestRbacWithOIDC(t *testing.T) {

	m, _ := mockoidc.Run()
	defer m.Shutdown()

	fmt.Println(m.ClientID, m.Issuer())

	admin := &mockoidc.MockUser{Subject: "admin-user"}
	m.QueueUser(admin)
	m.QueueCode(AuthCode)
	tokenAdmin, _ := getTokens(t, m.AuthorizationEndpoint(), m.TokenEndpoint(), m.ClientID, m.ClientSecret)

	custom := &mockoidc.MockUser{Subject: "custom-user"}
	m.QueueUser(custom)
	m.QueueCode(AuthCode)
	tokenCustom, _ := getTokens(t, m.AuthorizationEndpoint(), m.TokenEndpoint(), m.ClientID, m.ClientSecret)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviate().WithRBAC().WithRbacAdmins(admin.Subject).WithOIDC(m.Issuer(), m.ClientID).Start(ctx)

	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	// type Config struct {
	//   	ClientID     string
	//   	ClientSecret string
	//   	Issuer       string
	//
	//   	AccessTTL  time.Duration
	//   	RefreshTTL time.Duration
	// }

	//helper.SetupClient("127.0.0.1:8081")

	className := t.Name() + "Class"
	readSchemaAction := authorization.ReadCollections
	createSchemaAction := authorization.CreateCollections

	err = createClass(t, &models.Class{Class: className}, helper.CreateAuth(tokenCustom))
	require.Error(t, err)
	var forbidden *clschema.SchemaObjectsCreateForbidden
	require.True(t, errors.As(err, &forbidden))

	createSchemaRoleName := "createSchema"
	createSchemaRole := &models.Role{
		Name: &createSchemaRoleName,
		Permissions: []*models.Permission{
			{Action: &readSchemaAction, Collections: &models.PermissionCollections{Collection: &className}},
			{Action: &createSchemaAction, Collections: &models.PermissionCollections{Collection: &className}},
		},
	}

	helper.DeleteRole(t, tokenAdmin, createSchemaRoleName)
	helper.CreateRole(t, tokenAdmin, createSchemaRole)
	defer helper.DeleteRole(t, tokenAdmin, createSchemaRoleName)
	helper.AssignRoleToUser(t, tokenAdmin, createSchemaRoleName, custom.Subject)

	err = createClass(t, &models.Class{Class: className}, helper.CreateAuth(tokenCustom))
	require.NoError(t, err)

}

func getTokens(t *testing.T, authEndpoint, tokenEndpoint, clientID, clientSecret string) (string, string) {
	// getting the token is a two-step process:
	// 1) authorizing with the clientSecret, the return contains an auth-code. However, (dont ask me why) the OIDC flow
	//    demands a redirect, so we will not get the return. In the mockserver, we can set the auth code to whatever
	//    we want, so we simply will use the same code for each call. Note that even though we do not get the return, we
	//    still need to make the request to initialize the session
	// 2) call the token endpoint with the auth code. This returns an access and refresh token.
	//
	// The access token can be used to authenticate with weaviate and the refresh token can be used to get a new valid
	// token in case the access token lifetime expires

	client := &http.Client{}

	data := url.Values{}
	data.Set("response_type", "code")
	data.Set("code", AuthCode)
	data.Set("redirect_uri", "google.com") // needs to be present
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)
	data.Set("scope", "email")
	data.Set("state", "email")

	req, err := http.NewRequest("POST", authEndpoint, bytes.NewBufferString(data.Encode()))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// not getting a useful return value as we dont provide a valid redirect
	_, err = client.Do(req)
	assert.NoError(t, err)

	data2 := url.Values{}
	data2.Set("grant_type", "authorization_code")
	data2.Set("client_id", clientID)
	data2.Set("client_secret", clientSecret)
	data2.Set("code", AuthCode)
	data2.Set("scope", "email")
	data2.Set("state", "email")
	req2, err := http.NewRequest("POST", tokenEndpoint, bytes.NewBufferString(data2.Encode()))
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req2)
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var tokenResponse map[string]interface{}
	err = json.Unmarshal(body, &tokenResponse)
	assert.NoError(t, err)

	accessToken := tokenResponse["access_token"].(string)
	refreshToken := tokenResponse["refresh_token"].(string)

	return accessToken, refreshToken
}
