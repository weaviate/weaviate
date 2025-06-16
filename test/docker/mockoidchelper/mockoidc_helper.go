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

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
)

const (
	authCode     = "auth"
	clientSecret = "Secret"
	clientID     = "mock-oidc-test"
)

type Response struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
}

type tokensHandler struct {
	client                      *http.Client
	authEndpoint, tokenEndpoint string
}

func newTokensHandler() (*tokensHandler, error) {
	getEndpointsAndClient := func() (string, string, *http.Client, error) {
		hostname := os.Getenv("MOCK_HOSTNAME")
		certificate := os.Getenv("MOCK_CERTIFICATE")
		if certificate != "" {
			certBlock, _ := pem.Decode([]byte(certificate))
			cert, err := x509.ParseCertificate(certBlock.Bytes)
			if err != nil {
				return "", "", nil, fmt.Errorf("failed to decode certificate: %w", err)
			}

			certPool := x509.NewCertPool()
			certPool.AddCert(cert)

			// Create an HTTP client with self signed certificate
			client := &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						RootCAs:    certPool,
						MinVersion: tls.VersionTLS12,
					},
				},
			}

			// Adjust endpoints to use https
			authEndpoint := "https://" + hostname + "/oidc/authorize"
			tokenEndpoint := "https://" + hostname + "/oidc/token"

			return authEndpoint, tokenEndpoint, client, nil
		}
		// Default HTTP client
		client := &http.Client{}

		authEndpoint := "http://" + hostname + "/oidc/authorize"
		tokenEndpoint := "http://" + hostname + "/oidc/token"

		return authEndpoint, tokenEndpoint, client, nil
	}

	authEndpoint, tokenEndpoint, client, err := getEndpointsAndClient()
	if err != nil {
		return nil, err
	}
	return &tokensHandler{authEndpoint: authEndpoint, tokenEndpoint: tokenEndpoint, client: client}, nil
}

func (t *tokensHandler) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	accessToken, refreshToken, err := getTokensFromMockOIDC(t.client, t.authEndpoint, t.tokenEndpoint)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := Response{AccessToken: accessToken, RefreshToken: refreshToken}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, fmt.Sprintf("encode response: %v", err), http.StatusInternalServerError)
	}
}

func getTokensFromMockOIDC(client *http.Client, authEndpoint, tokenEndpoint string) (string, string, error) {
	// getting the token is a two-step process:
	// 1) authorizing with the clientSecret, the return contains an auth-code. However, (dont ask me why) the OIDC flow
	//    demands a redirect, so we will not get the return. In the mockserver, we can set the auth code to whatever
	//    we want, so we simply will use the same code for each call. Note that even though we do not get the return, we
	//    still need to make the request to initialize the session
	// 2) call the token endpoint with the auth code. This returns an access and refresh token.
	//
	// The access token can be used to authenticate with weaviate and the refresh token can be used to get a new valid
	// token in case the access token lifetime expires

	data := url.Values{}
	data.Set("response_type", "code")
	data.Set("code", authCode)
	data.Set("redirect_uri", "google.com") // needs to be present
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)
	data.Set("state", "email")
	data.Set("scope", "openid groups")
	req, err := http.NewRequest("POST", authEndpoint, bytes.NewBufferString(data.Encode()))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// not getting a useful return value as we dont provide a valid redirect
	resp, err := client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	data2 := url.Values{}
	data2.Set("grant_type", "authorization_code")
	data2.Set("client_id", clientID)
	data2.Set("client_secret", clientSecret)
	data2.Set("code", authCode)
	data2.Set("scope", "email")
	data2.Set("state", "email")
	req2, err := http.NewRequest("POST", tokenEndpoint, bytes.NewBufferString(data2.Encode()))
	if err != nil {
		return "", "", err
	}
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp2, err := client.Do(req2)
	if err != nil {
		return "", "", err
	}
	defer resp2.Body.Close()
	body, _ := io.ReadAll(resp2.Body)
	var tokenResponse map[string]interface{}
	err = json.Unmarshal(body, &tokenResponse)
	if err != nil {
		return "", "", err
	}
	accessToken, ok := tokenResponse["id_token"].(string)
	if !ok {
		return "", "", fmt.Errorf("failed to get access token from: %v", tokenResponse)
	}
	refreshToken, ok := tokenResponse["refresh_token"].(string)
	if !ok {
		return "", "", fmt.Errorf("failed to get refresh token from: %v", tokenResponse)
	}
	return accessToken, refreshToken, nil
}

func main() {
	// Create tokens handler
	tk, err := newTokensHandler()
	if err != nil {
		log.Fatalf("Failed to create tokens handler: %v", err)
	}
	// Create a new ServeMux for routing
	mux := http.NewServeMux()
	mux.HandleFunc("/tokens", tk.handler)

	// Create the HTTP server
	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		// Start the server
		log.Println("Starting server on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()
	defer func() {
		// Perform graceful shutdown
		if err := server.Shutdown(context.Background()); err != nil {
			log.Fatalf("Server shutdown failed: %v", err)
		}
		log.Println("Server gracefully stopped")
	}()

	// Create a channel to receive OS signals
	sigChan := make(chan os.Signal, 1)
	// Notify the channel of the interrupt signal (Ctrl+C)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Print a message indicating the program is running
	log.Println("Program is running. Press Ctrl+C to stop.")

	// Block until a signal is received
	sig := <-sigChan
	log.Printf("Received signal: %v. Shutting down...\n", sig)
}
