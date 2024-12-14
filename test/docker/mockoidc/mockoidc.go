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
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/oauth2-proxy/mockoidc"
)

const (
	authCode     = "auth"
	clientSecret = "Secret"
	clientID     = "mock-oidc-test"
)

func main() {
	rsaKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	m, _ := mockoidc.NewServer(rsaKey)
	m.ClientSecret = clientSecret
	m.ClientID = clientID
	hostname := os.Getenv("MOCK_HOSTNAME")
	addr := "0.0.0.0:48001"
	if hostname != "" {
		addr = fmt.Sprintf("%s:48001", hostname)
	}
	ln, _ := net.Listen("tcp", addr)
	m.Start(ln, nil)
	defer m.Shutdown()

	admin := &mockoidc.MockUser{Subject: "admin-user"}
	m.QueueUser(admin)
	m.QueueCode(authCode)

	custom := &mockoidc.MockUser{Subject: "custom-user"}
	m.QueueUser(custom)
	m.QueueCode(authCode)

	fmt.Println(m.Issuer())
	// Create a channel to receive OS signals
	sigChan := make(chan os.Signal, 1)
	// Notify the channel of the interrupt signal (Ctrl+C)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Print a message indicating the program is running
	fmt.Println("Program is running. Press Ctrl+C to stop.")

	// Block until a signal is received
	sig := <-sigChan
	fmt.Printf("Received signal: %v. Shutting down...\n", sig)
}
