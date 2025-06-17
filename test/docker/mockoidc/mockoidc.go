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
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/weaviate/mockoidc"
)

const (
	authCode     = "auth"
	clientSecret = "Secret"
	clientID     = "mock-oidc-test"
)

// This starts a mock OIDC server listening on 48001.
// All codes to authenticate are hardcoded and the server returns tokens for two custom users:
// - admin-user
// - custom-user
// afterwards the mockoidc default user is returned

func main() {
	getTLSConfig := func() *tls.Config {
		certificate := os.Getenv("MOCK_CERTIFICATE")
		certificateKey := os.Getenv("MOCK_CERTIFICATE_PRIVATE_KEY")
		if certificate != "" && certificateKey != "" {
			log.Println("Creating TLS config self signed certificates")
			// read certificates
			certBlock, _ := pem.Decode([]byte(certificate))
			cert, err := x509.ParseCertificate(certBlock.Bytes)
			if err != nil {
				log.Fatalf("parse certificate: %v", err)
			}
			var privKey crypto.PrivateKey
			keyBlock, _ := pem.Decode([]byte(certificateKey))
			if keyBlock.Type == "RSA PRIVATE KEY" {
				privKey, err = x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
				if err != nil {
					log.Fatalf("parse certificate's private rsa key: %v", err)
				}
			} else {
				privKey, err = x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
				if err != nil {
					log.Fatalf("parse certificate's private key: %v", err)
				}
			}
			// TLS configuration
			tlsCert := tls.Certificate{
				Certificate: [][]byte{cert.Raw},
				PrivateKey:  privKey,
			}
			tlsConfig := &tls.Config{
				Certificates: []tls.Certificate{tlsCert},
				MinVersion:   tls.VersionTLS12,
			}
			return tlsConfig
		}
		// Default OIDC server
		return nil
	}
	tlsConfig := getTLSConfig()
	rsaKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	m, _ := mockoidc.NewServer(rsaKey)
	m.ClientSecret = clientSecret
	m.ClientID = clientID
	hostname := os.Getenv("MOCK_HOSTNAME")
	m.Hostname = fmt.Sprintf("%s:48001", hostname)
	addr := "0.0.0.0:48001"
	if hostname != "" {
		addr = fmt.Sprintf("%s:48001", hostname)
	}
	ln, _ := net.Listen("tcp", addr)
	m.Start(ln, tlsConfig)
	defer m.Shutdown()

	admin := &mockoidc.MockUser{Subject: "admin-user"}
	m.QueueUser(admin)
	m.QueueCode(authCode)

	custom := &mockoidc.MockUser{Subject: "custom-user", Groups: []string{"custom-group"}}
	m.QueueUser(custom)
	m.QueueCode(authCode)

	log.Printf("issuer: %v\n", m.Issuer())
	log.Printf("discovery endpoint: %v\n", m.DiscoveryEndpoint())
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
