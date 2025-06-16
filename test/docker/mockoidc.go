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

package docker

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const MockOIDC = "mock-oidc"

func startMockOIDC(ctx context.Context, networkName, mockoidcImage, certificate, certificatePrivateKey string) (*DockerContainer, error) {
	path, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	getContextPath := func(path string) string {
		if strings.Contains(path, "test/acceptance_with_go_client") {
			return path[:strings.Index(path, "/test/acceptance_with_go_client")]
		}
		if strings.Contains(path, "test/acceptance") {
			return path[:strings.Index(path, "/test/acceptance")]
		}
		return path[:strings.Index(path, "/test/modules")]
	}
	fromDockerFile := testcontainers.FromDockerfile{}
	if mockoidcImage == "" {
		contextPath := fmt.Sprintf("%s/test/docker/mockoidc", getContextPath(path))
		fromDockerFile = testcontainers.FromDockerfile{
			Context:       contextPath,
			Dockerfile:    "Dockerfile",
			PrintBuildLog: true,
			KeepImage:     false,
		}
	}
	containerEnvs := map[string]string{
		"MOCK_HOSTNAME": MockOIDC,
	}
	if certificate != "" && certificatePrivateKey != "" {
		containerEnvs["MOCK_CERTIFICATE"] = certificate
		containerEnvs["MOCK_CERTIFICATE_PRIVATE_KEY"] = certificatePrivateKey
	}
	port := nat.Port("48001/tcp")
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: fromDockerFile,
			Image:          mockoidcImage,
			ExposedPorts:   []string{"48001/tcp"},
			Name:           MockOIDC,
			Hostname:       MockOIDC,
			AutoRemove:     true,
			Networks:       []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {MockOIDC},
			},
			Env: containerEnvs,
			WaitingFor: wait.ForAll(
				wait.ForListeningPort(port),
			).WithStartupTimeoutDefault(60 * time.Second),
		},
		Started: true,
		Reuse:   true,
	})
	if err != nil {
		return nil, err
	}
	uri, err := container.PortEndpoint(ctx, port, "")
	if err != nil {
		return nil, err
	}
	endpoints := make(map[EndpointName]endpoint)
	endpoints[HTTP] = endpoint{port, uri}
	envSettings := make(map[string]string)
	envSettings["AUTHENTICATION_OIDC_ENABLED"] = "true"
	envSettings["AUTHENTICATION_OIDC_CLIENT_ID"] = "mock-oidc-test"
	envSettings["AUTHENTICATION_OIDC_USERNAME_CLAIM"] = "sub"
	envSettings["AUTHENTICATION_OIDC_GROUPS_CLAIM"] = "groups"
	envSettings["AUTHENTICATION_OIDC_SCOPES"] = "openid"
	if certificate != "" && certificatePrivateKey != "" {
		envSettings["AUTHENTICATION_OIDC_ISSUER"] = fmt.Sprintf("https://%s:48001/oidc", MockOIDC)
		envSettings["AUTHENTICATION_OIDC_CERTIFICATE"] = certificate
	} else {
		envSettings["AUTHENTICATION_OIDC_ISSUER"] = fmt.Sprintf("http://%s:48001/oidc", MockOIDC)
	}
	return &DockerContainer{MockOIDC, endpoints, container, envSettings}, nil
}

func GenerateCertificateAndKey(dnsName string) (string, string, error) {
	// Generate a private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}

	// Create a certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: dnsName,
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour), // Valid for 1 year
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{dnsName},                  // SAN for localhost
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, // SAN for 127.0.0.1
	}

	// Create the self-signed certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return "", "", err
	}

	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	certKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})

	return string(cert), string(certKey), nil
}
