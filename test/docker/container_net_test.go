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

package docker

import (
	"fmt"
	"strings"
	"testing"
)

func TestPickNetOctet(t *testing.T) {
	t.Run("valid SOAK_NET_OCTET wins", func(t *testing.T) {
		t.Setenv("SOAK_NET_OCTET", "42")
		if got := pickNetOctet(); got != 42 {
			t.Fatalf("want 42, got %d", got)
		}
	})
	t.Run("invalid/excluded override falls back to a random in-range octet", func(t *testing.T) {
		for _, v := range []string{"128", "0", "255", "300", "-1", "abc", " 42"} {
			t.Setenv("SOAK_NET_OCTET", v)
			if got := pickNetOctet(); got < 16 || got > 127 {
				t.Fatalf("override %q: fallback octet %d not in [16,127]", v, got)
			}
		}
	})
	t.Run("unset always yields an octet in [16,127]", func(t *testing.T) {
		t.Setenv("SOAK_NET_OCTET", "")
		for i := 0; i < 2000; i++ {
			if got := pickNetOctet(); got < 16 || got > 127 {
				t.Fatalf("random octet %d not in [16,127]", got)
			}
		}
	})
}

// subnet, gateway, node IPs, and MinIO name must all derive from the cluster's
// octet, else the network is broken / MinIO unreachable.
func TestOctetDerivedAddresses(t *testing.T) {
	const octet = 73
	prefix := fmt.Sprintf("10.%d.", octet)
	for name, got := range map[string]string{
		"subnet":    subnetForOctet(octet),
		"gateway":   gatewayForOctet(octet),
		"weaviate0": staticIPForHostname(octet, Weaviate0),
		"weaviate1": staticIPForHostname(octet, Weaviate1),
		"weaviate2": staticIPForHostname(octet, Weaviate2),
	} {
		if !strings.HasPrefix(got, prefix) {
			t.Fatalf("%s=%q does not start with %q", name, got, prefix)
		}
	}
	if got := staticIPForHostname(octet, "not-a-weaviate-node"); got != "" {
		t.Fatalf("non-weaviate static IP should be empty, got %q", got)
	}
	if want := fmt.Sprintf("%s-%d", MinIO, octet); minioContainerName(octet) != want {
		t.Fatalf("minioContainerName(%d)=%q, want %q", octet, minioContainerName(octet), want)
	}
}
