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

package resolver

import (
	"fmt"
	"net"
	"strings"
)

// FormatAddressWithPort formats an IP address with a port, properly handling IPv6 addresses
func FormatAddressWithPort(addr string, port uint16) string {
	// Remove any existing brackets
	addr = strings.Trim(addr, "[]")

	// Add brackets for IPv6 addresses
	if IsIPv6(addr) {
		return fmt.Sprintf("[%s]:%d", addr, port)
	}

	return fmt.Sprintf("%s:%d", addr, port)
}

// ParseAddressWithPort parses an address with port, properly handling IPv6 addresses
func ParseAddressWithPort(addr string) (string, uint16, error) {
	var host string
	var portStr string

	// Try to use standard SplitHostPort first
	if strings.HasPrefix(addr, "[") {
		var err error
		host, portStr, err = net.SplitHostPort(addr)
		if err != nil {
			return "", 0, err
		}
	} else if IsIPv6(addr) {
		// For non-bracketed IPv6 addresses, we need custom parsing
		lastColon := strings.LastIndex(addr, ":")
		if lastColon == -1 {
			return addr, 0, nil
		}

		host = addr[:lastColon]
		portStr = addr[lastColon+1:]
	} else {
		// Regular IPv4 or hostname
		var err error
		host, portStr, err = net.SplitHostPort(addr)
		if err != nil {
			return "", 0, err
		}
	}

	// Parse port number
	port, err := net.LookupPort("tcp", portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port: %w", err)
	}

	return host, uint16(port), nil
}

// IsIPv6 checks if an address is an IPv6 address
func IsIPv6(addr string) bool {
	return strings.Count(strings.Trim(addr, "[]"), ":") >= 2
}
