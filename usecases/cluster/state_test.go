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

package cluster

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateClusterConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid configuration",
			config: Config{
				Hostname:       "test-node",
				GossipBindPort: 7946,
				DataBindPort:   7947,
				AdvertisePort:  7946,
				AdvertiseAddr:  "192.168.1.100",
				BindAddr:       "0.0.0.0",
			},
			expectError: false,
		},
		{
			name: "empty hostname",
			config: Config{
				Hostname: "",
			},
			expectError: true,
			errorMsg:    "hostname cannot be empty",
		},
		{
			name: "invalid gossip port - too low",
			config: Config{
				Hostname:       "test-node",
				GossipBindPort: 1023,
			},
			expectError: true,
			errorMsg:    "invalid GossipBindPort: 1023 (must be between 1024-65535)",
		},
		{
			name: "invalid gossip port - too high",
			config: Config{
				Hostname:       "test-node",
				GossipBindPort: 65536,
			},
			expectError: true,
			errorMsg:    "invalid GossipBindPort: 65536 (must be between 1024-65535)",
		},
		{
			name: "invalid data port",
			config: Config{
				Hostname:     "test-node",
				DataBindPort: 100,
			},
			expectError: true,
			errorMsg:    "invalid DataBindPort: 100 (must be between 1024-65535)",
		},
		{
			name: "invalid advertise port",
			config: Config{
				Hostname:      "test-node",
				AdvertisePort: 70000,
			},
			expectError: true,
			errorMsg:    "invalid AdvertisePort: 70000 (must be between 1024-65535)",
		},
		{
			name: "invalid advertise address",
			config: Config{
				Hostname:      "test-node",
				AdvertiseAddr: "invalid-ip",
			},
			expectError: true,
			errorMsg:    "invalid AdvertiseAddr: invalid-ip (must be a valid IP address)",
		},
		{
			name: "invalid bind address",
			config: Config{
				Hostname: "test-node",
				BindAddr: "not-an-ip",
			},
			expectError: true,
			errorMsg:    "invalid BindAddr: not-an-ip (must be a valid IP address)",
		},
		{
			name: "valid IPv6 addresses",
			config: Config{
				Hostname:      "test-node",
				AdvertiseAddr: "2001:db8::1",
				BindAddr:      "::",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateClusterConfig(tt.config)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSelectMemberlistConfig(t *testing.T) {
	tests := []struct {
		name           string
		config         Config
		expectedType   string
		expectedConfig func(*memberlist.Config) bool
	}{
		{
			name: "localhost configuration",
			config: Config{
				Localhost: true,
			},
			expectedType: "LOCAL",
			expectedConfig: func(cfg *memberlist.Config) bool {
				// Check if it's using local config characteristics
				return cfg.ProbeTimeout == 200*time.Millisecond &&
					cfg.PushPullInterval == 15*time.Second
			},
		},
		{
			name: "WAN configuration with advertise address",
			config: Config{
				AdvertiseAddr: "192.168.1.100",
			},
			expectedType: "WAN",
			expectedConfig: func(cfg *memberlist.Config) bool {
				// Check if it's using WAN config characteristics
				return cfg.TCPTimeout == 30*time.Second &&
					cfg.ProbeTimeout == 3*time.Second
			},
		},
		{
			name:   "LAN configuration (default)",
			config: Config{
				// No special flags
			},
			expectedType: "LAN",
			expectedConfig: func(cfg *memberlist.Config) bool {
				// Check if it's using LAN config characteristics
				return cfg.TCPTimeout == 10*time.Second &&
					cfg.ProbeTimeout == 500*time.Millisecond
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := selectMemberlistConfig(tt.config)

			assert.NotNil(t, cfg)
			assert.Equal(t, tt.expectedType, getConfigType(tt.config))
			assert.True(t, tt.expectedConfig(cfg), "Configuration doesn't match expected type")
		})
	}
}

func TestConfigureMemberlistPorts(t *testing.T) {
	tests := []struct {
		name           string
		config         Config
		expectedBind   int
		expectedAdvert int
	}{
		{
			name: "set gossip bind port",
			config: Config{
				GossipBindPort: 7946,
			},
			expectedBind:   7946,
			expectedAdvert: 0, // Not set
		},
		{
			name: "set advertise port explicitly",
			config: Config{
				GossipBindPort: 7946,
				AdvertisePort:  8000,
			},
			expectedBind:   7946,
			expectedAdvert: 8000,
		},
		{
			name: "set advertise port with advertise addr",
			config: Config{
				GossipBindPort: 7000,
				AdvertiseAddr:  "192.168.1.100",
			},
			expectedBind:   7000,
			expectedAdvert: 7000, // Should default to GossipBindPort
		},
		{
			name: "advertise port overrides default",
			config: Config{
				GossipBindPort: 7946,
				AdvertiseAddr:  "192.168.1.100",
				AdvertisePort:  8000,
			},
			expectedBind:   7946,
			expectedAdvert: 8000, // Should override default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &memberlist.Config{}
			configureMemberlistPorts(cfg, tt.config)

			assert.Equal(t, tt.expectedBind, cfg.BindPort)
			assert.Equal(t, tt.expectedAdvert, cfg.AdvertisePort)
		})
	}
}

func TestConfigureMemberlistAddresses(t *testing.T) {
	tests := []struct {
		name           string
		config         Config
		expectedBind   string
		expectedAdvert string
		expectError    bool
	}{
		{
			name: "set bind address",
			config: Config{
				BindAddr: "192.168.1.100",
			},
			expectedBind: "192.168.1.100",
			expectError:  false,
		},
		{
			name: "set advertise address",
			config: Config{
				AdvertiseAddr: "10.0.0.1",
			},
			expectedAdvert: "10.0.0.1",
			expectError:    false,
		},
		{
			name: "set both addresses",
			config: Config{
				BindAddr:      "0.0.0.0",
				AdvertiseAddr: "192.168.1.100",
			},
			expectedBind:   "0.0.0.0",
			expectedAdvert: "192.168.1.100",
			expectError:    false,
		},
		{
			name: "IPv6 addresses",
			config: Config{
				BindAddr:      "::",
				AdvertiseAddr: "2001:db8::1",
			},
			expectedBind:   "::",
			expectedAdvert: "2001:db8::1",
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &memberlist.Config{}
			err := configureMemberlistAddresses(cfg, tt.config)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedBind, cfg.BindAddr)
				assert.Equal(t, tt.expectedAdvert, cfg.AdvertiseAddr)
			}
		})
	}
}

func TestConfigureMemberlistSettings(t *testing.T) {
	tests := []struct {
		name                    string
		config                  Config
		raftTimeoutsMultiplier  int
		expectedTCPTimeout      time.Duration
		expectedSuspicionMult   int
		expectedDeadReclaimTime time.Duration
	}{
		{
			name:   "LAN configuration with default settings",
			config: Config{
				// No AdvertiseAddr = LAN
			},
			raftTimeoutsMultiplier:  1,
			expectedTCPTimeout:      10 * time.Second,
			expectedSuspicionMult:   0, // Not set by configureMemberlistSettings
			expectedDeadReclaimTime: 60 * time.Second,
		},
		{
			name: "WAN configuration",
			config: Config{
				AdvertiseAddr: "192.168.1.100",
			},
			raftTimeoutsMultiplier:  1,
			expectedTCPTimeout:      30 * time.Second,
			expectedSuspicionMult:   0, // Not set by configureMemberlistSettings
			expectedDeadReclaimTime: 60 * time.Second,
		},
		{
			name: "localhost configuration",
			config: Config{
				Localhost: true,
			},
			raftTimeoutsMultiplier:  1,
			expectedTCPTimeout:      10 * time.Second,
			expectedSuspicionMult:   0, // Not set by configureMemberlistSettings
			expectedDeadReclaimTime: 60 * time.Second,
		},
		{
			name: "fast failure detection enabled",
			config: Config{
				MemberlistFastFailureDetection: true,
			},
			raftTimeoutsMultiplier:  1,
			expectedTCPTimeout:      10 * time.Second,
			expectedSuspicionMult:   1,               // Overridden by fast failure detection
			expectedDeadReclaimTime: 1 * time.Second, // Overridden by fast failure detection
		},
		{
			name: "WAN with timeout multiplier",
			config: Config{
				AdvertiseAddr: "192.168.1.100",
			},
			raftTimeoutsMultiplier:  2,
			expectedTCPTimeout:      60 * time.Second, // 30 * 2
			expectedSuspicionMult:   0,                // Not set by configureMemberlistSettings
			expectedDeadReclaimTime: 60 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &memberlist.Config{}
			configureMemberlistSettings(cfg, tt.config, tt.raftTimeoutsMultiplier)

			assert.Equal(t, tt.expectedTCPTimeout, cfg.TCPTimeout)
			assert.Equal(t, tt.expectedSuspicionMult, cfg.SuspicionMult)
			assert.Equal(t, tt.expectedDeadReclaimTime, cfg.DeadNodeReclaimTime)
		})
	}
}

// TestConfigSelectionAndSettingsIntegration tests the full integration of config selection and settings
func TestConfigSelectionAndSettingsIntegration(t *testing.T) {
	tests := []struct {
		name                    string
		config                  Config
		raftTimeoutsMultiplier  int
		expectedTCPTimeout      time.Duration
		expectedSuspicionMult   int
		expectedDeadReclaimTime time.Duration
		expectedConfigType      string
	}{
		{
			name:   "LAN configuration with defaults",
			config: Config{
				// No special flags = LAN
			},
			raftTimeoutsMultiplier:  1,
			expectedTCPTimeout:      10 * time.Second,
			expectedSuspicionMult:   4, // Default LAN config
			expectedDeadReclaimTime: 60 * time.Second,
			expectedConfigType:      "LAN",
		},
		{
			name: "WAN configuration with defaults",
			config: Config{
				AdvertiseAddr: "192.168.1.100",
			},
			raftTimeoutsMultiplier:  1,
			expectedTCPTimeout:      30 * time.Second,
			expectedSuspicionMult:   6, // Default WAN config
			expectedDeadReclaimTime: 60 * time.Second,
			expectedConfigType:      "WAN",
		},
		{
			name: "LOCAL configuration with defaults",
			config: Config{
				Localhost: true,
			},
			raftTimeoutsMultiplier:  1,
			expectedTCPTimeout:      10 * time.Second,
			expectedSuspicionMult:   3, // Default LOCAL config
			expectedDeadReclaimTime: 60 * time.Second,
			expectedConfigType:      "LOCAL",
		},
		{
			name: "LAN with fast failure detection",
			config: Config{
				MemberlistFastFailureDetection: true,
			},
			raftTimeoutsMultiplier:  1,
			expectedTCPTimeout:      10 * time.Second,
			expectedSuspicionMult:   1,               // Overridden by fast failure detection
			expectedDeadReclaimTime: 1 * time.Second, // Overridden by fast failure detection
			expectedConfigType:      "LAN",
		},
		{
			name: "WAN with fast failure detection",
			config: Config{
				AdvertiseAddr:                  "192.168.1.100",
				MemberlistFastFailureDetection: true,
			},
			raftTimeoutsMultiplier:  1,
			expectedTCPTimeout:      30 * time.Second,
			expectedSuspicionMult:   1,               // Overridden by fast failure detection
			expectedDeadReclaimTime: 1 * time.Second, // Overridden by fast failure detection
			expectedConfigType:      "WAN",
		},
		{
			name: "WAN with timeout multiplier",
			config: Config{
				AdvertiseAddr: "192.168.1.100",
			},
			raftTimeoutsMultiplier:  2,
			expectedTCPTimeout:      60 * time.Second, // 30 * 2
			expectedSuspicionMult:   6,                // Default WAN config
			expectedDeadReclaimTime: 60 * time.Second,
			expectedConfigType:      "WAN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Select the appropriate config (this sets the default SuspicionMult)
			cfg := selectMemberlistConfig(tt.config)

			// Apply our settings (this may override some values)
			configureMemberlistSettings(cfg, tt.config, tt.raftTimeoutsMultiplier)

			assert.Equal(t, tt.expectedConfigType, getConfigType(tt.config))
			assert.Equal(t, tt.expectedTCPTimeout, cfg.TCPTimeout)
			assert.Equal(t, tt.expectedSuspicionMult, cfg.SuspicionMult)
			assert.Equal(t, tt.expectedDeadReclaimTime, cfg.DeadNodeReclaimTime)
		})
	}
}

func TestGetConfigType(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "localhost",
			config: Config{
				Localhost: true,
			},
			expected: "LOCAL",
		},
		{
			name: "WAN with advertise address",
			config: Config{
				AdvertiseAddr: "192.168.1.100",
			},
			expected: "WAN",
		},
		{
			name:   "LAN default",
			config: Config{
				// No special flags
			},
			expected: "LAN",
		},
		{
			name: "localhost takes precedence over advertise addr",
			config: Config{
				Localhost:     true,
				AdvertiseAddr: "192.168.1.100",
			},
			expected: "LOCAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getConfigType(tt.config)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractHost(t *testing.T) {
	tests := []struct {
		name     string
		addr     string
		expected string
	}{
		{
			name:     "IPv4 with port",
			addr:     "192.168.1.100:7946",
			expected: "192.168.1.100",
		},
		{
			name:     "IPv4 without port",
			addr:     "192.168.1.100",
			expected: "192.168.1.100",
		},
		{
			name:     "hostname with port",
			addr:     "node0:7946",
			expected: "node0",
		},
		{
			name:     "hostname without port",
			addr:     "node0",
			expected: "node0",
		},
		{
			name:     "IPv6 with brackets and port",
			addr:     "[2803:6082:5088:4fc9:9efc:a9d7:143a:0a00]:7946",
			expected: "2803:6082:5088:4fc9:9efc:a9d7:143a:0a00",
		},
		{
			name:     "IPv6 loopback with brackets and port",
			addr:     "[::1]:7946",
			expected: "::1",
		},
		{
			name:     "IPv6 bare (no brackets, no port) — fallback",
			addr:     "2803:6082:5088:4fc9:9efc:a9d7:143a:0a00",
			expected: "2803:6082:5088:4fc9:9efc:a9d7:143a:0a00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractHost(tt.addr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractHostPreservesValidIPForLookup(t *testing.T) {
	// Verify that extractHost returns a value that net.ParseIP accepts.
	// This is the critical property: the old code using strings.Split(":")[0]
	// would return "[2803" for bracketed IPv6, which is not a valid IP.
	ipv6Addrs := []string{
		"[2803:6082:5088:4fc9:9efc:a9d7:143a:0a00]:7946",
		"[2001:db8::1]:8300",
		"[::1]:7946",
	}

	for _, addr := range ipv6Addrs {
		t.Run(addr, func(t *testing.T) {
			host := extractHost(addr)
			// The extracted host must be parseable as an IP address
			ip := net.ParseIP(host)
			assert.NotNil(t, ip, "extractHost(%q) = %q, which is not a valid IP", addr, host)
		})
	}

	// IPv6 with zone ID: net.SplitHostPort preserves the zone encoding as-is.
	// net.ParseIP does not accept zone IDs, but the extracted host is still
	// usable for net.Dial and net.LookupIP which handle zones.
	t.Run("IPv6 with zone ID", func(t *testing.T) {
		host := extractJoinHost("[fe80::1%25eth0]:7946")
		assert.Equal(t, "fe80::1%25eth0", host)
		// Zone IDs are not parseable by net.ParseIP, but that's expected.
		assert.Nil(t, net.ParseIP(host))
	})

	// Verify the old buggy behavior would fail
	buggyHost := func(addr string) string {
		// This is what the old code did: strings.Split(addr, ":")[0]
		parts := make([]string, 0)
		for i, c := range addr {
			if c == ':' {
				parts = append(parts, addr[:i])
				break
			}
		}
		if len(parts) == 0 {
			return addr
		}
		return parts[0]
	}

	// The old code would extract "[2803" from "[2803:...]:7946"
	broken := buggyHost("[2803:6082:5088:4fc9:9efc:a9d7:143a:0a00]:7946")
	assert.Equal(t, "[2803", broken)
	assert.Nil(t, net.ParseIP(broken), "buggy extraction should NOT be a valid IP")
}

func TestIPv6AddressConstruction(t *testing.T) {
	// Verify that net.JoinHostPort produces correct bracket notation for IPv6.
	// This is the fix applied across multiple Weaviate source files.
	tests := []struct {
		name     string
		host     string
		port     int
		expected string
	}{
		{
			name:     "IPv4 address",
			host:     "192.168.1.100",
			port:     8300,
			expected: "192.168.1.100:8300",
		},
		{
			name:     "IPv6 full address",
			host:     "2803:6082:5088:4fc9:9efc:a9d7:143a:0a00",
			port:     8300,
			expected: "[2803:6082:5088:4fc9:9efc:a9d7:143a:0a00]:8300",
		},
		{
			name:     "IPv6 loopback",
			host:     "::1",
			port:     8301,
			expected: "[::1]:8301",
		},
		{
			name:     "IPv6 abbreviated",
			host:     "2001:db8::1",
			port:     7946,
			expected: "[2001:db8::1]:7946",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := net.JoinHostPort(tt.host, fmt.Sprintf("%d", tt.port))
			assert.Equal(t, tt.expected, result)

			// Verify round-trip: JoinHostPort → SplitHostPort
			host, port, err := net.SplitHostPort(result)
			require.NoError(t, err)
			assert.Equal(t, tt.host, host)
			assert.Equal(t, fmt.Sprintf("%d", tt.port), port)
		})
	}
}
