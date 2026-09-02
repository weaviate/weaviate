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

package rest

import (
	"net"
	"os"
	"strconv"

	flags "github.com/jessevdk/go-flags"

	entcfg "github.com/weaviate/weaviate/entities/config"
)

// listenFlags mirrors the generated server's listener flags, tags included, so
// the banner can know the REST address before the generated code parses them.
type listenFlags struct {
	Schemes []string `long:"scheme"`
	Host    string   `long:"host" default:"localhost" env:"HOST"`
	Port    int      `long:"port" env:"PORT"`
	TLSHost string   `long:"tls-host" env:"TLS_HOST"`
	TLSPort int      `long:"tls-port" env:"TLS_PORT"`
}

// restURLFromArgs derives the REST listener's URL from the command line the
// generated server will parse, ignoring every flag that is not a listener flag.
func restURLFromArgs(args []string) string {
	var f listenFlags
	parser := flags.NewParser(&f, flags.IgnoreUnknown)
	if _, err := parser.ParseArgs(args); err != nil {
		f = listenFlags{Host: "localhost"}
	}
	if len(f.Schemes) == 0 {
		f.Schemes = []string{"https"} // the generated server's default scheme
	}
	for _, s := range f.Schemes {
		if s == "http" {
			return "http://" + net.JoinHostPort(displayHost(f.Host), strconv.Itoa(f.Port))
		}
	}
	if f.TLSHost == "" {
		f.TLSHost = f.Host
	}
	return "https://" + net.JoinHostPort(displayHost(f.TLSHost), strconv.Itoa(f.TLSPort))
}

// displayHost turns a bind address into one a user can open: an unspecified
// host (0.0.0.0, ::) or none at all is shown as localhost.
func displayHost(host string) string {
	if host == "" {
		return "localhost"
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return "localhost"
	}
	return host
}

// bannerDisabled is read straight from the environment, like LOG_FORMAT and
// LOG_LEVEL.
func bannerDisabled() bool {
	return entcfg.Enabled(os.Getenv("DISABLE_STARTUP_BANNER"))
}
