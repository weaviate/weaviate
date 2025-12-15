//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package build

import (
	"runtime"

	"github.com/prometheus/common/version"
)

// These package level variables are set during build time via ldflags -X
// Also these information are passed to prometheus version package so that it's available in `weaviate_build_info` metric
var (
	// Version is Weviate's version. e.g: 1.26.4.
	// We usually read it from `openapi-specs/schema.json` in this repository.
	Version string

	// Revision represents the GIT commit of the build.
	Revision string

	// Branch is the GIT branch of the build.
	Branch string

	// BuildUser represents who triggered the build.
	// Usually something like `$(whoami)@$(hostname)`
	BuildUser string

	// BuildDate represents build date and time of the build.
	BuildDate string

	// GoVersion represents the Go compiler version used in the build
	GoVersion string
)

func init() {
	GoVersion = runtime.Version()
}

const (
	AppName = "weaviate"
)

func SetPrometheusBuildInfo() {
	version.Version = Version
	version.Revision = Revision
	version.Branch = Branch
	version.BuildUser = BuildUser
	version.BuildDate = BuildDate
	version.GoVersion = GoVersion
}
