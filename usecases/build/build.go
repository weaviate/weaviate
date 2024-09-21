package build

import (
	"runtime"

	"github.com/prometheus/common/version"
)

// These package level variables are set during build time via ldflags -X
// Also these information are passed to prometheus version package so that it's available in nice `weaviate_info` metric
var (
	Version   string
	Revision  string
	Branch    string
	BuildUser string
	BuildDate string
	GoVersion string
)

const (
	AppName = "weaviate"
)

func SetPrometheusBuildInfo() {
	version.Version = Version
	version.Revision = Revision
	version.Branch = Branch
	version.BuildUser = BuildUser
	version.BuildDate = BuildDate
	version.GoVersion = runtime.Version()
}
