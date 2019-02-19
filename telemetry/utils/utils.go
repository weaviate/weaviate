package utils

import (
	"github.com/creativesoftwarefdn/weaviate/restapi"
)

// check whether the feature toggle is set to 'enabled' (default) or disabled
func IsEnabled() bool {
	flag := restapi.GetTelemetryFlag()
	return flag
}
