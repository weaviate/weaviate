package utils

import (
//	"github.com/creativesoftwarefdn/weaviate/restapi"
)

// check whether the feature toggle is set to 'enabled' (default) or disabled
func IsEnabled() bool {
	// TODO: perform flag error handling in this func, use messaging for non true/false states and assume false
	//	flag := restapi.GetTelemetryFlag()
	flag := Enabled
	return flag
}
