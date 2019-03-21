package utils

// TODO: determine the best place for these settings. This works, but reading a json file might be a better solution
var Enabled bool = true
var Interval int = 1
var URL string = "127.0.0.1:8087/mock/new"

// check whether the feature toggle is set to 'enabled' (default) or disabled.
// TODO: perform flag error handling in this func, use messaging for non true/false states and assume false
func IsEnabled() bool {
	return Enabled
}

func GetInterval() int {
	return Interval
}

func GetURL() string {
	return URL
}
