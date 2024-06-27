package config

func Enabled(value string) bool {
	if value == "" {
		return false
	}

	if value == "on" ||
		value == "enabled" ||
		value == "1" ||
		value == "true" {
		return true
	}

	return false
}
