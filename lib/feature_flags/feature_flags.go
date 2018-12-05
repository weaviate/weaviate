package feature_flags

import (
	"fmt"
	"os"
)

// Flags and their default value
var (
	EnableDevUI   bool = false
	EnableGetMeta bool = false
)

var flags = []struct {
	toggleVar *bool
	envVar    string
}{
	{
		toggleVar: &EnableDevUI,
		envVar:    "DEVELOPMENT_UI",
	},
	{
		toggleVar: &EnableGetMeta,
		envVar:    "FEATURE_GET_META",
	},
}

func init() {
	for _, flag := range flags {
		val, present := os.LookupEnv(flag.envVar)
		if present {
			if val == "on" || val == "ON" {
				*flag.toggleVar = true
				continue
			}
			if val == "off" || val == "OFF" {
				*flag.toggleVar = false
				continue
			}
			panic(fmt.Sprintf("Expected feature toggle %s to be set to 'on' or 'off', not '%s'", flag.envVar, val))
		}
	}
}
