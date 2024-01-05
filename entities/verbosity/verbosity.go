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

package verbosity

import "fmt"

const (
	OutputMinimal = "minimal"
	OutputVerbose = "verbose"
)

// ParseOutput extracts the verbosity value from the provided nullable string
// If `output` is nil, the default selection is "minimal"
func ParseOutput(output *string) (string, error) {
	if output != nil {
		switch *output {
		case OutputMinimal, OutputVerbose:
			return *output, nil
		default:
			return "", fmt.Errorf(`invalid output: "%s", possible values are: "%s", "%s"`,
				*output, OutputMinimal, OutputVerbose)
		}
	}
	return OutputMinimal, nil
}
