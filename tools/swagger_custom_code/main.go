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

package main

import (
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	if err := overrideObject("entities/models/vectors.go"); err != nil {
		log.Fatal(err)
	}
	if err := overrideHandleShutdown("adapters/handlers/rest/server.go"); err != nil {
		log.Fatal(err)
	}
}

// generatedShutdownGate is the end of go-swagger's handleShutdown, which calls
// ServerShutdown only when every HTTP server drained within GracefulTimeout.
const generatedShutdownGate = `	// Wait until all listeners have successfully shut down before calling ServerShutdown
	success := true
	for range servers {
		success = success && <-shutdownChan
	}
	if success {
		s.api.ServerShutdown()
	}
`

const shutdownAfterDrain = `	// Wait for every listener's Shutdown to return. ServerShutdown runs even when
	// a drain timed out, since it leaves the cluster and closes the database.
	for range servers {
		<-shutdownChan
	}
	s.api.ServerShutdown()
`

// overrideHandleShutdown fails unless generatedShutdownGate occurs exactly once,
// so a go-swagger template change stops the generation instead of dropping the fix.
func overrideHandleShutdown(name string) error {
	bytes, err := os.ReadFile(name)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}

	src := string(bytes)
	if n := strings.Count(src, generatedShutdownGate); n != 1 {
		return fmt.Errorf("%s: expected the handleShutdown gate once, found it %d times", name, n)
	}
	src = strings.Replace(src, generatedShutdownGate, shutdownAfterDrain, 1)
	return os.WriteFile(name, []byte(src), 0)
}

func overrideObject(name string) error {
	bytes, err := os.ReadFile(name)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}

	objectStr := string(bytes)

	importStr := `import (
	"fmt"
	"encoding/json"`

	objectStr = strings.Replace(objectStr, "import (", importStr, 1)

	unmarshalStr := `
// UnmarshalJSON custom unmarshalling method
func (v *Vectors) UnmarshalJSON(data []byte) error {
	var rawVectors map[string]json.RawMessage
	if err := json.Unmarshal(data, &rawVectors); err != nil {
		return err
	}

	if len(rawVectors) > 0 {
		*v = make(Vectors)
		for targetVector, rawMessage := range rawVectors {
			// Try unmarshaling as []float32
			var vector []float32
			if err := json.Unmarshal(rawMessage, &vector); err == nil {
				if len(vector) > 0 {
					(*v)[targetVector] = vector
				}
				continue
			}
			// Try unmarshaling as [][]float32
			var multiVector [][]float32
			if err := json.Unmarshal(rawMessage, &multiVector); err == nil {
				if len(multiVector) > 0 {
					(*v)[targetVector] = multiVector
				}
				continue
			}
			return fmt.Errorf("vectors: cannot unmarshal vector into either []float32 or [][]float32 for target vector %s", targetVector)
		}
	}
	return nil
}
`
	return os.WriteFile(name, []byte(fmt.Sprintf("%s%s", objectStr, unmarshalStr)), 0)
}
