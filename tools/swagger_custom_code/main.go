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

package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	overrideObject("entities/models/object.go")
}

func overrideObject(name string) error {
	bytes, err := os.ReadFile(name)
	if err != nil {
		return fmt.Errorf("%s: %v", name, err)
	}

	objectStr := string(bytes)

	importStr := `import (
	"fmt"
	"encoding/json"`

	objectStr = strings.Replace(objectStr, "import (", importStr, 1)

	unmarshalStr := `
// UnmarshalJSON custom unmarshal code
func (m *Object) UnmarshalJSON(data []byte) error {
	type alias Object
	aux := &struct {
		Vectors map[string]json.RawMessage ` + "`" + `json:"vectors,omitempty"` + "`" + `
		*alias
	}{
		alias: (*alias)(m),
	}

	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}

	// Vectors are nil
	if len(aux.Vectors) == 0 {
		return nil
	}

	m.Vectors = make(Vectors)
	for targetVector, rawMessage := range aux.Vectors {
		// Try unmarshaling as []float32
		var vector []float32
		if err := json.Unmarshal(rawMessage, &vector); err == nil {
			if len(vector) > 0 {
				m.Vectors[targetVector] = vector
			}
			continue
		}

		// Try unmarshaling as [][]float32
		var multiVector [][]float32
		if err := json.Unmarshal(rawMessage, &multiVector); err == nil {
			if len(multiVector) > 0 {
				m.Vectors[targetVector] = multiVector
			}
			continue
		}

		return fmt.Errorf("vectors: cannot unmarshal vector into either []float32 or [][]float32 for target vector %s", targetVector)
	}

	return nil
}
`
	return os.WriteFile(name, []byte(fmt.Sprintf("%s%s", objectStr, unmarshalStr)), 0)
}
