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

package rbac

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	MODEL = `
	[request_definition]
	r = sub, obj, act

	[policy_definition]
	p = sub, obj, act, dom

	[role_definition]
	g = _, _

	[policy_effect]
	e = some(where (p.eft == allow))

	[matchers]
	m = g(r.sub, p.sub) && (keyMatch5(r.obj, p.obj) || regexMatch(r.obj, p.obj)) && regexMatch(r.act, p.act)
`
)

func CreateStorage(filePath string) error {
	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories: %w", err)
	}

	_, err := os.Stat(filePath)
	if err == nil { // file exists
		return nil
	}

	if os.IsNotExist(err) {
		file, err := os.Create(filePath)
		if err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}
		defer file.Close()
		return nil
	}

	return err
}
