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

package deprecations

import "github.com/sirupsen/logrus"

//go:generate go run gen.go
//go:generate goimports -w data.go

func Log(logger logrus.FieldLogger, id string) {
	logger.WithField("deprecation", ByID[id]).Warning(ByID[id].Msg)
}
