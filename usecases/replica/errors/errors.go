//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package errors

import (
	"errors"
)

var (
	// MsgCLevel consistency level cannot be achieved
	MsgCLevel = "cannot achieve consistency level"

	ErrReplicas = errors.New("cannot reach enough replicas")
	ErrRepair   = errors.New("read repair error")
	ErrRead     = errors.New("read error")

	ErrNoDiffFound = errors.New("no diff found")
)
