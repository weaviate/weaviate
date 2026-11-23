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

package distributedtask

import (
	"strings"

	"github.com/sirupsen/logrus"
)

// UnreadablePayloads lets one pass over a task list log a single line for all undecodable payloads.
type UnreadablePayloads struct {
	count    int
	sample   []string
	firstErr error
}

func (u *UnreadablePayloads) Add(taskID string, err error) {
	u.count++
	if len(u.sample) < 5 {
		u.sample = append(u.sample, capWidth(taskID))
	}
	if u.firstErr == nil {
		u.firstErr = err
	}
}

// Log writes one line, or nothing when no task was added.
func (u *UnreadablePayloads) Log(logger logrus.FieldLogger, what string) {
	if u.count == 0 || logger == nil {
		return
	}
	logger.Warnf("%d %s; e.g. %s: %s", u.count, what, strings.Join(u.sample, ", "), capWidth(u.firstErr.Error()))
}
