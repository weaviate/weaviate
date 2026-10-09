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

package namespaces

import (
	"errors"
	"strings"
)

// refusalSentinels lists every error RequireShardLoadable refuses a shard load
// with.
var refusalSentinels = []error{
	ErrNamespaceSuspended,
	ErrNamespaceResuming,
	ErrNamespaceDeleting,
	ErrInvalidState,
}

// RefusalText returns err's text and true when err wraps an error
// RequireShardLoadable returns. The text always ends with that error's text,
// which the coordinating node matches on.
func RefusalText(err error) (string, bool) {
	for _, sentinel := range refusalSentinels {
		if !errors.Is(err, sentinel) {
			continue
		}
		text := err.Error()
		if !strings.HasSuffix(text, sentinel.Error()) {
			text += ": " + sentinel.Error()
		}
		return text, true
	}
	return "", false
}
