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

package sentry

import (
	libsentry "github.com/getsentry/sentry-go"
)

func Recover(err any) {
	if !Enabled() {
		return
	}

	libsentry.CurrentHub().Recover(err)
}

func CaptureException(err error) {
	if !Enabled() {
		return
	}

	libsentry.CaptureException(err)
}
