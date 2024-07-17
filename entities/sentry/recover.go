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
