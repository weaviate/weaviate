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

package errors

import (
	"errors"

	"github.com/sirupsen/logrus"
)

const (
	// DocsLinkField holds the link in a log entry's structured data instead of
	// its message: operators fingerprint and alert on message text, so a URL
	// appended there breaks their rules.
	DocsLinkField = "docs_url"

	// docsLinkBase resolves an id through a redirect kept in the docs repo, so
	// a docs restructure cannot rot a URL already compiled into a release.
	docsLinkBase = "https://docs.weaviate.io/e/"

	docsIDNotEnoughMappings = "core-mem001"
)

func docsLink(id string) string {
	return docsLinkBase + id
}

// DocsLinkFields returns the log fields pointing at the page that explains err,
// and nil for an error with no documented page. logrus ignores nil fields, so a
// log site reporting errors of mixed origin can add them unconditionally.
func DocsLinkFields(err error) logrus.Fields {
	if errors.Is(err, ErrNotEnoughMappings) {
		return logrus.Fields{DocsLinkField: docsLink(docsIDNotEnoughMappings)}
	}

	return nil
}
