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

package banner

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// Action is the log field value that marks banner entries.
const Action = "banner"

// Fetcher returns the current content; Fetch is the production one.
type Fetcher func(ctx context.Context) (*Content, error)

// clusterIDPoll is how often the repeater checks for the cluster id before
// the first banner. The id is committed through raft once a leader exists.
const clusterIDPoll = time.Second

// Repeater logs the banner once the cluster has an id and again every
// interval, drawing the content fetched from the website when there is one
// and the embedded art otherwise. It runs on its own timer, independent of
// the telemetry ticker.
type Repeater struct {
	logger    logrus.FieldLogger
	clusterID func() string
	interval  time.Duration
	fetch     Fetcher
	content   atomic.Pointer[Content]
}

// NewRepeater builds a repeater. clusterID returns "" until raft has committed
// an id; a non-positive interval means the default of 24h, and a nil fetcher
// means Fetch against the website's art file.
func NewRepeater(logger logrus.FieldLogger, clusterID func() string, interval time.Duration, fetch Fetcher) *Repeater {
	if interval <= 0 {
		interval = defaultInterval
	}
	if fetch == nil {
		client := newClient()
		fetch = func(ctx context.Context) (*Content, error) { return Fetch(ctx, client, artURL) }
	}
	return &Repeater{logger: logger, clusterID: clusterID, interval: interval, fetch: fetch}
}

// Run waits for the cluster id, logs the banner, and logs it again every
// interval until ctx is done. A cluster that never gets an id never sees a
// banner: the banner's link is only useful with the id on it.
func (r *Repeater) Run(ctx context.Context) {
	if !r.waitForClusterID(ctx) {
		return
	}
	r.refresh(ctx)
	r.emit()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.refresh(ctx)
			r.emit()
		}
	}
}

func (r *Repeater) waitForClusterID(ctx context.Context) bool {
	if r.clusterID() != "" {
		return true
	}
	ticker := time.NewTicker(clusterIDPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if r.clusterID() != "" {
				return true
			}
		}
	}
}

func (r *Repeater) refresh(ctx context.Context) {
	fctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	content, err := r.fetch(fctx)
	if err != nil {
		// Debug, not error: a node without egress fails this forever. Not the
		// banner action: the error text is remote input and must stay on the
		// formatter's quoted single-line path.
		r.logger.WithField("action", "banner_fetch").Debugf("banner art not fetched, keeping the last known art: %v", err)
		return
	}
	r.content.Store(content)
}

// Content is what the next emission draws.
func (r *Repeater) Content() *Content {
	if c := r.content.Load(); c != nil {
		return c
	}
	return &Content{Art: embeddedArt}
}

func (r *Repeater) emit() {
	docsURL := enterrors.WithClusterID(landingURL())
	content := r.Content()
	r.logger.WithFields(logrus.Fields{
		"action":                Action,
		enterrors.DocsLinkField: docsURL,
	}).Info(render(content.Art, content.Message, docsURL))
}
