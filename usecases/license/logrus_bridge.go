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

package license

import (
	"context"
	"log/slog"

	"github.com/sirupsen/logrus"
)

// logrusHandler adapts the protocol package's slog.Logger to Weaviate's
// logrus logger, so license log lines carry the usual fields and format.
type logrusHandler struct {
	logger logrus.FieldLogger
	attrs  []slog.Attr
}

func newSlogLogger(l logrus.FieldLogger) *slog.Logger {
	return slog.New(&logrusHandler{logger: l.WithField("action", "license")})
}

func (h *logrusHandler) Enabled(_ context.Context, level slog.Level) bool {
	return true
}

func (h *logrusHandler) Handle(_ context.Context, r slog.Record) error {
	fields := logrus.Fields{}
	for _, a := range h.attrs {
		fields[a.Key] = a.Value.Any()
	}
	r.Attrs(func(a slog.Attr) bool {
		fields[a.Key] = a.Value.Any()
		return true
	})
	entry := h.logger.WithFields(fields)
	switch {
	case r.Level >= slog.LevelError:
		entry.Error(r.Message)
	case r.Level >= slog.LevelWarn:
		entry.Warn(r.Message)
	case r.Level >= slog.LevelInfo:
		entry.Info(r.Message)
	default:
		entry.Debug(r.Message)
	}
	return nil
}

func (h *logrusHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logrusHandler{logger: h.logger, attrs: append(append([]slog.Attr{}, h.attrs...), attrs...)}
}

func (h *logrusHandler) WithGroup(string) slog.Handler { return h }
