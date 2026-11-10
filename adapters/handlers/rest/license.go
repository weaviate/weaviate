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

package rest

import (
	"context"
	"log/slog"
	"time"

	"github.com/sirupsen/logrus"

	licenseclient "github.com/weaviate/weaviate/adapters/clients/license"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/usecases/build"
	"github.com/weaviate/weaviate/usecases/config"
	licensechecker "github.com/weaviate/weaviate/usecases/license/checker"
)

// newLicenseChecker only constructs the checker. Network traffic starts when
// Run is called in the shutdown-managed goroutine after raft is available.
func newLicenseChecker(appState *state.State) (*licensechecker.Checker, error) {
	if appState.ServerConfig.Config.License.LicenseID == "" {
		return nil, nil
	}
	key, err := config.LoadLicenseKey()
	if err != nil {
		return nil, err
	}
	trusted, err := licenseclient.TrustedServerKeys()
	if err != nil {
		return nil, err
	}
	client, err := licenseclient.NewClient(key, trusted)
	if err != nil {
		return nil, err
	}
	client.UserAgent = "weaviate/" + build.Version

	checker := &licensechecker.Checker{
		Client:          client,
		InstanceID:      appState.ServerConfig.Config.Cluster.Hostname,
		WeaviateVersion: build.Version,
		ReportingOnly:   true,
		Log:             slog.New(licenseLogHandler{logger: appState.Logger}),
		ClusterIDFunc: func() string {
			if appState.ClusterService == nil {
				return ""
			}
			return appState.ClusterService.ClusterID()
		},
	}
	return checker, nil
}

// runLicenseChecker waits outside the startup path until raft has an identity.
// Sending an early empty ID could bind or report the license against the wrong
// cluster and would not be corrected until the next scheduled verification.
func runLicenseChecker(ctx context.Context, checker *licensechecker.Checker) {
	for checker.ClusterIDFunc != nil && checker.ClusterIDFunc() == "" {
		select {
		case <-ctx.Done():
			return
		case <-time.After(25 * time.Millisecond):
		}
	}
	checker.Run(ctx)
}

type licenseLogHandler struct {
	logger logrus.FieldLogger
	attrs  []slog.Attr
	group  string
}

func (h licenseLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	if logger, ok := h.logger.(interface {
		IsLevelEnabled(logrus.Level) bool
	}); ok {
		return logger.IsLevelEnabled(logrusLevel(level))
	}
	return true
}

func (h licenseLogHandler) Handle(_ context.Context, record slog.Record) error {
	fields := logrus.Fields{"action": "license"}
	for _, attr := range h.attrs {
		h.addAttr(fields, attr)
	}
	record.Attrs(func(attr slog.Attr) bool {
		h.addAttr(fields, attr)
		return true
	})
	h.logger.WithFields(fields).Log(logrusLevel(record.Level), record.Message)
	return nil
}

func (h licenseLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return h
}

func (h licenseLogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	if h.group != "" {
		name = h.group + "." + name
	}
	h.group = name
	return h
}

func (h licenseLogHandler) addAttr(fields logrus.Fields, attr slog.Attr) {
	key := attr.Key
	if h.group != "" {
		key = h.group + "." + key
	}
	fields[key] = attr.Value.Resolve().Any()
}

func logrusLevel(level slog.Level) logrus.Level {
	switch {
	case level >= slog.LevelError:
		return logrus.ErrorLevel
	case level >= slog.LevelWarn:
		return logrus.WarnLevel
	case level >= slog.LevelInfo:
		return logrus.InfoLevel
	default:
		return logrus.DebugLevel
	}
}
