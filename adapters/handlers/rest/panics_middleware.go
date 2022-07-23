package rest

import (
	"net/http"
	"syscall"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func makeCatchPanics(logger logrus.FieldLogger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer handlePanics(logger, r)
			next.ServeHTTP(w, r)
		})
	}
}

func handlePanics(logger logrus.FieldLogger, r *http.Request) {
	recovered := recover()
	if recovered == nil {
		return
	}

	err, ok := recovered.(error)
	if !ok {
		// not a typed error, we can not handle this error other returning it to
		// the user
		logger.WithField("error", recovered).Error()
		return
	}

	if errors.Is(err, syscall.EPIPE) {
		handleBrokenPipe(err, logger, r)
		return
	}
}

func handleBrokenPipe(err error, logger logrus.FieldLogger, r *http.Request) {
	logger.WithError(err).WithFields(logrus.Fields{
		"method":      r.Method,
		"path":        r.URL,
		"description": "A broken pipe error occurs when Weaviate tries to write a response onto a connection that has already been closed or reset by the client. Typically, this is the case when the server was not able to respond within the configured client-side timeout.",
		"hint":        "Either try increasing the client-side timeout, or sending a computationally cheaper request, for example by reducing a batch size, reducing a limit, using less complex filters, etc.",
	}).Errorf("broken pipe")
}
